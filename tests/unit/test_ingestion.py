# tests/unit/test_ingestion.py
# -----------------------------------------------------------------------------
# Unit tests for src/extract/ingest_api.py, ingest_csv.py, and
# src/utils/ingestion_helpers.py.
# External calls (HTTP, MySQL) are mocked.
# -----------------------------------------------------------------------------
import re
from datetime import datetime
from unittest.mock import MagicMock

import pytest


# =============================================================================
# Fixtures local to this file
# =============================================================================
@pytest.fixture
def mock_mysql_conn(monkeypatch):
    """
    Patches mysql.connector.connect to return a MagicMock connection.
    Yields the mock cursor so tests can configure fetchone() etc.
    """
    cursor = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value = cursor

    mock_connect = MagicMock(return_value=conn)
    monkeypatch.setattr("mysql.connector.connect", mock_connect)
    # Credentials have no built-in default; the mocked connection needs a dummy value.
    monkeypatch.setenv("MYSQL_PASSWORD", "unit-test-password")
    return cursor


# =============================================================================
# Test group 1: Watermark tracking
# =============================================================================
class TestWatermark:
    """get_watermark returns last_extracted_at; update_watermark upserts."""

    def test_get_watermark_returns_none_when_absent(self, mock_mysql_conn):
        """A missing watermark row → None (signals full load)."""
        mock_mysql_conn.fetchone.return_value = None

        from src.utils.ingestion_helpers import get_watermark

        result = get_watermark(pipeline_name="orders")

        assert result is None
        # Verify the correct SQL was used
        executed_sql = mock_mysql_conn.execute.call_args[0][0]
        assert "watermarks" in executed_sql.lower()
        assert "pipeline_name" in executed_sql.lower()

    def test_get_watermark_returns_stored_timestamp(self, mock_mysql_conn):
        """An existing watermark row → datetime."""
        expected = datetime(2026, 6, 16, 23, 5, 20)
        mock_mysql_conn.fetchone.return_value = (expected,)

        from src.utils.ingestion_helpers import get_watermark

        result = get_watermark(pipeline_name="orders")

        assert result == expected

    def test_get_watermark_uses_parameterised_query(self, mock_mysql_conn):
        """Verify pipeline_name is passed as a parameter, not string-interpolated."""
        mock_mysql_conn.fetchone.return_value = None

        from src.utils.ingestion_helpers import get_watermark

        get_watermark(pipeline_name="orders")

        # Second arg to execute() should be the params tuple
        args = mock_mysql_conn.execute.call_args
        assert args[0][1] == ("orders",)

    def test_update_watermark_executes_upsert(self, mock_mysql_conn):
        """update_watermark issues INSERT ... ON DUPLICATE KEY UPDATE."""
        ts = datetime(2026, 6, 17, 10, 0, 0)

        from src.utils.ingestion_helpers import update_watermark

        update_watermark(pipeline_name="orders", last_extracted_at=ts)

        sql = mock_mysql_conn.execute.call_args[0][0]
        assert "INSERT" in sql.upper()
        assert "ON DUPLICATE KEY UPDATE" in sql.upper()
        # Parameters should include pipeline_name and the timestamp
        params = mock_mysql_conn.execute.call_args[0][1]
        assert params[0] == "orders"
        assert ts in params


# =============================================================================
# Test group 2: Batch ID generation
# =============================================================================
class TestBatchId:
    def test_batch_id_format(self):
        """batch_id matches batch_YYYYMMDD_HHMMSS."""
        from src.utils.ingestion_helpers import generate_batch_id

        batch_id = generate_batch_id()
        assert re.match(r"^batch_\d{8}_\d{6}$", batch_id), batch_id

    def test_batch_id_has_correct_prefix(self):
        from src.utils.ingestion_helpers import generate_batch_id

        assert generate_batch_id().startswith("batch_")

    def test_batch_ids_differ_across_seconds(self, monkeypatch):
        """Two calls one second apart produce different IDs."""
        from datetime import datetime as real_dt

        import src.utils.ingestion_helpers as helpers

        class FakeDatetime:
            _now = real_dt(2026, 6, 16, 10, 0, 0)

            @classmethod
            def now(cls):
                return cls._now

            @classmethod
            def set_now(cls, value):
                cls._now = value

        monkeypatch.setattr(helpers, "datetime", FakeDatetime)
        id_1 = helpers.generate_batch_id()

        FakeDatetime.set_now(real_dt(2026, 6, 16, 10, 0, 1))
        id_2 = helpers.generate_batch_id()

        assert id_1 != id_2
        assert id_1 == "batch_20260616_100000"
        assert id_2 == "batch_20260616_100001"


# =============================================================================
# Test group 3: Pipeline run management
# =============================================================================
class TestPipelineRun:
    def test_start_pipeline_run_returns_run_id(self, mock_mysql_conn):
        """run_id is generated and returned, prefixed with pipeline_name."""
        from src.utils.ingestion_helpers import start_pipeline_run

        run_id = start_pipeline_run(
            pipeline_name="ingest_api",
            source_system="mock-api",
            batch_id="batch_20260616_100000",
        )

        assert run_id.startswith("ingest_api_")
        # <pipeline>_<YYYYMMDD>_<HHMMSS>_<microseconds>_<6-hex suffix>
        assert re.match(r"^ingest_api_\d{8}_\d{6}_\d{6}_[0-9a-f]{6}$", run_id), run_id
        assert len(run_id) <= 100  # pipeline_runs.run_id is VARCHAR(100)

    def test_start_pipeline_run_inserts_row(self, mock_mysql_conn):
        """Verify the INSERT statement was executed with correct params."""
        from src.utils.ingestion_helpers import start_pipeline_run

        start_pipeline_run(
            pipeline_name="ingest_api",
            source_system="mock-api",
            batch_id="batch_20260616_100000",
        )

        sql = mock_mysql_conn.execute.call_args[0][0]
        assert "INSERT INTO pipeline_runs" in sql
        assert "running" in sql.lower()

    def test_complete_pipeline_run_updates_row(self, mock_mysql_conn):
        """complete_pipeline_run issues an UPDATE with the final status."""
        from src.utils.ingestion_helpers import complete_pipeline_run

        complete_pipeline_run(
            run_id="ingest_api_20260616_100000",
            status="success",
            rows_extracted=100,
            rows_loaded=100,
        )

        sql = mock_mysql_conn.execute.call_args[0][0]
        assert "UPDATE pipeline_runs" in sql
        params = mock_mysql_conn.execute.call_args[0][1]
        assert "success" in params


# =============================================================================
# Test group 4: HTTP 429 retry with backoff
# =============================================================================
class TestRetryBackoff:
    """
    ingest_api.py retries HTTP 429 with exponential backoff.
    Uses requests.get + time.sleep.
    """

    def test_retry_on_429_then_success(self, monkeypatch):
        response_429 = MagicMock()
        response_429.status_code = 429
        response_429.headers = {"Retry-After": "1"}

        response_200 = MagicMock()
        response_200.status_code = 200
        response_200.json.return_value = {"orders": [], "next_page": None}
        response_200.raise_for_status = MagicMock()

        responses = iter([response_429, response_200])
        monkeypatch.setattr(
            "requests.get",
            MagicMock(side_effect=lambda *a, **kw: next(responses)),
        )
        monkeypatch.setattr("time.sleep", lambda s: None)

        # Skip if fetch_page doesn't exist yet
        pytest.importorskip("src.extract.ingest_api")
        import src.extract.ingest_api as api_mod

        if not hasattr(api_mod, "fetch_page"):
            pytest.skip("fetch_page not implemented yet")

        result = api_mod.fetch_page(
            base_url="http://mock-api:8000", page=1, page_size=100
        )
        assert result is not None


# =============================================================================
# Test group 5: CSV file type detection (best-effort, may skip)
# =============================================================================
class TestCsvQuarantine:
    def test_detect_file_type_returns_orders_for_orders_dir(self, tmp_path):
        orders_dir = tmp_path / "orders"
        orders_dir.mkdir()
        csv_file = orders_dir / "orders_batch_20260616.csv"
        csv_file.write_text("x\n")

        try:
            from src.extract.ingest_csv import detect_file_type
        except ImportError:
            pytest.skip("ingest_csv not importable")

        assert detect_file_type(csv_file) == "orders"

    def test_detect_file_type_returns_clickstream_for_clickstream_dir(self, tmp_path):
        cs_dir = tmp_path / "clickstream"
        cs_dir.mkdir()
        csv_file = cs_dir / "clickstream_batch.csv"
        csv_file.write_text("x\n")

        try:
            from src.extract.ingest_csv import detect_file_type
        except ImportError:
            pytest.skip("ingest_csv not importable")

        assert detect_file_type(csv_file) == "clickstream"
