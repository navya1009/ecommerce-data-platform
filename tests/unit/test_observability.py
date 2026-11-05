# tests/unit/test_observability.py
# -----------------------------------------------------------------------------
# Phase 9.1 observability: logger, metrics, lineage, and their call sites.
# No Docker and no AWS: psycopg.connect and boto3.client are patched.
# -----------------------------------------------------------------------------
import argparse
import json
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


# =============================================================================
# Shared fixtures
# =============================================================================
@pytest.fixture
def pipeline_env(monkeypatch):
    """Known pipeline context; AWS_REGION unset unless a test sets it."""
    monkeypatch.setenv("GIT_COMMIT_SHA", "abc123def")
    monkeypatch.setenv("PIPELINE_RUN_ID", "run_test_001")
    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", "ecommerce_pipeline")
    monkeypatch.setenv("AIRFLOW_CTX_TASK_ID", "spark_transform_orders")
    monkeypatch.setenv("ENVIRONMENT", "local")
    monkeypatch.delenv("AWS_REGION", raising=False)
    return monkeypatch


class ListHandler(logging.Handler):
    """Keeps every record it sees, after logger-level filters have run."""

    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)


class FakeCloudWatchHandler(logging.Handler):
    """Stands in for watchtower.CloudWatchLogHandler: a real Handler that emits nothing."""

    def __init__(self, **kwargs):
        super().__init__()
        self.kwargs = kwargs


# =============================================================================
# Logger
# =============================================================================
class TestLogger:
    @pytest.mark.parametrize(
        "environment, expected_level",
        [
            ("local", logging.DEBUG),
            ("dev", logging.INFO),
            ("staging", logging.WARNING),
            ("prod", logging.ERROR),
        ],
    )
    def test_level_follows_environment(self, pipeline_env, environment, expected_level):
        from src.utils.logger import get_logger

        pipeline_env.setenv("ENVIRONMENT", environment)
        logger = get_logger(f"test.level.{environment}")
        assert isinstance(logger, logging.Logger)
        assert logger.level == expected_level

    def test_records_carry_required_context_fields(self, pipeline_env):
        from src.utils.logger import get_logger

        logger = get_logger("test.fields")
        capture = ListHandler()
        logger.addHandler(capture)
        logger.info("Ingested rows", extra={"rows": 210263})

        record = capture.records[-1]
        assert record.pipeline_run_id == "run_test_001"
        assert record.dag_id == "ecommerce_pipeline"
        assert record.task_id == "spark_transform_orders"
        assert record.environment == "local"
        assert record.git_commit_sha == "abc123def"

    def test_json_output_parses_with_context_keys(self, pipeline_env, capsys):
        from src.utils.logger import get_logger

        pipeline_env.setenv("ENVIRONMENT", "dev")
        logger = get_logger("test.json")
        logger.info("Ingested rows", extra={"rows": 5, "batch_id": "batch_1"})

        line = capsys.readouterr().err.strip().splitlines()[-1]
        payload = json.loads(line)
        assert payload["message"] == "Ingested rows"
        assert payload["rows"] == 5
        assert payload["batch_id"] == "batch_1"
        for key in (
            "pipeline_run_id",
            "dag_id",
            "task_id",
            "environment",
            "git_commit_sha",
        ):
            assert key in payload, f"missing {key} in JSON log"
        assert payload["environment"] == "dev"

    def test_no_cloudwatch_handler_when_local(self, pipeline_env):
        from src.utils.logger import get_logger

        with patch("watchtower.CloudWatchLogHandler") as cw_handler, patch(
            "boto3.client"
        ) as boto_client:
            pipeline_env.setenv("AWS_REGION", "us-east-1")  # region set, but local
            get_logger("test.local.nocw")
        cw_handler.assert_not_called()
        boto_client.assert_not_called()

    def test_cloudwatch_handler_attached_in_dev(self, pipeline_env):
        from src.utils.logger import get_logger

        pipeline_env.setenv("ENVIRONMENT", "dev")
        pipeline_env.setenv("AWS_REGION", "us-east-1")
        with patch(
            "watchtower.CloudWatchLogHandler",
            side_effect=lambda **kw: FakeCloudWatchHandler(**kw),
        ) as cw_handler, patch("boto3.client") as boto_client:
            logger = get_logger("test.dev.cw")

        cw_handler.assert_called_once()
        kwargs = cw_handler.call_args.kwargs
        assert kwargs["log_group_name"] == "/aws/ecommerce-data-platform/dev"
        assert kwargs["log_stream_name"] == "run_test_001"
        boto_client.assert_called_once()
        assert any(isinstance(h, FakeCloudWatchHandler) for h in logger.handlers)

    def test_cloudwatch_setup_failure_does_not_raise(self, pipeline_env):
        from src.utils.logger import get_logger

        pipeline_env.setenv("ENVIRONMENT", "prod")
        pipeline_env.setenv("AWS_REGION", "us-east-1")
        with patch("boto3.client", side_effect=RuntimeError("no credentials")):
            logger = get_logger("test.prod.broken")
        assert logger.handlers  # console handler still attached


# =============================================================================
# Metrics
# =============================================================================
def _insert_params(connect_mock):
    """The parameter tuple of the INSERT executed on the mocked connection."""
    cursor = connect_mock.return_value.cursor.return_value
    cursor.execute.assert_called_once()
    return cursor.execute.call_args.args[1]


class TestMetrics:
    def test_success_writes_one_row(self, pipeline_env):
        from src.utils.metrics import track_metrics

        with patch("psycopg.connect") as connect:
            with track_metrics(
                pipeline_name="spark_transform_orders", run_id="r1"
            ) as m:
                m.rows_loaded = 7
                m.rows_quarantined = 2
        params = _insert_params(connect)
        assert params[0] == "r1"
        assert params[1] == "spark_transform_orders"
        assert params[3] == "success"
        assert params[6] == 7 and params[7] == 2
        connect.return_value.commit.assert_called_once()

    def test_failure_writes_failed_row_and_reraises(self, pipeline_env):
        from src.utils.metrics import track_metrics

        with patch("psycopg.connect") as connect:
            with pytest.raises(ValueError, match="boom"):
                with track_metrics(pipeline_name="spark_transform_orders", run_id="r2"):
                    raise ValueError("boom")
        assert _insert_params(connect)[3] == "failed"

    def test_processing_time_is_non_negative(self, pipeline_env):
        from src.utils.metrics import track_metrics

        with patch("psycopg.connect") as connect:
            with track_metrics(pipeline_name="p", run_id="r3"):
                pass
        assert _insert_params(connect)[8] >= 0

    def test_cloudwatch_skipped_when_local(self, pipeline_env):
        from src.utils.metrics import track_metrics

        pipeline_env.setenv("AWS_REGION", "us-east-1")  # set, but environment is local
        with patch("psycopg.connect"), patch("boto3.client") as boto_client:
            with track_metrics(pipeline_name="p", run_id="r4"):
                pass
        boto_client.assert_not_called()

    def test_cloudwatch_called_in_dev_with_namespace(self, pipeline_env):
        from src.utils.metrics import track_metrics

        pipeline_env.setenv("ENVIRONMENT", "dev")
        pipeline_env.setenv("AWS_REGION", "us-east-1")
        with patch("psycopg.connect"), patch("boto3.client") as boto_client:
            with track_metrics(pipeline_name="p", run_id="r5") as m:
                m.rows_extracted = 100
        boto_client.assert_called_once()
        assert boto_client.call_args.args == ("cloudwatch",)
        assert boto_client.call_args.kwargs["region_name"] == "us-east-1"
        put = boto_client.return_value.put_metric_data
        put.assert_called_once()
        kwargs = put.call_args.kwargs
        assert kwargs["Namespace"] == "EcommerceDataPlatform/dev"
        names = {datum["MetricName"] for datum in kwargs["MetricData"]}
        assert {
            "RowsExtracted",
            "RowsTransformed",
            "RowsLoaded",
            "RowsQuarantined",
            "ProcessingTimeSeconds",
            "PipelineRunSuccess",
        } <= names

    def test_cloudwatch_failure_does_not_raise(self, pipeline_env):
        from src.utils.metrics import track_metrics

        pipeline_env.setenv("ENVIRONMENT", "dev")
        pipeline_env.setenv("AWS_REGION", "us-east-1")
        with patch("psycopg.connect"), patch(
            "boto3.client", side_effect=RuntimeError("cloudwatch down")
        ):
            with track_metrics(pipeline_name="p", run_id="r6"):
                pass  # must complete without raising

    def test_db_failure_does_not_mask_run_result(self, pipeline_env):
        from src.utils.metrics import track_metrics

        with patch("psycopg.connect", side_effect=ConnectionError("db down")):
            with track_metrics(pipeline_name="p", run_id="r7") as m:
                m.rows_loaded = 1  # the run itself still succeeds


# =============================================================================
# Lineage
# =============================================================================
def _cursor_with(rows, columns):
    cursor = MagicMock()
    cursor.description = [SimpleNamespace(name=c) for c in columns]
    cursor.fetchall.return_value = rows
    return cursor


class TestLineage:
    def test_record_lineage_inserts_row(self, pipeline_env):
        from src.utils.lineage import record_lineage

        with patch("psycopg.connect") as connect:
            record_lineage(
                run_id="ingest_api_20260618_100812",
                source_system="mock_api",
                source_table="orders",
                target_table="stg_raw_orders",
                rows_written=1378,
                transformation_applied="api_pagination+watermark",
            )
        params = connect.return_value.cursor.return_value.execute.call_args.args[1]
        assert params == (
            "ingest_api_20260618_100812",
            "mock_api",
            "orders",
            "stg_raw_orders",
            1378,
            "api_pagination+watermark",
        )
        connect.return_value.commit.assert_called_once()

    def test_get_lineage_by_run_shape(self, pipeline_env):
        from src.utils.lineage import get_lineage_by_run

        columns = [
            "id",
            "run_id",
            "source_system",
            "source_table",
            "target_table",
            "rows_written",
            "transformation_applied",
            "recorded_at",
        ]
        rows = [
            (
                1,
                "run_x",
                "csv",
                "data/raw/orders.csv",
                "stg_raw_orders",
                3,
                "csv_chunked_load",
                None,
            )
        ]
        with patch("psycopg.connect") as connect:
            connect.return_value.cursor.return_value = _cursor_with(rows, columns)
            result = get_lineage_by_run(run_id="run_x")
        assert result == [dict(zip(columns, rows[0]))]
        assert result[0]["target_table"] == "stg_raw_orders"

    def test_get_lineage_for_table_shape(self, pipeline_env):
        from src.utils.lineage import get_lineage_for_table

        columns = [
            "id",
            "run_id",
            "source_system",
            "source_table",
            "target_table",
            "rows_written",
            "transformation_applied",
            "recorded_at",
        ]
        rows = [
            (
                2,
                "run_y",
                "mysql_staging",
                "stg_raw_orders",
                "warehouse.fact_orders",
                5,
                "clean+dedupe+enrich+validate",
                None,
            )
        ]
        with patch("psycopg.connect") as connect:
            cursor = _cursor_with(rows, columns)
            connect.return_value.cursor.return_value = cursor
            result = get_lineage_for_table(table_name="warehouse.fact_orders")
        assert result == [dict(zip(columns, rows[0]))]
        params = cursor.execute.call_args.args[1]
        assert params == (
            ["fact_orders", "warehouse.fact_orders"],
            ["fact_orders", "warehouse.fact_orders"],
        )

    def test_get_metrics_shape(self, pipeline_env):
        from src.utils.metrics import get_recent_metrics

        columns = ["pipeline_name", "status", "rows_loaded"]
        with patch("psycopg.connect") as connect:
            connect.return_value.cursor.return_value = _cursor_with(
                [("p", "success", 9)], columns
            )
            result = get_recent_metrics("p", hours=6)
        assert result == [{"pipeline_name": "p", "status": "success", "rows_loaded": 9}]


# =============================================================================
# Call sites: lineage wiring in ingestion and the Spark job
# =============================================================================
ORDERS_COLUMNS = [
    "source_order_id",
    "user_id",
    "product_id",
    "quantity",
    "unit_price",
    "order_status",
    "order_date",
]


class TestLineageCallSites:
    def test_csv_ingest_records_one_lineage_edge(self, pipeline_env, tmp_path):
        from src.extract import ingest_csv

        pipeline_env.chdir(tmp_path)  # process_csv_file moves files to data/processed
        csv_path = tmp_path / "orders_batch_test.csv"
        pd.DataFrame(
            {
                "source_order_id": [1, 2, 3],
                "user_id": [10, 11, 12],
                "product_id": [100, 101, 102],
                "quantity": [1, 2, 3],
                "unit_price": [9.5, 4.0, 2.25],
                "order_status": ["completed"] * 3,
                "order_date": ["2026-06-01"] * 3,
            }
        ).to_csv(csv_path, index=False)

        with patch.object(ingest_csv, "record_lineage") as record, patch.object(
            ingest_csv, "get_mysql_engine", return_value=MagicMock()
        ), patch.object(pd.DataFrame, "to_sql", return_value=None):
            result = ingest_csv.process_csv_file(str(csv_path), "orders")

        assert result["success"] is True
        record.assert_called_once()
        kwargs = record.call_args.kwargs
        assert kwargs["source_system"] == "csv"
        assert kwargs["source_table"] == str(csv_path)
        assert kwargs["target_table"] == "stg_raw_orders"
        assert kwargs["rows_written"] == 3
        assert kwargs["transformation_applied"] == "csv_chunked_load"

    def test_csv_ingest_continues_when_lineage_fails(self, pipeline_env, tmp_path):
        from src.extract import ingest_csv

        pipeline_env.chdir(tmp_path)
        csv_path = tmp_path / "orders_batch_fail.csv"
        pd.DataFrame({c: [1] for c in ORDERS_COLUMNS}).to_csv(csv_path, index=False)

        with patch.object(
            ingest_csv, "record_lineage", side_effect=ConnectionError("lineage down")
        ), patch.object(
            ingest_csv, "get_mysql_engine", return_value=MagicMock()
        ), patch.object(
            pd.DataFrame, "to_sql", return_value=None
        ):
            result = ingest_csv.process_csv_file(str(csv_path), "orders")
        assert result["success"] is True

    def _run_etl_with_mocks(self, transform, record_side_effect=None):
        valid_df = MagicMock()
        valid_df.count.return_value = 5
        quarantine_df = MagicMock()
        quarantine_df.count.return_value = 0
        source_df = MagicMock()
        source_df.count.return_value = 10

        args = argparse.Namespace(batch_id="batch_test", env="dev", max_records=None)
        with patch("psycopg.connect"), patch.object(
            transform, "SparkSession"
        ), patch.object(
            transform, "read_unprocessed_orders", return_value=source_df
        ), patch.object(
            transform,
            "read_dimension_tables",
            return_value=(MagicMock(), MagicMock(), MagicMock()),
        ), patch.object(
            transform, "clean_orders", return_value=MagicMock()
        ), patch.object(
            transform, "enrich_orders", return_value=MagicMock()
        ), patch.object(
            transform, "validate_and_split_data", return_value=(valid_df, quarantine_df)
        ), patch.object(
            transform, "load_to_fact_orders", return_value=5
        ), patch.object(
            transform, "update_processed_flag", return_value=5
        ), patch.object(
            transform, "record_lineage", side_effect=record_side_effect
        ) as record:
            summary = transform.run_etl(args)
        return summary, record

    def test_transform_orders_records_lineage_once(self, pipeline_env):
        from src.spark_jobs import transform_orders

        summary, record = self._run_etl_with_mocks(transform_orders)
        assert summary["status"] == "success"
        record.assert_called_once()
        kwargs = record.call_args.kwargs
        assert (
            kwargs["run_id"] == "run_test_001"
        )  # PIPELINE_RUN_ID takes precedence over batch_id
        assert kwargs["source_system"] == "mysql_staging"
        assert kwargs["source_table"] == "stg_raw_orders"
        assert kwargs["target_table"] == "warehouse.fact_orders"
        assert kwargs["rows_written"] == 5
        assert kwargs["transformation_applied"] == "clean+dedupe+enrich+validate"

    def test_transform_orders_survives_lineage_failure(self, pipeline_env):
        from src.spark_jobs import transform_orders

        summary, record = self._run_etl_with_mocks(
            transform_orders, record_side_effect=ConnectionError("lineage down")
        )
        assert summary["status"] == "success"
        record.assert_called_once()


# =============================================================================
# Phase 9.1 extension: wiring for ingest_api, metrics alias, lineage name matching
# =============================================================================
def _api_order(order_id="o1"):
    return {
        "order_id": order_id,
        "line_id": "l1",
        "user_id": 1,
        "product_id": 2,
        "quantity": 1,
        "unit_price": 5.0,
        "discount": 0.0,
        "order_status": "completed",
        "order_date": "2026-06-01T10:00:00Z",
    }


class TestIngestApiLineage:
    def _ingest(self, records):
        from src.extract import ingest_api

        client = MagicMock()
        client.get.return_value = {
            "data": records,
            "total": len(records),
            "page": 1,
            "limit": 500,
            "total_pages": 1,
        }
        with patch.object(ingest_api, "APIClient", return_value=client), patch.object(
            ingest_api, "start_pipeline_run", return_value="run_api_1"
        ), patch.object(
            ingest_api, "generate_batch_id", return_value="batch_1"
        ), patch.object(
            ingest_api, "get_watermark", return_value=None
        ), patch.object(
            ingest_api, "write_raw_data", return_value="data/raw/api/orders/x.json"
        ), patch.object(
            ingest_api, "update_watermark"
        ), patch.object(
            ingest_api, "complete_pipeline_run"
        ), patch.object(
            ingest_api, "record_lineage"
        ) as record:
            ingest_api.ingest_orders_from_api("http://api", limit=500, days_back=7)
        return record

    def test_zero_row_batch_records_no_lineage(self, pipeline_env):
        record = self._ingest([])
        record.assert_not_called()

    def test_written_batch_records_one_edge_with_run_id(self, pipeline_env):
        record = self._ingest([_api_order("o1"), _api_order("o2")])
        record.assert_called_once()
        kwargs = record.call_args.kwargs
        assert kwargs["run_id"] == "run_api_1"
        assert kwargs["source_system"] == "mock_api"
        assert kwargs["source_table"] == "orders"
        assert kwargs["target_table"] == "raw/api/orders"
        assert kwargs["rows_written"] == 2
        assert kwargs["transformation_applied"] == "api_pagination+watermark"


class TestPhase91Extension:
    def test_rows_read_is_alias_for_rows_extracted(self, pipeline_env):
        from src.utils.metrics import RunMetrics

        m = RunMetrics("p", "r")
        m.rows_read = 9
        assert m.rows_extracted == 9
        assert m.rows_read == 9

    def test_table_name_variants_cover_both_forms(self, pipeline_env):
        from src.utils.lineage import table_name_variants

        assert "warehouse.fact_orders" in table_name_variants("fact_orders")
        assert "fact_orders" in table_name_variants("warehouse.fact_orders")
