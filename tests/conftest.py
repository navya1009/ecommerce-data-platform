# tests/conftest.py
# -----------------------------------------------------------------------------
# Shared pytest fixtures for the entire test suite.
# -----------------------------------------------------------------------------
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Make src/ importable from tests/ without installing the package.
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# =============================================================================
# Spark fixtures
# =============================================================================
@pytest.fixture(scope="session")
def spark():
    """
    Session-scoped local SparkSession. Created once, reused by all Spark tests.
    Uses local[2] = 2 threads. Java must be installed.
    """
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("ecommerce-data-platform-tests")
        .config("spark.sql.shuffle.partitions", "2")  # fewer partitions = faster
        .config("spark.ui.enabled", "false")  # no UI, quieter
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def sample_orders_df(spark):
    """A small DataFrame matching the shape of stg_raw_orders."""
    from datetime import datetime

    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("source_order_id", LongType(), True),
            StructField("source_line_id", LongType(), True),
            StructField("user_id", LongType(), True),
            StructField("product_id", LongType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("discount", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("order_status", StringType(), True),
            StructField("order_date", TimestampType(), True),
        ]
    )

    rows = [
        # normal order
        (1001, 1, 501, 9001, 2, 50.0, 5.0, 95.0, "completed", datetime(2026, 6, 1)),
        # duplicate of first line (should be deduped)
        (1001, 1, 501, 9001, 2, 50.0, 5.0, 95.0, "completed", datetime(2026, 6, 1)),
        # second line of same order
        (1001, 2, 501, 9002, 1, 30.0, 0.0, 30.0, "completed", datetime(2026, 6, 1)),
        # negative total → should be quarantined
        (1002, 1, 502, 9003, 1, 20.0, 0.0, -20.0, "completed", datetime(2026, 6, 2)),
        # uppercase status → should normalize
        (1003, 1, 503, 9004, 1, 40.0, 0.0, 40.0, "COMPLETED", datetime(2026, 6, 3)),
        # mixed-case status → should normalize
        (1004, 1, 504, 9005, 1, 40.0, 0.0, 40.0, "Complete", datetime(2026, 6, 4)),
    ]
    return spark.createDataFrame(rows, schema=schema)


# =============================================================================
# Database fixtures (mocked)
# =============================================================================
@pytest.fixture
def mock_postgres_engine():
    """Returns a MagicMock that behaves like a SQLAlchemy engine."""
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = MagicMock()
    return engine


@pytest.fixture
def mock_mysql_engine():
    """Same as postgres, but for MySQL."""
    return MagicMock()


@pytest.fixture
def mock_watermark_table():
    """In-memory fake for the watermarks control table."""
    return {"last_extracted_at": None}


# =============================================================================
# HTTP fixtures
# =============================================================================
@pytest.fixture
def mock_requests_ok(monkeypatch):
    """Patches requests.get to always return a 200 with empty JSON."""
    from unittest.mock import MagicMock

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"orders": [], "next_page": None}
    mock_response.raise_for_status = MagicMock()

    mock_get = MagicMock(return_value=mock_response)
    monkeypatch.setattr("requests.get", mock_get)
    return mock_get


# =============================================================================
# Filesystem fixtures
# =============================================================================
@pytest.fixture
def tmp_csv_wrong_headers(tmp_path):
    """Creates a CSV file with mismatched headers for quarantine testing."""
    csv_file = tmp_path / "orders_bad.csv"
    csv_file.write_text("wrong_col,another_col,third\n" "1,2,3\n" "4,5,6\n")
    return csv_file


@pytest.fixture
def tmp_csv_valid(tmp_path):
    """Creates a valid orders CSV."""
    csv_file = tmp_path / "orders_good.csv"
    csv_file.write_text(
        "source_order_id,source_line_id,user_id,product_id,quantity,"
        "unit_price,discount,total_amount,order_status,order_date\n"
        "1001,1,501,9001,2,50.0,5.0,95.0,completed,2026-06-01T00:00:00\n"
    )
    return csv_file


# =============================================================================
# Environment fixtures
# =============================================================================
@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    """
    Ensures tests don't accidentally read real AWS credentials or DB hosts.
    autouse=True means this runs for every test.
    """
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:4566")
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("MYSQL_HOST", "localhost")
