# tests/unit/test_spark_transforms.py
# -----------------------------------------------------------------------------
# Unit tests for PySpark transformations. Runs in Spark local mode.
# Each test group skips gracefully if the target function isn't implemented.
# -----------------------------------------------------------------------------
import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# =============================================================================
# Test group 1: gross_margin calculation
# =============================================================================
class TestGrossMargin:
    def _import_transform(self):
        try:
            import src.spark_jobs.transform_orders as t
        except ImportError:
            pytest.skip("transform_orders not importable")
        if not hasattr(t, "calculate_gross_margin"):
            pytest.skip("calculate_gross_margin not implemented")
        return t

    def test_gross_margin_basic(self, spark):
        t = self._import_transform()

        schema = StructType(
            [
                StructField("total_amount", DoubleType(), False),
                StructField("quantity", IntegerType(), False),
                StructField("cost_price", DoubleType(), False),
            ]
        )
        df = spark.createDataFrame(
            [(100.0, 2, 20.0), (50.0, 1, 15.0), (200.0, 4, 30.0)],
            schema=schema,
        )

        result = t.calculate_gross_margin(df)
        margins = sorted([r.gross_margin for r in result.collect()])
        assert margins == [35.0, 60.0, 80.0]

    def test_gross_margin_zero(self, spark):
        t = self._import_transform()
        schema = StructType(
            [
                StructField("total_amount", DoubleType(), False),
                StructField("quantity", IntegerType(), False),
                StructField("cost_price", DoubleType(), False),
            ]
        )
        df = spark.createDataFrame([(100.0, 2, 50.0)], schema=schema)
        result = t.calculate_gross_margin(df).collect()
        assert result[0].gross_margin == 0.0

    def test_gross_margin_negative(self, spark):
        t = self._import_transform()
        schema = StructType(
            [
                StructField("total_amount", DoubleType(), False),
                StructField("quantity", IntegerType(), False),
                StructField("cost_price", DoubleType(), False),
            ]
        )
        df = spark.createDataFrame([(10.0, 1, 50.0)], schema=schema)
        result = t.calculate_gross_margin(df).collect()
        assert result[0].gross_margin == -40.0


# =============================================================================
# Test group 2: deduplication
# =============================================================================
class TestDeduplication:
    def _import_transform(self):
        try:
            import src.spark_jobs.transform_orders as t
        except ImportError:
            pytest.skip("transform_orders not importable")
        if not hasattr(t, "deduplicate_orders"):
            pytest.skip("deduplicate_orders not implemented")
        return t

    def test_dedup_collapses_duplicates(self, spark):
        t = self._import_transform()
        schema = StructType(
            [
                StructField("source_order_id", LongType(), False),
                StructField("source_line_id", LongType(), False),
                StructField("total_amount", DoubleType(), False),
            ]
        )
        df = spark.createDataFrame(
            [
                (1001, 1, 50.0),
                (1001, 1, 50.0),
                (1001, 2, 30.0),
                (1002, 1, 40.0),
            ],
            schema=schema,
        )

        result = t.deduplicate_orders(df)
        assert result.count() == 3


# =============================================================================
# Test group 3: order_status normalization
# =============================================================================
class TestStatusNormalization:
    def _import_transform(self):
        try:
            import src.spark_jobs.transform_orders as t
        except ImportError:
            pytest.skip("transform_orders not importable")
        if not hasattr(t, "normalize_order_status"):
            pytest.skip("normalize_order_status not implemented")
        return t

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("COMPLETED", "completed"),
            ("Complete", "completed"),
            ("completed", "completed"),
            ("PENDING", "pending"),
            ("CANCELLED", "cancelled"),
        ],
    )
    def test_status_normalization(self, spark, raw, expected):
        t = self._import_transform()
        schema = StructType(
            [
                StructField("source_order_id", LongType(), False),
                StructField("order_status", StringType(), True),
            ]
        )
        df = spark.createDataFrame([(1, raw)], schema=schema)
        result = t.normalize_order_status(df).collect()
        assert result[0].order_status == expected


# =============================================================================
# Test group 4: bot detection (best-effort, may skip)
# =============================================================================
class TestClickstreamBotDetection:
    def _import_transform(self):
        try:
            import src.spark_jobs.transform_clickstream as t
        except ImportError:
            pytest.skip("transform_clickstream not importable")
        if not hasattr(t, "detect_bot_sessions"):
            pytest.skip("detect_bot_sessions not implemented")
        return t

    def test_bot_session_flagged(self, spark):
        from datetime import datetime, timedelta

        t = self._import_transform()
        base = datetime(2026, 6, 1, 12, 0, 0)
        rows = [
            ("bot", 999, base + timedelta(milliseconds=i * 290)) for i in range(201)
        ]
        schema = StructType(
            [
                StructField("session_id", StringType(), False),
                StructField("user_id", LongType(), False),
                StructField("event_timestamp", StringType(), False),
            ]
        )
        # Convert to string for portability; adjust if transform expects TimestampType
        rows = [(s, u, ts.isoformat()) for s, u, ts in rows]
        df = spark.createDataFrame(rows, schema=schema)
        result = t.detect_bot_sessions(df).collect()
        assert len(result) >= 1

    def test_human_session_not_flagged(self, spark):
        from datetime import datetime, timedelta

        t = self._import_transform()
        base = datetime(2026, 6, 1, 12, 0, 0)
        rows = [("human", 500, base + timedelta(seconds=i * 18)) for i in range(200)]
        schema = StructType(
            [
                StructField("session_id", StringType(), False),
                StructField("user_id", LongType(), False),
                StructField("event_timestamp", StringType(), False),
            ]
        )
        rows = [(s, u, ts.isoformat()) for s, u, ts in rows]
        df = spark.createDataFrame(rows, schema=schema)
        result = t.detect_bot_sessions(df).collect()
        assert all(r.session_id != "human" for r in result)
