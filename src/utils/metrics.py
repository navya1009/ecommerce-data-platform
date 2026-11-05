"""
Per-run pipeline metrics.

    with track_metrics(pipeline_name="spark_transform_orders", run_id=run_id) as m:
        m.rows_loaded = 123

On exit one row is written to warehouse.pipeline_metrics, and (outside local
or when AWS_REGION is unset) the values are pushed to CloudWatch custom metrics.
A failed run is recorded with status='failed' and the exception is re-raised.
Recording and CloudWatch problems are logged, never raised, so observability
cannot fail the pipeline.
"""

import os
import time
from contextlib import contextmanager
from datetime import datetime, timezone

import boto3

from .logger import get_logger
from .warehouse_conn import connect_warehouse, rows_to_dicts

logger = get_logger(__name__)

METRIC_COLUMNS = (
    "pipeline_run_id",
    "pipeline_name",
    "task_name",
    "status",
    "rows_extracted",
    "rows_transformed",
    "rows_loaded",
    "rows_quarantined",
    "processing_time_seconds",
    "data_freshness_lag_minutes",
    "recorded_at",
)

INSERT_SQL = f"""
    INSERT INTO warehouse.pipeline_metrics ({", ".join(METRIC_COLUMNS)})
    VALUES ({", ".join(["%s"] * len(METRIC_COLUMNS))})
"""


class RunMetrics:
    """Mutable bag the caller fills in while the run is in progress."""

    def __init__(self, pipeline_name: str, run_id: str, task_name: str = None):
        self.pipeline_name = pipeline_name
        self.run_id = run_id
        self.task_name = task_name
        self.status = "success"
        self.rows_extracted = 0
        self.rows_transformed = 0
        self.rows_loaded = 0
        self.rows_quarantined = 0
        self.data_freshness_lag_minutes = None
        self.processing_time_seconds = None
        self.recorded_at = datetime.now(timezone.utc)

    @property
    def rows_read(self) -> int:
        """Alias for rows_extracted: rows the job read from its sources."""
        return self.rows_extracted

    @rows_read.setter
    def rows_read(self, value: int) -> None:
        self.rows_extracted = value


def current_environment() -> str:
    return os.getenv("ENVIRONMENT", "local").lower()


def _cloudwatch_enabled(environment: str) -> bool:
    return environment != "local" and bool(os.getenv("AWS_REGION"))


def _insert_row(m: RunMetrics) -> None:
    conn = connect_warehouse()
    try:
        cursor = conn.cursor()
        cursor.execute(
            INSERT_SQL,
            (
                m.run_id,
                m.pipeline_name,
                m.task_name,
                m.status,
                m.rows_extracted,
                m.rows_transformed,
                m.rows_loaded,
                m.rows_quarantined,
                m.processing_time_seconds,
                m.data_freshness_lag_minutes,
                m.recorded_at,
            ),
        )
        cursor.close()
        conn.commit()
    finally:
        conn.close()


def _push_to_cloudwatch(m: RunMetrics, environment: str) -> None:
    """Send one datum per metric. Failures are logged and swallowed."""
    if not _cloudwatch_enabled(environment):
        return
    data = [
        ("RowsExtracted", m.rows_extracted, "Count"),
        ("RowsTransformed", m.rows_transformed, "Count"),
        ("RowsLoaded", m.rows_loaded, "Count"),
        ("RowsQuarantined", m.rows_quarantined, "Count"),
        ("ProcessingTimeSeconds", m.processing_time_seconds, "Seconds"),
        ("PipelineRunSuccess", 1 if m.status == "success" else 0, "Count"),
    ]
    if m.data_freshness_lag_minutes is not None:
        data.append(("DataFreshnessLagMinutes", m.data_freshness_lag_minutes, "None"))

    try:
        client_kwargs = {"region_name": os.getenv("AWS_REGION")}
        endpoint_url = os.getenv("AWS_ENDPOINT_URL")
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url
        client = boto3.client("cloudwatch", **client_kwargs)
        client.put_metric_data(
            Namespace=f"EcommerceDataPlatform/{environment}",
            MetricData=[
                {"MetricName": name, "Value": float(value), "Unit": unit}
                for name, value, unit in data
                if value is not None
            ],
        )
    except Exception as exc:  # noqa: BLE001 - never raise from metrics
        logger.warning(
            "CloudWatch metric push failed",
            extra={"extra": {"pipeline_name": m.pipeline_name, "error": str(exc)}},
        )


def _finish(m: RunMetrics, started_at: float) -> None:
    m.processing_time_seconds = round(time.perf_counter() - started_at, 3)
    try:
        _insert_row(m)
    except Exception as exc:  # noqa: BLE001 - metrics must not fail the run
        logger.error(
            "Failed to write pipeline_metrics row",
            extra={"extra": {"pipeline_name": m.pipeline_name, "error": str(exc)}},
        )
    _push_to_cloudwatch(m, current_environment())


@contextmanager
def track_metrics(pipeline_name: str, run_id: str = None, task_name: str = None):
    """Track one pipeline run. run_id defaults to the PIPELINE_RUN_ID env var."""
    m = RunMetrics(
        pipeline_name=pipeline_name,
        run_id=run_id or os.getenv("PIPELINE_RUN_ID") or "local",
        task_name=task_name,
    )
    started_at = time.perf_counter()
    try:
        yield m
    except Exception:
        m.status = "failed"
        _finish(m, started_at)
        raise
    _finish(m, started_at)


def get_recent_metrics(pipeline_name: str, hours: int = 24) -> list:
    """Metric rows for one pipeline from the last `hours` hours, newest first."""
    conn = connect_warehouse()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT * FROM warehouse.pipeline_metrics
            WHERE pipeline_name = %s
              AND recorded_at >= NOW() - (%s * INTERVAL '1 hour')
            ORDER BY recorded_at DESC
            """,
            (pipeline_name, hours),
        )
        rows = rows_to_dicts(cursor)
        cursor.close()
        return rows
    finally:
        conn.close()
