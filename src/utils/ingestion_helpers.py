"""
Shared utilities for all ingestion scripts
"""

import json
import logging
import os
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import mysql.connector
from sqlalchemy import create_engine

# ============================================
# LOGGING SETUP - Structured JSON logging
# ============================================


class JSONLogFormatter(logging.Formatter):
    """Custom formatter that outputs JSON format for logs"""

    def format(self, record):
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "message": record.getMessage(),
        }
        if hasattr(record, "extra"):
            log_entry.update(record.extra)
        return json.dumps(log_entry)


def setup_logging(module_name: str, log_level: str = "INFO"):
    """Configure structured logging for ingestion scripts"""
    logger = logging.getLogger(module_name)
    logger.setLevel(log_level)

    # Console handler
    handler = logging.StreamHandler()
    handler.setFormatter(JSONLogFormatter())
    logger.addHandler(handler)

    return logger


# ============================================
# DATABASE HELPERS
# ============================================


def _required_env(name: str) -> str:
    """Credentials have no built-in default. Fail with a clear message if unset."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"{name} must be set in the environment (no default is built in)"
        )
    return value


def get_mysql_connection():
    """Create MySQL database connection"""
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", 3306)),
        user=os.getenv("MYSQL_USER", "root"),
        password=_required_env("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE", "ecommerce_staging"),
    )


def get_mysql_engine():
    """Create SQLAlchemy engine"""
    connection_string = f"mysql+mysqlconnector://{os.getenv('MYSQL_USER', 'root')}:{_required_env('MYSQL_PASSWORD')}@{os.getenv('MYSQL_HOST', 'localhost')}:{int(os.getenv('MYSQL_PORT', 3306))}/{os.getenv('MYSQL_DATABASE', 'ecommerce_staging')}"  # noqa: E501
    return create_engine(connection_string)


# ============================================
# WATERMARK MANAGEMENT
# ============================================


def get_watermark(pipeline_name: str) -> Optional[datetime]:
    """Get last extracted timestamp for a pipeline"""
    conn = get_mysql_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT last_extracted_at FROM watermarks WHERE pipeline_name = %s",
        (pipeline_name,),
    )
    result = cursor.fetchone()

    cursor.close()
    conn.close()

    if result:
        return result[0]
    return None  # First run - return None for initial full load


def update_watermark(pipeline_name: str, last_extracted_at: datetime):
    """Update watermark after successful extraction"""
    conn = get_mysql_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO watermarks (pipeline_name, last_extracted_at)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE last_extracted_at = %s
        """,
        (pipeline_name, last_extracted_at, last_extracted_at),
    )
    conn.commit()

    cursor.close()
    conn.close()


# ============================================
# PIPELINE RUN MANAGEMENT
# ============================================


def start_pipeline_run(pipeline_name: str, source_system: str, batch_id: str) -> str:
    """Record the start of a pipeline run"""
    # Microseconds + random suffix so same-second runs cannot collide on the PK
    run_id = f"{pipeline_name}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}_{uuid.uuid4().hex[:6]}"
    conn = get_mysql_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO pipeline_runs (run_id, pipeline_name, source_system, batch_id, start_time, status)
        VALUES (%s, %s, %s, %s, %s, 'running')
        """,
        (run_id, pipeline_name, source_system, batch_id, datetime.now()),
    )
    conn.commit()

    cursor.close()
    conn.close()
    return run_id


def complete_pipeline_run(
    run_id: str,
    status: str,
    rows_extracted: int = 0,
    rows_loaded: int = 0,
    error_message: str = None,
):
    """Record completion of a pipeline run"""
    conn = get_mysql_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE pipeline_runs
        SET end_time = %s, status = %s, rows_extracted = %s, rows_loaded = %s, error_message = %s
        WHERE run_id = %s
        """,
        (datetime.now(), status, rows_extracted, rows_loaded, error_message, run_id),
    )
    conn.commit()

    cursor.close()
    conn.close()


# ============================================
# BATCH ID GENERATOR
# ============================================


def generate_batch_id():
    """Generate unique batch ID"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"batch_{timestamp}"


# ============================================
# S3 / LOCAL STORAGE HELPERS
# ============================================


def write_raw_data(
    data: Dict[str, Any],
    prefix: str,
    filename: str,
    use_s3: bool = False,
    bucket: str = None,
):
    """
    Write raw data to S3 or local storage.
    If AWS credentials exist, writes to S3. Otherwise falls back to local.
    """
    if use_s3 or (
        os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY")
    ):
        try:
            import boto3

            s3 = boto3.client("s3")
            bucket = bucket or os.getenv("S3_BUCKET", "ecommerce-data-lake")
            key = f"{prefix}/{filename}"
            s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(data))
            return f"s3://{bucket}/{key}"
        except Exception:
            # Fallback to local
            return write_raw_data_local(data, prefix, filename)
    else:
        return write_raw_data_local(data, prefix, filename)


def write_raw_data_local(data: Dict[str, Any], prefix: str, filename: str):
    """Write raw data to local filesystem"""
    os.makedirs(f"data/raw/{prefix}", exist_ok=True)
    filepath = f"data/raw/{prefix}/{filename}"
    with open(filepath, "w") as f:
        json.dump(data, f, default=str, indent=2)
    return filepath


# ============================================
# CONTEXT MANAGER FOR DATABASE CONNECTIONS
# ============================================


@contextmanager
def db_connection():
    """Context manager for database connections"""
    conn = get_mysql_connection()
    try:
        yield conn
    finally:
        conn.close()
