# isort: skip_file
"""
Extracts changed records from PostgreSQL using high-watermark on updated_at
"""

import os
import sys
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.ingestion_helpers import generate_batch_id  # noqa: E402
from utils.ingestion_helpers import get_watermark  # noqa: E402
from utils.ingestion_helpers import (  # noqa: E402
    complete_pipeline_run,
    setup_logging,
    start_pipeline_run,
    update_watermark,
)

load_dotenv()

# ============================================
# CONFIGURATION
# ============================================

BATCH_SIZE = 10000  # Records per batch

# ============================================
# DATABASE CONNECTIONS
# ============================================


def _required_env(name: str) -> str:
    """Credentials have no built-in default. Fail with a clear message if unset."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"{name} must be set in the environment (no default is built in)"
        )
    return value


def get_postgres_engine():
    """Create PostgreSQL connection for source"""
    # Change from localhost to postgres (the Docker service name)
    connection_string = f"postgresql://{os.getenv('PG_SOURCE_USER', 'admin')}:{_required_env('PG_SOURCE_PASSWORD')}@{os.getenv('PG_SOURCE_HOST', 'postgres')}:{int(os.getenv('PG_SOURCE_PORT', 5432))}/{os.getenv('PG_SOURCE_DB', 'postgres')}"  # noqa: E501
    return create_engine(connection_string)


def get_mysql_engine():
    """Create MySQL connection for staging"""
    connection_string = f"mysql+mysqlconnector://{os.getenv('MYSQL_USER', 'root')}:{_required_env('MYSQL_PASSWORD')}@{os.getenv('MYSQL_HOST', 'localhost')}:{int(os.getenv('MYSQL_PORT', 3306))}/{os.getenv('MYSQL_DATABASE', 'ecommerce_staging')}"  # noqa: E501
    return create_engine(connection_string)


# ============================================
# MAIN CDC FUNCTION
# ============================================


def extract_from_postgres(
    table_name: str,
    primary_key: str,
    updated_column: str = "updated_at",
    chunk_size: int = BATCH_SIZE,
    logger=None,
):
    """
    Extract new/changed records from PostgreSQL using high-watermark

    Args:
        table_name: Source table name
        primary_key: Primary key column name
        updated_column: Column tracking last update timestamp
        chunk_size: Number of records per batch
        logger: Logger instance
    """
    if logger is None:
        logger = setup_logging("ingest_cdc")

    batch_id = generate_batch_id()
    run_id = start_pipeline_run("ingest_cdc", "postgres_source", batch_id)

    logger.info(
        "Starting PostgreSQL CDC extraction",
        extra={"extra": {"table": table_name, "batch_id": batch_id, "run_id": run_id}},
    )

    pg_engine = get_postgres_engine()
    mysql_engine = get_mysql_engine()

    try:
        # Get watermark
        watermark_key = f"postgres_{table_name}"
        last_extracted = get_watermark(watermark_key)

        # Build query
        if last_extracted is None:
            # Initial full load
            query = f"""
                SELECT * FROM {table_name}
                ORDER BY {primary_key}
            """
            logger.info("Initial full load - no watermark found")
        else:
            # Incremental load
            query = f"""
                SELECT * FROM {table_name}
                WHERE {updated_column} > :watermark
                ORDER BY {updated_column}
            """
            logger.info(
                "Incremental load",
                extra={"extra": {"watermark": last_extracted.isoformat()}},
            )

        # Extract in chunks
        all_rows = []
        total_rows = 0

        with pg_engine.connect() as conn:
            if last_extracted:
                result = conn.execute(text(query), {"watermark": last_extracted})
            else:
                result = conn.execute(text(query))

            # Process in chunks
            while True:
                chunk = result.fetchmany(chunk_size)
                if not chunk:
                    break

                rows = [dict(row._mapping) for row in chunk]
                all_rows.extend(rows)
                total_rows += len(rows)

                logger.info(
                    "Chunk extracted",
                    extra={
                        "extra": {"chunk_size": len(rows), "total_rows": total_rows}
                    },
                )

                # Write chunk to MySQL staging
                df = pd.DataFrame(rows)
                df["ingested_at"] = datetime.now()
                df["batch_id"] = batch_id
                df["is_processed"] = False

                # Map to staging table
                staging_table = f"stg_{table_name}"
                df.to_sql(staging_table, mysql_engine, if_exists="append", index=False)

        # Update watermark with latest timestamp
        if all_rows:
            latest_timestamp = max(row.get(updated_column) for row in all_rows)
            update_watermark(watermark_key, latest_timestamp)
            logger.info(
                "Watermark updated",
                extra={"extra": {"new_watermark": latest_timestamp.isoformat()}},
            )

        complete_pipeline_run(
            run_id, "success", rows_extracted=total_rows, rows_loaded=total_rows
        )

        return {
            "success": True,
            "run_id": run_id,
            "batch_id": batch_id,
            "records_extracted": total_rows,
        }

    except Exception as e:
        logger.error("CDC extraction failed", extra={"extra": {"error": str(e)}})
        complete_pipeline_run(run_id, "failed", error_message=str(e))
        raise


# ============================================
# COMMAND LINE EXECUTION
# ============================================

if __name__ == "__main__":
    logger = setup_logging("ingest_cdc")

    # Extract from users table
    result = extract_from_postgres("users", "user_id", logger=logger)
    print(result)
