# isort: skip_file
"""
Watches for new CSV files and ingests them into staging
"""

import json
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.ingestion_helpers import get_mysql_engine  # noqa: E402
from utils.ingestion_helpers import generate_batch_id, setup_logging  # noqa: E402
from utils.lineage import record_lineage  # noqa: E402

load_dotenv()

# ============================================
# CONFIGURATION
# ============================================

CSV_CHUNK_SIZE = int(os.getenv("CSV_CHUNK_SIZE", 10000))
SQL_INSERT_CHUNK_SIZE = int(os.getenv("SQL_INSERT_CHUNK_SIZE", 1000))

# Expected columns for each CSV type
EXPECTED_SCHEMAS = {
    "products": {
        "required": ["product_id", "name", "category", "brand", "selling_price"],
        "optional": ["cost_price", "subcategory", "is_available"],
    },
    "inventory": {
        "required": ["product_id", "warehouse_id", "quantity_on_hand"],
        "optional": ["warehouse_name", "reorder_level", "last_updated"],
    },
    "orders": {
        "required": [
            "source_order_id",
            "user_id",
            "product_id",
            "quantity",
            "unit_price",
            "order_status",
            "order_date",
        ],
        "optional": ["discount", "source_line_id", "total_amount"],
    },
    "clickstream": {
        "required": ["session_id", "event_type", "event_timestamp"],
        "optional": [
            "event_id",
            "user_id",
            "product_id",
            "page_url",
            "device_type",
            "browser_name",
        ],
    },
}

# ============================================
# CSV VALIDATION
# ============================================


def validate_csv_headers(filepath: str, file_type: str) -> Tuple[bool, str]:
    """
    Validate CSV headers match expected schema
    Returns True if valid, False otherwise
    """
    df = pd.read_csv(filepath, nrows=1)
    actual_columns = set(df.columns)

    expected = EXPECTED_SCHEMAS.get(file_type)
    if not expected:
        return False, f"Unknown file type: {file_type}"

    required = set(expected.get("required", []))

    # Check all required columns exist
    if not required.issubset(actual_columns):
        missing = required - actual_columns
        return False, f"Missing required columns: {missing}"

    return True, "Valid"


def quarantine_invalid_file(filepath: str, error_message: str):
    """Move invalid file to quarantine with error log"""
    quarantine_dir = Path("data/quarantine")
    quarantine_dir.mkdir(parents=True, exist_ok=True)

    # Move file
    filename = Path(filepath).name
    dest = quarantine_dir / filename
    shutil.move(filepath, dest)

    # Write error log
    error_log = quarantine_dir / f"{filename}.error.log"
    with open(error_log, "w") as f:
        json.dump(
            {
                "filename": filename,
                "error": error_message,
                "timestamp": datetime.now().isoformat(),
            },
            f,
            indent=2,
        )


def process_csv_file(filepath: str, file_type: str, logger=None):
    """
    Process a single CSV file:
    1. Validate headers
    2. Read data
    3. Write to MySQL staging
    4. Move to processed folder
    """
    if logger is None:
        logger = setup_logging("ingest_csv")

    batch_id = generate_batch_id()

    logger.info(
        "Processing CSV file",
        extra={
            "extra": {
                "filepath": filepath,
                "file_type": file_type,
                "batch_id": batch_id,
            }
        },
    )

    # Validate headers
    is_valid, error_msg = validate_csv_headers(filepath, file_type)

    if not is_valid:
        logger.error(
            "Invalid CSV headers",
            extra={"extra": {"filepath": filepath, "error": error_msg}},
        )
        quarantine_invalid_file(filepath, error_msg)
        return {"success": False, "error": error_msg}

    try:
        # Write to MySQL
        engine = get_mysql_engine()
        staging_table = f"stg_raw_{file_type}"
        total_records = 0

        for chunk_number, df in enumerate(
            pd.read_csv(filepath, chunksize=CSV_CHUNK_SIZE), start=1
        ):
            # Add metadata columns
            df["ingested_at"] = datetime.now()
            df["batch_id"] = batch_id
            df["is_processed"] = False

            df.to_sql(
                staging_table,
                engine,
                if_exists="append",
                index=False,
                chunksize=SQL_INSERT_CHUNK_SIZE,
            )

            total_records += len(df)
            logger.info(
                "CSV chunk loaded",
                extra={
                    "extra": {
                        "filepath": filepath,
                        "chunk_number": chunk_number,
                        "chunk_records": len(df),
                        "total_records": total_records,
                    }
                },
            )

        # Move to processed
        processed_dir = Path("data/processed")
        processed_dir.mkdir(parents=True, exist_ok=True)
        dest = processed_dir / Path(filepath).name
        shutil.move(filepath, dest)

        logger.info(
            "File processed successfully",
            extra={
                "extra": {
                    "filepath": filepath,
                    "records": total_records,
                    "destination": str(dest),
                }
            },
        )

        # Lineage is best-effort: a lineage DB failure must not fail ingestion
        try:
            record_lineage(
                run_id=os.getenv("PIPELINE_RUN_ID") or batch_id,
                source_system="csv",
                source_table=filepath,
                target_table=staging_table,
                rows_written=total_records,
                transformation_applied="csv_chunked_load",
            )
        except Exception as lineage_error:
            logger.warning(
                "Lineage record failed, continuing",
                extra={"extra": {"filepath": filepath, "error": str(lineage_error)}},
            )

        return {
            "success": True,
            "filepath": filepath,
            "records": total_records,
            "destination": str(dest),
        }

    except Exception as e:
        logger.error(
            "Error processing CSV",
            extra={"extra": {"filepath": filepath, "error": str(e)}},
        )
        quarantine_invalid_file(filepath, str(e))
        return {"success": False, "error": str(e)}


def detect_file_type(filepath: Path) -> Optional[str]:
    """Detect supported CSV type from either parent folder or filename."""
    folder_name = filepath.parent.name.lower()
    filename = filepath.name.lower()

    for file_type in EXPECTED_SCHEMAS:
        if file_type in filename or folder_name == file_type:
            return file_type

    return None


def watch_and_ingest_csv(watch_dir: str = "data/raw", logger=None):
    """Watch directory and process all new CSV files"""
    if logger is None:
        logger = setup_logging("ingest_csv")

    watch_path = Path(watch_dir)

    if not watch_path.exists():
        logger.warning(
            "CSV watch directory does not exist",
            extra={"extra": {"watch_dir": str(watch_path)}},
        )
        return []

    # Find all CSV files
    csv_files = list(watch_path.glob("**/*.csv"))

    if not csv_files:
        logger.info(
            "No CSV files found to ingest",
            extra={"extra": {"watch_dir": str(watch_path)}},
        )
        return []

    results = []
    for filepath in csv_files:
        file_type = detect_file_type(filepath)

        if file_type is None:
            logger.warning(
                "Unknown CSV file type", extra={"extra": {"filepath": str(filepath)}}
            )
            continue

        result = process_csv_file(str(filepath), file_type, logger)
        results.append(result)

    return results


# ============================================
# COMMAND LINE EXECUTION
# ============================================

if __name__ == "__main__":
    logger = setup_logging("ingest_csv")
    results = watch_and_ingest_csv(logger=logger)
    print(json.dumps(results, indent=2))
