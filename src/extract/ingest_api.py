# isort: skip_file
"""
Ingests order data from REST API with watermark-based incremental extraction
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List
from urllib.parse import urljoin

import requests
from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.ingestion_helpers import generate_batch_id  # noqa: E402
from utils.ingestion_helpers import get_watermark  # noqa: E402
from utils.ingestion_helpers import (  # noqa: E402
    complete_pipeline_run,
    start_pipeline_run,
    update_watermark,
    write_raw_data,
)
from utils.lineage import record_lineage  # noqa: E402
from utils.logger import get_logger  # noqa: E402

load_dotenv()

# ============================================
# PYDANTIC MODELS FOR SCHEMA VALIDATION
# ============================================


class OrderItemSchema(BaseModel):
    """Schema for individual order item"""

    order_id: str
    line_id: str
    user_id: int
    product_id: int
    quantity: int = Field(gt=0, le=100)  # Must be >0 and <=100
    unit_price: float = Field(gt=0)
    discount: float = Field(ge=0, le=100)
    order_status: str
    order_date: str


class OrderAPIResponse(BaseModel):
    """Schema for API response"""

    data: List[OrderItemSchema]
    total: int
    page: int
    limit: int
    total_pages: int


# ============================================
# API CLIENT WITH RETRY LOGIC
# ============================================


class APIClient:
    """API client with exponential backoff retry"""

    def __init__(self, base_url: str, retries: int = 3, backoff_factor: float = 1.0):
        self.base_url = base_url
        self.session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=retries,
            backoff_factor=backoff_factor,  # 1s, 2s, 4s, 8s
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def get(self, endpoint: str, params: Dict[str, Any] = None) -> Dict:
        """Make GET request with retries"""
        url = urljoin(self.base_url, endpoint)
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()


# ============================================
# MAIN INGESTION FUNCTION
# ============================================


def ingest_orders_from_api(
    api_base_url: str, limit: int = 1000, days_back: int = 7, logger=None
):
    """
    Ingests orders from API using watermark-based incremental extraction

    Args:
        api_base_url: Base URL of the API
        limit: Number of records per page
        days_back: Days to look back if no watermark exists (initial load)
        logger: Logger instance

    Returns:
        dict: Summary of ingestion results
    """
    if logger is None:
        logger = get_logger(__name__)

    batch_id = generate_batch_id()
    run_id = start_pipeline_run("ingest_api", "orders_api", batch_id)

    logger.info(
        "Starting API ingestion",
        extra={
            "batch_id": batch_id,
            "pipeline_run_id": run_id,
        },
    )

    try:
        # Get watermark (last extraction timestamp)
        last_extracted = get_watermark("ingest_api")

        if last_extracted is None:
            # First run: look back days_back days
            start_date = datetime.now() - timedelta(days=days_back)
            logger.info(
                "No watermark found - initial full load",
                extra={
                    "extra": {
                        "days_back": days_back,
                        "start_date": start_date.isoformat(),
                    }
                },
            )
        else:
            start_date = last_extracted
            logger.info(
                "Incremental load from watermark",
                extra={
                    "watermark": start_date.isoformat(),
                    "pipeline_run_id": run_id,
                },
            )

        # Prepare API client
        client = APIClient(api_base_url)

        # Get first page to determine total pages
        page_size = min(limit, 500)
        params = {"since": start_date.isoformat(), "page_size": page_size, "page": 1}

        response = client.get("/orders", params=params)
        validated_response = OrderAPIResponse(**response)

        total_pages = validated_response.total_pages
        all_records = []
        latest_timestamp = start_date

        logger.info(
            "Fetching pages",
            extra={
                "extra": {
                    "total_pages": total_pages,
                    "total_records": validated_response.total,
                }
            },
        )

        # Fetch all pages
        for page in range(1, total_pages + 1):
            params["page"] = page
            response = client.get("/orders", params=params)
            validated_response = OrderAPIResponse(**response)

            # Validate each record
            for record in validated_response.data:
                try:
                    if isinstance(record, OrderItemSchema):
                        validated = record
                    else:
                        validated = OrderItemSchema(**record)
                    record_dict = (
                        validated.model_dump()
                        if hasattr(validated, "model_dump")
                        else validated.dict()
                    )
                    all_records.append(record_dict)

                    # Track latest timestamp for watermark
                    record_date = (
                        datetime.fromisoformat(
                            record_dict["order_date"].replace("Z", "+00:00")
                        )
                        .astimezone(timezone.utc)
                        .replace(tzinfo=None)
                    )
                    if record_date > latest_timestamp:
                        latest_timestamp = record_date

                except ValidationError as e:
                    logger.warning(
                        "Invalid record skipped",
                        extra={"extra": {"record": record, "error": str(e)}},
                    )

            logger.info(
                "Page fetched",
                extra={
                    "extra": {"page": page, "records": len(validated_response.data)}
                },
            )

        # Write raw data to storage (S3 or local)
        if all_records:
            filename = f"orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            filepath = write_raw_data(
                all_records, prefix="api/orders", filename=filename
            )
            logger.info(
                "Raw data written",
                extra={
                    "filepath": filepath,
                    "rows": len(all_records),
                    "batch_id": batch_id,
                    "pipeline_run_id": run_id,
                },
            )

        # Update watermark
        if latest_timestamp > start_date:
            update_watermark("ingest_api", latest_timestamp)
            logger.info(
                "Watermark updated",
                extra={
                    "watermark": latest_timestamp.isoformat(),
                    "pipeline_run_id": run_id,
                },
            )

        # Lineage: record only batches that wrote rows (a zero-row run is not an edge).
        # The API job lands raw JSON files and does not write stg_raw_orders itself,
        # so the target is the raw storage prefix.
        if all_records:
            try:
                record_lineage(
                    run_id=run_id or batch_id,
                    source_system="mock_api",
                    source_table="orders",
                    target_table="raw/api/orders",
                    rows_written=len(all_records),
                    transformation_applied="api_pagination+watermark",
                )
            except Exception as lineage_error:
                logger.warning(
                    "Lineage record failed, continuing",
                    extra={"batch_id": batch_id, "error": str(lineage_error)},
                )

        # Complete pipeline run
        complete_pipeline_run(
            run_id,
            "success",
            rows_extracted=len(all_records),
            rows_loaded=len(all_records),
        )

        return {
            "success": True,
            "run_id": run_id,
            "batch_id": batch_id,
            "records_extracted": len(all_records),
            "latest_watermark": latest_timestamp.isoformat(),
        }

    except Exception as e:
        logger.error(
            "Pipeline failed",
            extra={
                "error": str(e),
                "error_type": type(e).__name__,
                "pipeline_run_id": run_id,
                "batch_id": batch_id,
            },
        )
        complete_pipeline_run(run_id, "failed", error_message=str(e))
        raise


# ============================================
# COMMAND LINE EXECUTION
# ============================================

if __name__ == "__main__":
    logger = get_logger(__name__)

    api_url = os.getenv("ORDERS_API_URL", "http://mock-api:8000")

    result = ingest_orders_from_api(api_url, logger=logger)
    print(json.dumps(result, indent=2))
