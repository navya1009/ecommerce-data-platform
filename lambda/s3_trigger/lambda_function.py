"""
AWS Lambda function to trigger Airflow DAG on S3 file upload.

Triggered by: S3 ObjectCreated events on raw-data bucket
Action: Calls Airflow REST API to start ecommerce_daily_pipeline DAG

Environment Variables:
- AIRFLOW_API_URL: Base URL of Airflow REST API (e.g., http://airflow.example.com:8080)
- AIRFLOW_USERNAME: Airflow username for authentication
- AIRFLOW_PASSWORD: Airflow password for authentication
- DAG_ID: Airflow DAG ID to trigger (default: ecommerce_daily_pipeline)

Logs: All trigger attempts are logged to CloudWatch automatically (print/logging)
"""

import json
import logging
import os
from datetime import datetime
from urllib.parse import unquote_plus

import requests
from requests.auth import HTTPBasicAuth

# Setup logging (Lambda sends these to CloudWatch automatically)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuration from environment
AIRFLOW_API_URL = os.getenv("AIRFLOW_API_URL", "http://airflow:8080")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")
DAG_ID = os.getenv("DAG_ID", "ecommerce_daily_pipeline")


def lambda_handler(event, context):
    """
    Lambda entry point called by S3 event notification.

    Args:
        event: S3 event containing bucket/key info
        context: Lambda runtime context

    Returns:
        dict: Response with status code and details
    """
    logger.info(f"Received event: {json.dumps(event)}")

    # Extract S3 details from the event
    try:
        records = event.get("Records", [])
        if not records:
            logger.warning("No Records in event")
            return {"statusCode": 400, "body": "No records in event"}

        for record in records:
            # Validate event is from S3
            if record.get("eventSource") != "aws:s3":
                logger.warning(f"Skipping non-S3 event: {record.get('eventSource')}")
                continue

            bucket = record["s3"]["bucket"]["name"]
            key = unquote_plus(record["s3"]["object"]["key"])
            event_name = record.get("eventName", "Unknown")

            logger.info(f"S3 event: {event_name} for s3://{bucket}/{key}")

            # Trigger Airflow DAG
            result = trigger_airflow_dag(bucket, key, event_name)
            logger.info(f"Trigger result: {result}")

        return {"statusCode": 200, "body": "DAG trigger attempted"}

    except Exception as e:
        logger.exception(f"Error processing event: {e}")
        return {"statusCode": 500, "body": f"Error: {str(e)}"}


def trigger_airflow_dag(bucket, key, event_name):
    """
    Call Airflow REST API to trigger the DAG.

    Args:
        bucket: S3 bucket name
        key: S3 object key
        event_name: S3 event type (e.g., ObjectCreated:Put)

    Returns:
        dict: Trigger result with status and message
    """
    # Build the DAG run payload
    # Conf parameters are passed to the DAG as {{ dag_run.conf }}
    dag_run_conf = {
        "s3_bucket": bucket,
        "s3_key": key,
        "trigger_source": "s3_event",
        "trigger_event": event_name,
        "triggered_at": datetime.utcnow().isoformat() + "Z",
    }

    # Use date-based run_id to prevent duplicate runs for same S3 file
    # Airflow run_id must be unique per DAG
    run_id = f"s3_{bucket}_{key.replace('/', '_')}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

    payload = {"dag_run_id": run_id, "conf": dag_run_conf}

    # Airflow REST API endpoint
    url = f"{AIRFLOW_API_URL}/api/v1/dags/{DAG_ID}/dagRuns"

    try:
        logger.info(f"POST {url}")
        logger.info(f"Payload: {json.dumps(payload)}")

        response = requests.post(
            url,
            json=payload,
            auth=HTTPBasicAuth(AIRFLOW_USERNAME, AIRFLOW_PASSWORD),
            timeout=10,
            headers={"Content-Type": "application/json"},
        )

        # Handle response codes
        if response.status_code == 200:
            logger.info(f"DAG triggered successfully: {response.json()}")
            return {
                "status": "triggered",
                "run_id": run_id,
                "response": response.json(),
            }

        elif response.status_code == 409:
            # Conflict - DAG run with this run_id already exists
            # OR max_active_runs reached
            logger.warning(f"DAG run conflict (409): {response.text}")
            return {
                "status": "skipped",
                "reason": "duplicate_or_max_active_runs",
                "message": response.text,
            }

        else:
            logger.error(
                f"Failed to trigger DAG: {response.status_code} - {response.text}"
            )
            return {
                "status": "failed",
                "status_code": response.status_code,
                "message": response.text,
            }

    except requests.exceptions.ConnectionError as e:
        logger.error(f"Cannot connect to Airflow API at {AIRFLOW_API_URL}: {e}")
        return {"status": "failed", "reason": "connection_error", "message": str(e)}

    except requests.exceptions.Timeout:
        logger.error("Timeout calling Airflow API")
        return {"status": "failed", "reason": "timeout"}


# For local testing
if __name__ == "__main__":
    test_event = {
        "Records": [
            {
                "eventSource": "aws:s3",
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {"name": "ecommerce-raw-data"},
                    "object": {"key": "orders/orders_20260101.csv"},
                },
            }
        ]
    }
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2))
