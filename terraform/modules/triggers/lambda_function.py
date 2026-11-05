# terraform/modules/triggers/lambda_function.py
"""
S3 -> Airflow trigger Lambda.
Fires when a new object lands in the raw-data bucket, then POSTs to the
Airflow REST API to kick off the ecommerce_daily_pipeline DAG.
"""
import json
import logging
import os
import urllib.error
import urllib.request

logger = logging.getLogger()
logger.setLevel(logging.INFO)

AIRFLOW_BASE_URL = os.environ.get("AIRFLOW_BASE_URL", "http://localhost:8080")
AIRFLOW_DAG_ID = os.environ.get("AIRFLOW_DAG_ID", "ecommerce_daily_pipeline")
AIRFLOW_USER = os.environ.get("AIRFLOW_USER", "admin")
AIRFLOW_PASSWORD = os.environ.get("AIRFLOW_PASSWORD", "admin")


def lambda_handler(event, context):
    logger.info("Received event: %s", json.dumps(event))

    triggered_dag_runs = []

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        # Build a unique dag_run_id so retries don't conflict.
        dag_run_id = f"s3_{bucket}_{key}".replace("/", "_").replace(".", "_")

        url = f"{AIRFLOW_BASE_URL}/api/v1/dags/{AIRFLOW_DAG_ID}/dagRuns"
        payload = json.dumps({"dag_run_id": dag_run_id}).encode("utf-8")

        req = urllib.request.Request(url, data=payload, method="POST")
        req.add_header("Content-Type", "application/json")

        # Basic auth
        import base64

        creds = base64.b64encode(f"{AIRFLOW_USER}:{AIRFLOW_PASSWORD}".encode()).decode()
        req.add_header("Authorization", f"Basic {creds}")

        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                body = resp.read().decode()
                logger.info("Airflow responded %s: %s", resp.status, body)
                triggered_dag_runs.append(
                    {"dag_run_id": dag_run_id, "status": resp.status}
                )
        except urllib.error.HTTPError as e:
            if e.code == 409:
                logger.warning("DAG run %s already exists — skipping.", dag_run_id)
                triggered_dag_runs.append(
                    {"dag_run_id": dag_run_id, "status": "duplicate"}
                )
            else:
                logger.error("HTTP error %s: %s", e.code, e.read().decode())
                raise
        except Exception as e:
            logger.error("Failed to trigger DAG: %s", e)
            raise

    return {
        "statusCode": 200,
        "body": json.dumps({"triggered": triggered_dag_runs}),
    }
