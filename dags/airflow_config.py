"""
Airflow configuration for the e-commerce data pipeline
"""

from datetime import timedelta

from callbacks import on_failure_callback, on_retry_callback, on_success_callback

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-team@company.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": on_failure_callback,
    "on_retry_callback": on_retry_callback,
    "on_success_callback": on_success_callback,
}

# DAG configuration
DAG_ID = "ecommerce_daily_pipeline"
SCHEDULE_INTERVAL = "0 1 * * *"  # Daily at 01:00 UTC
START_DATE = "2024-01-01"

# API configuration
API_BASE_URL = "http://mock-api:8000"
API_HEALTH_ENDPOINT = f"{API_BASE_URL}/health"

# Spark configuration
SPARK_MASTER = "spark://spark-master:7077"
SPARK_JOBS_DIR = "/app/src/spark_jobs"

# Slack configuration
SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
SLACK_CHANNEL = "#data-pipeline-alerts"
