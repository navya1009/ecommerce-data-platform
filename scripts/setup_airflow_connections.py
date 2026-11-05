#!/usr/bin/env python3
"""
Programmatically set up Airflow Connections and Variables.

This script uses the Airflow REST API to create all needed connections
and variables so the setup is reproducible.

Usage:
    python scripts/setup_airflow_connections.py

Environment Variables:
    AIRFLOW_API_URL: Airflow REST API base URL (default: http://localhost:8080)
    AIRFLOW_USERNAME: Airflow username (default: admin)
    AIRFLOW_PASSWORD: Airflow password (default: admin)
"""
import json
import logging
import os
import sys
from typing import Any, Dict
from urllib.parse import urlparse

import requests
from requests.auth import HTTPBasicAuth

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# ============================================
# CONFIGURATION
# ============================================

AIRFLOW_API_URL = os.getenv("AIRFLOW_API_URL", "http://localhost:8080")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")
AUTH = HTTPBasicAuth(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)


# ============================================
# CONNECTIONS TO CREATE
# ============================================


def _env(name: str, default):
    """Environment value if set and non-empty, else the Docker-network default."""
    value = os.getenv(name)
    return value if value else default


def _slack_parts():
    """Host and scheme of SLACK_WEBHOOK_URL when set; the public Slack host otherwise."""
    url = os.getenv("SLACK_WEBHOOK_URL")
    if url:
        parsed = urlparse(url)
        return parsed.hostname, parsed.scheme or "https"
    return "hooks.slack.com", "https"


# Every value below defaults to the local Docker service. For production, set the
# environment variables to the RDS endpoints and the real credentials instead.
# Nothing here is hardcoded to Docker any more: the defaults are only the defaults.
_SLACK_HOST, _SLACK_SCHEME = _slack_parts()

CONNECTIONS = [
    {
        "connection_id": "postgres_dwh",
        "conn_type": "postgres",
        "host": _env("POSTGRES_HOST", "postgres"),
        "port": int(_env("POSTGRES_PORT", 5432)),
        "schema": _env("POSTGRES_DB", "ecommerce_warehouse"),
        "login": _env("POSTGRES_USER", "admin"),
        "password": _env("POSTGRES_PASSWORD", "postgres123"),
        "description": "PostgreSQL data warehouse",
    },
    {
        "connection_id": "mysql_staging",
        "conn_type": "mysql",
        "host": _env("MYSQL_HOST", "mysql"),
        "port": int(_env("MYSQL_PORT", 3306)),
        "schema": _env("MYSQL_DATABASE", "ecommerce_staging"),
        "login": _env("MYSQL_STAGING_USER", "root"),
        "password": _env("MYSQL_STAGING_PASSWORD", "root123"),
        "description": "MySQL staging database",
    },
    {
        "connection_id": "spark_default",
        "conn_type": "spark",
        "host": _env("SPARK_MASTER_HOST", "spark-master"),
        "port": int(_env("SPARK_MASTER_PORT", 7077)),
        "extra": json.dumps(
            {
                "deploy-mode": "client",
                "spark-binary": "spark-submit",
            }
        ),
        "description": "Spark cluster connection",
    },
    {
        "connection_id": "mock_api",
        "conn_type": "http",
        "host": _env("MOCK_API_HOST", "mock-api"),
        "port": int(_env("MOCK_API_PORT", 8000)),
        "schema": _env("MOCK_API_SCHEME", "http"),
        "description": "Mock order API",
    },
    {
        "connection_id": "aws_default",
        "conn_type": "aws",
        "login": _env("AWS_ACCESS_KEY_ID", "dummy_access_key"),
        "password": _env("AWS_SECRET_ACCESS_KEY", "dummy_secret_key"),
        "extra": json.dumps(
            {
                "region_name": _env("AWS_REGION", "us-east-1"),
            }
        ),
        "description": "AWS credentials for S3 access",
    },
    {
        "connection_id": "slack_webhook",
        "conn_type": "http",
        "host": _SLACK_HOST,
        "schema": _SLACK_SCHEME,
        "description": "Slack webhook for notifications",
    },
]


# ============================================
# VARIABLES TO CREATE
# ============================================

VARIABLES = [
    {
        "key": "env",
        "value": "dev",
        "description": "Environment name",
    },
    {
        "key": "s3_bucket",
        "value": "ecommerce-raw-data",
        "description": "S3 bucket for raw data",
    },
    {
        "key": "alert_email",
        "value": "data-team@company.com",
        "description": "Email for alerts",
    },
    {
        "key": "slack_webhook_url",
        "value": "https://hooks.slack.com/services/YOUR/WEBHOOK/URL",
        "description": "Slack webhook URL for notifications",
    },
    {
        "key": "consecutive_failures_threshold",
        "value": "3",
        "description": "Number of consecutive failures before self-healing",
    },
]


# ============================================
# API HELPERS
# ============================================


def api_request(method: str, path: str, data: Dict = None) -> requests.Response:
    """Make a request to Airflow REST API."""
    url = f"{AIRFLOW_API_URL}/api/v1/{path}"
    logger.debug(f"{method} {url}")

    response = requests.request(
        method=method,
        url=url,
        json=data,
        auth=AUTH,
        timeout=30,
        headers={"Content-Type": "application/json"},
    )
    return response


# ============================================
# CONNECTION MANAGEMENT
# ============================================


def connection_exists(conn_id: str) -> bool:
    """Check if a connection already exists."""
    response = api_request("GET", f"connections/{conn_id}")
    return response.status_code == 200


def create_connection(conn: Dict[str, Any]) -> bool:
    """Create a new Airflow connection."""
    conn_id = conn["connection_id"]

    if connection_exists(conn_id):
        logger.info(f"Connection '{conn_id}' already exists, updating...")
        response = api_request("PATCH", f"connections/{conn_id}", data=conn)
    else:
        logger.info(f"Creating connection '{conn_id}'...")
        response = api_request("POST", "connections", data=conn)

    if response.status_code in (200, 201):
        logger.info(f"✅ Connection '{conn_id}' ready")
        return True
    else:
        logger.error(
            f"❌ Failed to create connection '{conn_id}': {response.status_code} - {response.text}"
        )
        return False


def delete_connection(conn_id: str) -> bool:
    """Delete an Airflow connection."""
    response = api_request("DELETE", f"connections/{conn_id}")
    return response.status_code in (200, 204)


# ============================================
# VARIABLE MANAGEMENT
# ============================================


def variable_exists(key: str) -> bool:
    """Check if a variable already exists."""
    response = api_request("GET", f"variables/{key}")
    return response.status_code == 200


def create_variable(var: Dict[str, Any]) -> bool:
    """Create a new Airflow variable."""
    key = var["key"]

    if variable_exists(key):
        logger.info(f"Variable '{key}' already exists, updating...")
        response = api_request("PATCH", f"variables/{key}", data=var)
    else:
        logger.info(f"Creating variable '{key}'...")
        response = api_request("POST", "variables", data=var)

    if response.status_code in (200, 201):
        logger.info(f"✅ Variable '{key}' ready")
        return True
    else:
        logger.error(
            f"❌ Failed to create variable '{key}': {response.status_code} - {response.text}"
        )
        return False


# ============================================
# MAIN SETUP FUNCTION
# ============================================


def setup_all():
    """Create all connections and variables."""
    logger.info("=" * 60)
    logger.info("SETTING UP AIRFLOW CONNECTIONS AND VARIABLES")
    logger.info("=" * 60)

    # Check Airflow API is reachable
    response = api_request("GET", "dags")
    if response.status_code != 200:
        logger.error(
            f"Cannot reach Airflow API at {AIRFLOW_API_URL}: "
            f"{response.status_code} - {response.text}"
        )
        sys.exit(1)

    logger.info(f"Connected to Airflow at {AIRFLOW_API_URL}")

    # Create connections
    logger.info("\n--- Connections ---")
    conn_success = 0
    for conn in CONNECTIONS:
        if create_connection(conn):
            conn_success += 1

    # Create variables
    logger.info("\n--- Variables ---")
    var_success = 0
    for var in VARIABLES:
        if create_variable(var):
            var_success += 1

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("SETUP COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Connections: {conn_success}/{len(CONNECTIONS)}")
    logger.info(f"Variables:   {var_success}/{len(VARIABLES)}")

    if conn_success == len(CONNECTIONS) and var_success == len(VARIABLES):
        logger.info("✅ All setup complete!")
        return 0
    else:
        logger.warning("⚠️  Some items failed to create")
        return 1


if __name__ == "__main__":
    sys.exit(setup_all())
