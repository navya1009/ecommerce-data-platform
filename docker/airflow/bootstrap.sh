#!/usr/bin/env bash
set -euo pipefail

export PATH="/home/airflow/.local/bin:${PATH}"

echo "Ensuring Docker CLI is available for docker exec tasks..."
if ! command -v docker >/dev/null 2>&1; then
  apt-get update
  apt-get install -y --no-install-recommends docker.io
  rm -rf /var/lib/apt/lists/*
fi

if [ -S /var/run/docker.sock ]; then
  DOCKER_GID="$(stat -c '%g' /var/run/docker.sock)"
  if ! getent group "${DOCKER_GID}" >/dev/null 2>&1; then
    groupadd --gid "${DOCKER_GID}" docker-host
  fi
  DOCKER_GROUP="$(getent group "${DOCKER_GID}" | cut -d: -f1)"
  usermod -aG "${DOCKER_GROUP}" airflow
fi

echo "Ensuring Python dependencies are installed..."
python -m pip install --no-cache-dir \
  "pandas==2.1.4" \
  "sqlalchemy==2.0.23" \
  "psycopg2-binary==2.9.9" \
  "mysql-connector-python==8.2.0" \
  "great-expectations==0.18.15" \
  "apache-airflow-providers-http" \
  "apache-airflow-providers-apache-spark" \
  "apache-airflow-providers-slack" \
  "apache-airflow-providers-postgres"

echo "Ensuring Airflow metadata database exists..."
python - <<'PY'
import os
import psycopg2

host = os.environ["POSTGRES_HOST"]
port = int(os.environ.get("POSTGRES_PORT", "5432"))
user = os.environ["POSTGRES_USER"]
password = os.environ["POSTGRES_PASSWORD"]
database = os.environ.get("AIRFLOW_METADATA_DB", "airflow")

conn = psycopg2.connect(
    host=host,
    port=port,
    user=user,
    password=password,
    dbname=os.environ.get("POSTGRES_DB", "ecommerce_warehouse"),
)
conn.autocommit = True
with conn.cursor() as cur:
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database,))
    if cur.fetchone() is None:
        cur.execute(f'CREATE DATABASE "{database}" OWNER "{user}"')
conn.close()
PY

echo "Upgrading Airflow metadata tables..."
su airflow -s /bin/bash -c "airflow db upgrade"

echo "Ensuring Airflow admin user exists..."
su airflow -s /bin/bash -c "airflow users create \
  --username "${AIRFLOW_ADMIN_USERNAME:-admin}" \
  --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com" || true

echo "Starting Airflow scheduler and webserver..."
su airflow -s /bin/bash -c "airflow scheduler" &
exec su airflow -s /bin/bash -c "airflow webserver"
