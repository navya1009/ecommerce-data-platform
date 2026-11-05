#!/bin/bash
# scripts/bootstrap_airflow.sh
# -----------------------------------------------------------------------------
# Bootstrap script for the Airflow EC2 instance.
# Runs as root at first boot via Terraform's user_data.
# Idempotent: safe to re-run manually.
# -----------------------------------------------------------------------------

set -euo pipefail

# Redirect all output to a log file (cloud-init already captures it, but
# this gives us a dedicated file to inspect later).
exec > >(tee -a /var/log/bootstrap_airflow.log) 2>&1

echo "=============================================="
echo "Starting Airflow bootstrap: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "=============================================="

# -----------------------------------------------------------------------------
# 1. Install Docker and docker-compose plugin
# -----------------------------------------------------------------------------
if ! command -v docker &>/dev/null; then
    echo "[1/7] Installing Docker..."
    dnf update -y
    dnf install -y docker git jq curl
    systemctl enable docker
    systemctl start docker
    usermod -aG docker ec2-user

    # Install docker-compose plugin (v2)
    mkdir -p /usr/local/lib/docker/cli-plugins
    curl -SL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64" \
        -o /usr/local/lib/docker/cli-plugins/docker-compose
    chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
else
    echo "[1/7] Docker already installed."
fi

# -----------------------------------------------------------------------------
# 2. Install AWS CLI v2 (needed for Secrets Manager)
# -----------------------------------------------------------------------------
if ! command -v aws &>/dev/null; then
    echo "[2/7] Installing AWS CLI v2..."
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip"
    dnf install -y unzip
    unzip -q /tmp/awscliv2.zip -d /tmp
    /tmp/aws/install
    rm -rf /tmp/aws /tmp/awscliv2.zip
else
    echo "[2/7] AWS CLI already installed."
fi

# -----------------------------------------------------------------------------
# 3. Fetch secrets from AWS Secrets Manager and write .env
# -----------------------------------------------------------------------------
# Terraform should have created a secret named "$${project}-$${env}-airflow-secrets"
# with a JSON payload: {"POSTGRES_PASSWORD":"...","MYSQL_PASSWORD":"...", ...}
SECRET_ID="${SECRET_ID}"
APP_DIR="/opt/ecommerce-data-platform"

echo "[3/7] Fetching secrets from Secrets Manager ($SECRET_ID)..."
mkdir -p "$APP_DIR"

if aws secretsmanager describe-secret --secret-id "$SECRET_ID" &>/dev/null; then
    aws secretsmanager get-secret-value \
        --secret-id "$SECRET_ID" \
        --query SecretString \
        --output text > /tmp/secrets.json

    # Convert JSON to .env format
    jq -r 'to_entries[] | "\(.key)=\(.value)"' /tmp/secrets.json > "$APP_DIR/.env"
    chmod 600 "$APP_DIR/.env"
    rm -f /tmp/secrets.json
    echo "         .env written to $APP_DIR/.env"
else
    echo "         WARNING: Secret $SECRET_ID not found. Creating placeholder .env."
    cat > "$APP_DIR/.env" <<EOF
POSTGRES_PASSWORD=changeme
MYSQL_PASSWORD=changeme
AIRFLOW__CORE__FERNET_KEY=changeme
EOF
    chmod 600 "$APP_DIR/.env"
fi

# -----------------------------------------------------------------------------
# 4. Clone the GitHub repo
# -----------------------------------------------------------------------------
REPO_URL="${REPO_URL}"
BRANCH="${BRANCH}"

if [ ! -d "$APP_DIR/.git" ]; then
    echo "[4/7] Cloning $REPO_URL ($BRANCH)..."
    cd /opt
    git clone --branch "$BRANCH" "$REPO_URL" ecommerce-data-platform
else
    echo "[4/7] Repo already cloned. Pulling latest..."
    cd "$APP_DIR"
    git fetch --all
    git reset --hard "origin/$BRANCH"
fi

chown -R ec2-user:ec2-user "$APP_DIR"

# -----------------------------------------------------------------------------
# 5. Start Airflow via docker-compose
# -----------------------------------------------------------------------------
echo "[5/7] Starting Airflow stack..."
cd "$APP_DIR"
docker compose up -d airflow

# -----------------------------------------------------------------------------
# 6. Install GitOps cron job (pull every 5 minutes)
# -----------------------------------------------------------------------------
echo "[6/7] Installing GitOps cron job..."

cat > /etc/cron.d/gitops-pull <<EOF
# Pull latest Git changes every 5 minutes and restart Airflow if DAGs changed.
*/5 * * * * ec2-user /usr/local/bin/gitops-pull.sh >> /var/log/gitops.log 2>&1
EOF

cat > /usr/local/bin/gitops-pull.sh <<'EOF'
#!/bin/bash
set -euo pipefail
APP_DIR="/opt/ecommerce-data-platform"
cd "$APP_DIR"

# Save current DAG checksum
BEFORE=$(find dags/ -type f -name '*.py' -exec sha256sum {} \; | sha256sum)

git fetch --quiet origin main
git reset --hard --quiet origin/main

AFTER=$(find dags/ -type f -name '*.py' -exec sha256sum {} \; | sha256sum)

if [ "$BEFORE" != "$AFTER" ]; then
    echo "$(date -u +%FT%TZ) DAG changes detected — Airflow will auto-reload."
    # Airflow's DAG folder is volume-mounted from ./dags — no restart needed.
    # If you use a non-mounted folder, run: docker exec ecommerce_airflow airflow dags reserialize
fi
EOF

chmod +x /usr/local/bin/gitops-pull.sh

# -----------------------------------------------------------------------------
# 7. Configure CloudWatch agent to ship logs
# -----------------------------------------------------------------------------
echo "[7/7] Configuring CloudWatch agent..."

dnf install -y amazon-cloudwatch-agent

cat > /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json <<EOF
{
  "logs": {
    "logs_collected": {
      "files": {
        "collect_list": [
          {
            "file_path": "/var/log/bootstrap_airflow.log",
            "log_group_name": "/aws/ec2/ecommerce-data-platform-${ENVIRONMENT}/airflow",
            "log_stream_name": "{instance_id}/bootstrap",
            "timezone": "UTC"
          },
          {
            "file_path": "/var/log/gitops.log",
            "log_group_name": "/aws/ec2/ecommerce-data-platform-${ENVIRONMENT}/airflow",
            "log_stream_name": "{instance_id}/gitops",
            "timezone": "UTC"
          }
        ]
      }
    }
  }
}
EOF

/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
    -a fetch-config \
    -m ec2 \
    -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json \
    -s

systemctl enable amazon-cloudwatch-agent

echo "=============================================="
echo "Airflow bootstrap complete: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "=============================================="