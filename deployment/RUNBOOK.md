# Production Deployment Runbook

**Audience:** a new team member on their first day. You have AWS access, you know AWS basics, and you have not seen this pipeline before.

**Status of this document:** written from the codebase as of Phase 11. Items marked **[UNVERIFIED]** are assumptions the repository does not back up. Confirm them before relying on them. Items marked **[BLOCKER]** must be fixed before a production deployment works.

---

## 0. What you are deploying

A daily batch pipeline:

- **Sources:** CSV files, a mock REST API, PostgreSQL CDC, and Kafka (code only).
- **Staging:** MySQL 8.0 (`ecommerce_staging`).
- **Transform:** PySpark jobs (`transform_orders.py`, `transform_clickstream.py`, `compute_aggregates.py`).
- **Warehouse:** PostgreSQL 15 star schema (`warehouse` schema).
- **Quality:** Great Expectations suites.
- **Orchestration:** Airflow DAG `ecommerce_daily_pipeline`, scheduled `0 1 * * *` (01:00 UTC), defined in `dags/airflow_config.py`.
- **Infrastructure:** Terraform in `terraform/` (the deployed root), with modules under `terraform/modules/`. Monitoring is CloudWatch and SNS.

Production runs on EC2 (Airflow, Spark), RDS (PostgreSQL and MySQL), S3, and CloudWatch.

---

## 1. Pre-deployment checklist

### 1.1 AWS permissions

The deploying IAM principal needs these service permissions, at least in the deployment account and region. Scope them down once the first deployment works.

| Service | Why | Typical managed policy or action set |
|---|---|---|
| EC2 | Airflow and Spark instances, security groups, key pairs, AMI lookup | `AmazonEC2FullAccess` or `ec2:*` on the VPC resources |
| RDS | PostgreSQL and MySQL instances, subnet groups, parameter groups | `AmazonRDSFullAccess` |
| S3 | Raw/processed buckets, scripts bucket, Terraform state bucket | `AmazonS3FullAccess` (or bucket-scoped) |
| IAM | Roles and instance profiles for EC2, GitHub OIDC role | `IAMFullAccess` (creates roles the pipeline assumes) |
| Lambda | S3-to-Airflow trigger functions (`lambda/`) | `AWSLambda_FullAccess` |
| CloudWatch | Log groups, dashboards, alarms | `CloudWatchFullAccess` |
| SNS | Alert topic and email subscription | `AmazonSNSFullAccess` |
| Secrets Manager | `ecommerce-data-platform-{env}-app-secrets` | `SecretsManagerReadWrite` |
| CloudFormation | Not used by this project. Listed because some AWS tooling requires it. | `AWSCloudFormationFullAccess` (optional) |
| DynamoDB | Terraform state lock table (Step 1) | `AmazonDynamoDBFullAccess` (or table-scoped) |

Check your identity before starting:

```bash
aws sts get-caller-identity
```

### 1.2 Required tools

| Tool | Version | Install |
|---|---|---|
| Terraform | >= 1.9.0 | https://developer.hashicorp.com/terraform/install |
| AWS CLI | v2 | https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html |
| Docker Desktop (or Docker Engine) | latest | https://docs.docker.com/get-docker/ |
| Ansible | 9.1.0 | `pip install ansible==9.1.0` (pinned in `requirements-dev.txt`) |
| psql | 15 | `sudo apt install postgresql-client-15` |
| mysql client | 8.0 | `sudo apt install mysql-client-8.0` (or the MySQL Shell bundle) |

### 1.3 Secrets in AWS Secrets Manager

The compute module reads one secret named `<name_prefix>-app-secrets` (`terraform/modules/compute/main.tf`, `SECRET_ID`). For prod, `name_prefix` is expected to be `ecommerce-data-platform-prod` **[UNVERIFIED: confirm the value of `name_prefix` in `terraform/environments/prod.tfvars`]**.

The keys listed below are the ones the setup asks for. **[UNVERIFIED]** The repository does not show how the EC2 bootstrap parses the secret. Confirm the expected format (one JSON object holding all keys is assumed below) with whoever owns `terraform/modules/compute` before creating it.

Generate values with a password manager or:

```bash
openssl rand -base64 32      # for passwords
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"   # Fernet key
```

Create the secret. Replace each `<...>` with the real value. Do not put values into shell history on a shared machine (prefix with a space if your shell is configured to skip it, or read them from a file).

```bash
aws secretsmanager create-secret \
  --name ecommerce-data-platform-prod-app-secrets \
  --region us-east-1 \
  --description "Production app secrets for ecommerce-data-platform" \
  --secret-string '{
    "POSTGRES_PASSWORD": "<strong-password>",
    "MYSQL_PASSWORD": "<strong-password>",
    "AIRFLOW__CORE__FERNET_KEY": "<fernet-key>",
    "SLACK_WEBHOOK_URL": "<https://hooks.slack.com/services/...>",
    "AIRFLOW_ADMIN_PASSWORD": "<strong-password>"
  }'
```

Per-key alternative, if the bootstrap reads one secret per key: create each as its own secret, e.g. `aws secretsmanager create-secret --name ecommerce-data-platform-prod-POSTGRES_PASSWORD --secret-string "<value>"`. **[UNVERIFIED]** Pick one approach and match the consumer.

Verify (the value is not printed):

```bash
aws secretsmanager describe-secret --secret-id ecommerce-data-platform-prod-app-secrets \
  --query '{Name:Name, Updated:LastChangedDate}'
```

### 1.4 GitHub repository secrets

Set these under **Settings → Secrets and variables → Actions**. The CI/CD workflows in `.github/workflows/` read them.

| Secret | Where the value comes from |
|---|---|
| `AWS_ROLE_ARN` | `terraform -chdir=terraform output -raw github_actions_role_arn` (OIDC role, `terraform/modules/iam`) |
| `AWS_REGION` | The region you deploy to, for example `us-east-1` |
| `ECR_REPOSITORY` | The ECR repository name for the Airflow/pipeline image **[UNVERIFIED: no ECR resource found in `terraform/`; confirm it exists or create it]** |
| `AIRFLOW_BASE_URL` | `http://$(terraform -chdir=terraform output -raw airflow_public_ip):8080` |
| `AIRFLOW_USER` | The Airflow admin username (default `admin`, see `docker/airflow/bootstrap.sh`) |
| `AIRFLOW_PASSWORD` | Same value as `AIRFLOW_ADMIN_PASSWORD` in Secrets Manager |
| `SLACK_WEBHOOK_URL` | Same Slack webhook as in Secrets Manager |
| `SSH_PRIVATE_KEY` (optional) | Private half of the EC2 key pair. Only needed if CI deploys over SSH. |

---

## 2. Step 1: bootstrap the Terraform state backend

Terraform stores its state in S3 and locks it with DynamoDB. Create both once per account. Use the same region everywhere.

```bash
export AWS_REGION=us-east-1          # set to your region
export STATE_BUCKET=ecommerce-data-platform-terraform-state
export LOCK_TABLE=ecommerce-data-platform-terraform-locks
```

### 2.1 S3 bucket

For `us-east-1` the bucket is created without a location constraint. For any other region add `--create-bucket-configuration LocationConstraint=$AWS_REGION`.

```bash
aws s3api create-bucket --bucket "$STATE_BUCKET" --region "$AWS_REGION" \
  $( [ "$AWS_REGION" != "us-east-1" ] && echo "--create-bucket-configuration LocationConstraint=$AWS_REGION" )

aws s3api put-bucket-versioning --bucket "$STATE_BUCKET" \
  --versioning-configuration Status=Enabled

aws s3api put-bucket-encryption --bucket "$STATE_BUCKET" \
  --server-side-encryption-configuration '{
    "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]
  }'

aws s3api put-public-access-block --bucket "$STATE_BUCKET" \
  --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
```

### 2.2 DynamoDB lock table

```bash
aws dynamodb create-table \
  --table-name "$LOCK_TABLE" \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region "$AWS_REGION"

aws dynamodb wait table-exists --table-name "$LOCK_TABLE" --region "$AWS_REGION"
```

### 2.3 Verify

```bash
aws s3api head-bucket --bucket "$STATE_BUCKET" && echo "bucket OK"
aws s3api get-bucket-versioning --bucket "$STATE_BUCKET"          # Status: Enabled
aws s3api get-public-access-block --bucket "$STATE_BUCKET"        # all four true
aws dynamodb describe-table --table-name "$LOCK_TABLE" \
  --query 'Table.{Name:TableName,Status:TableStatus,Key:KeySchema}'
```

Expected: `TableStatus` is `ACTIVE`; the key schema has `LockID` as `HASH`.

---

## 3. Step 2: provision infrastructure

### 3.1 Edit `terraform/main.tf` first

`terraform/main.tf` is wired for local testing against fakecloud. Two edits are required before a real deployment. **[BLOCKER]** Skipping them deploys nothing, or sends traffic to the fakecloud emulator.

1. **Enable the S3 backend.** The `backend "s3"` block is commented out. Around line 18 the file says `# Remote backend: state file lives in S3, locking via DynamoDB.`, and the block starts at line 22 (`# backend "s3" {`). Uncomment the whole block and set the bucket, key, region and lock table:

   ```hcl
   backend "s3" {
     bucket         = "ecommerce-data-platform-terraform-state"
     key            = "ecommerce-data-platform/terraform.tfstate"
     region         = "us-east-1"
     dynamodb_table = "ecommerce-data-platform-terraform-locks"
     encrypt        = true
   }
   ```

2. **Remove the fakecloud provider settings.** In the `provider "aws"` block, around lines 33 to 42, there are dummy credentials (`access_key = "test"`, `secret_key = "test"`), the `skip_*` flags, `s3_use_path_style`, and an `endpoints { ... }` block pointing at `http://localhost:4566`. Delete all of them. Real AWS needs none of them, and the dummy keys must not be used in production. Authenticate with your AWS profile or the role your CI assumes.

Confirm no emulator endpoint remains:

```bash
grep -n "4566\|fakecloud\|access_key\|secret_key\|skip_" terraform/main.tf && echo "STILL CONFIGURED" || echo "clean"
```

### 3.2 Provision

```bash
cd terraform
terraform init -migrate-state
terraform workspace new prod          # first time only; if it exists: terraform workspace select prod
terraform workspace select prod
terraform plan -var-file=environments/prod.tfvars -out=tfplan
# Read the plan. Check the resource counts and the names before applying.
terraform apply tfplan
terraform output > ../deployment/prod-outputs.txt
```

> `deployment/prod-outputs.txt` contains endpoints and identifiers. Do not commit it if it grows any secrets. Terraform outputs are not secrets, but treat the file as internal.

Outputs you will use later (all defined in `terraform/outputs.tf`):

| Output | Used for |
|---|---|
| `postgres_endpoint` | Step 3 migrations |
| `mysql_endpoint` | Step 3 migrations |
| `airflow_public_ip` | Steps 4 and 5 |
| `spark_master_public_ip` | Spark connection |
| `sns_alert_topic_arn` | Step 6 alert test |
| `github_actions_role_arn` | GitHub secret `AWS_ROLE_ARN` |

> **Note:** `terraform/` is the only Terraform tree. The duplicate `infrastructure/` tree was deleted, because it was not used by any script or workflow.

---

## 4. Step 3: database migrations

Run migrations once per environment, in order. Each file is idempotent or has been written to be safe to re-run, but apply each only once.

### 4.1 Connect to the databases

RDS sits in private subnets. Reach it from a machine inside the VPC: a bastion host, a VPN, or the Airflow EC2 instance. **[UNVERIFIED: no bastion is defined in the repository. Use the access path your team already has.]**

```bash
export PG_HOST=$(terraform -chdir=terraform output -raw postgres_endpoint)
export MYSQL_HOST=$(terraform -chdir=terraform output -raw mysql_endpoint)
```

If the endpoint includes a port, strip it. RDS endpoints from `terraform output` may look like `host.rds.amazonaws.com:5432`. Check with `echo $PG_HOST`.

Read the passwords from Secrets Manager. Do not paste them into scripts:

```bash
export PGPASSWORD=$(aws secretsmanager get-secret-value --secret-id ecommerce-data-platform-prod-app-secrets \
  --query SecretString --output text | python -c "import json,sys; print(json.load(sys.stdin)['POSTGRES_PASSWORD'])")
export MYSQL_PWD=$(aws secretsmanager get-secret-value --secret-id ecommerce-data-platform-prod-app-secrets \
  --query SecretString --output text | python -c "import json,sys; print(json.load(sys.stdin)['MYSQL_PASSWORD'])")
```

`MYSQL_PWD` is read by the `mysql` client. Clear both variables when you finish: `unset PGPASSWORD MYSQL_PWD`.

### 4.2 Postgres (warehouse)

Run from the repository root, in filename order. The loop uses the glob order, which is lexicographic.

```bash
for f in migrations/postgres/*.sql; do
  echo "Applying $f"
  psql -h "$PG_HOST" -p 5432 -U admin -d postgres -v ON_ERROR_STOP=0 -f "$f"
done
```

Notes:

- `ON_ERROR_STOP=0` lets the loop continue. `002_create_analytics_tables.sql` creates one index that migration 001 already created, and an error on that line is expected and harmless. Do not change this to `1` without also fixing that index statement.
- Empty files are skipped without error: `001_create_orders_table.sql`, `002_create_products_table.sql`, `003_add_indexes.sql`.
- The order is `001_create_warehouse_schema` → `002_create_analytics_tables` → `003`(empty) → `004_fact_orders_transform_columns` → `005_create_pipeline_metrics` → `006_create_pipeline_lineage` → `007_fix_fact_clickstream_schema` → `008_correct_segment_comment` (comment only) → `009_fix_inventory_product_id_type` (guarded: changes `agg_inventory_health.product_id` to BIGINT only if it is not already).

### 4.3 MySQL (staging)

Apply only the files listed below. Two files are known to be broken; skip them.

```bash
for f in migrations/mysql/001_create_staging_schema_v3.sql \
         migrations/mysql/002_create_control_tables.sql \
         migrations/mysql/003_create_dim_users.sql \
         migrations/mysql/004_fix_watermark_precision.sql; do
  echo "Applying $f"
  mysql -h "$MYSQL_HOST" -P 3306 -u admin ecommerce_staging < "$f"
done
```

**Skipped on purpose:**

- `migrations/mysql/001_create_staging_schema.sql` is written in PostgreSQL syntax and targets a `staging` database. Running it aborts MySQL initialization. It was superseded by the `_v3` file (Phase 8.2, Issue 3).
- `001_create_inventory_table.sql` and `002_create_cdc_log_table.sql` are empty placeholders.

**Note on the MySQL user:** the script connects as `admin`. **[UNVERIFIED: confirm the RDS master username. The Spark job hardcodes `root` for MySQL (`transform_orders.py`), so that user must exist with matching privileges, or the job will fail to authenticate.]**

### 4.4 Verify the schema

```bash
psql -h "$PG_HOST" -U admin -d postgres -c "\dt warehouse.*"
mysql -h "$MYSQL_HOST" -u admin ecommerce_staging -e "SHOW TABLES; SHOW COLUMNS FROM watermarks LIKE 'last_extracted_at';"
```

Expected: the `warehouse` schema has `fact_orders`, `fact_clickstream`, the `dim_*` tables, `quarantine_orders`, `pipeline_metrics`, `pipeline_lineage`, and the four `agg_*` tables. The `watermarks.last_extracted_at` column type is `datetime(6)`.

---

## 5. Step 4: configure Airflow

### 5.1 Find the host and check the stack

```bash
export AIRFLOW_IP=$(terraform -chdir=terraform output -raw airflow_public_ip)
ssh -i ~/.ssh/ecommerce-prod.pem ec2-user@"$AIRFLOW_IP"
```

**[UNVERIFIED]** The SSH user `ec2-user` and the key path `~/.ssh/ecommerce-prod.pem` are conventions, not facts in the repository. The compute module takes an optional `key_pair_name` and does not set a user. Use the user and key your AMI and key pair actually have.

On the EC2 instance:

```bash
cd /opt/ecommerce-data-platform         # path used by scripts/bootstrap_airflow.sh
docker compose ps                       # every service should be "running" or "healthy"
docker compose logs airflow --tail 50
```

### 5.2 Airflow connections

Connections are created by `scripts/setup_airflow_connections.py`, which calls the Airflow REST API. Every value is read from an environment variable. If the variable is unset, the script uses the local Docker default shown below, so the same script works in both places. Set the variables in the shell before you run it; do not rely on the defaults in production.

| Connection id | Type | Environment variables | Docker default (used if unset) | Production value to set |
|---|---|---|---|---|
| `postgres_dwh` | postgres | `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` | `postgres:5432`, `ecommerce_warehouse`, `admin` | the RDS PostgreSQL endpoint and credentials |
| `mysql_staging` | mysql | `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_DATABASE`, `MYSQL_STAGING_USER`, `MYSQL_STAGING_PASSWORD` | `mysql:3306`, `ecommerce_staging`, `root` | the RDS MySQL endpoint and credentials |
| `spark_default` | spark | `SPARK_MASTER_HOST`, `SPARK_MASTER_PORT` | `spark-master:7077` | the Spark master address (private IP or `spark_master_public_ip`) |
| `mock_api` | http | `MOCK_API_HOST`, `MOCK_API_PORT`, `MOCK_API_SCHEME` | `mock-api:8000`, `http` | the production API base URL, or leave unused if the mock API is not needed |
| `aws_default` | aws | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` | dummy key, `us-east-1` | prefer an IAM instance profile; if you set keys, use real ones |
| `slack_webhook` | http | `SLACK_WEBHOOK_URL` (host and scheme are taken from it) | `hooks.slack.com`, `https` | the Slack webhook from Secrets Manager |

**Check before you run it in production.** The script's defaults are Docker service names and dummy values. Confirm that every variable the production connections need is set, then run the script. Do not run it with the defaults on AWS, because the `aws_default` connection would hold dummy credentials and the Docker hostnames do not resolve there.

Run the script from the EC2 host:

```bash
cd /opt/ecommerce-data-platform
export POSTGRES_HOST=<rds-endpoint> POSTGRES_USER=<user> POSTGRES_PASSWORD=<from-secrets-manager>
export MYSQL_HOST=<rds-mysql-endpoint> MYSQL_STAGING_USER=<user> MYSQL_STAGING_PASSWORD=<from-secrets-manager>
export SPARK_MASTER_HOST=<spark-private-ip>
python scripts/setup_airflow_connections.py
```

**Spark job credentials come from the environment.** `transform_orders.py`, `transform_clickstream.py` and `compute_aggregates.py` read `MYSQL_USER`/`MYSQL_PASSWORD` and `POSTGRES_USER`/`POSTGRES_PASSWORD`. No password is built into `src/`. A job that starts without these variables stops with a message naming the missing one. On the Airflow host, set all four before the DAG runs. The ingestion modules read `MYSQL_PASSWORD` and `PG_SOURCE_PASSWORD` the same way.

Connection ids use underscores (`postgres_dwh`, not `postgres.dwh`), which is what the code expects.

### 5.3 Verify in the UI

Open `http://<airflow-ip>:8080`. The admin user and password come from `AIRFLOW_ADMIN_USERNAME` and `AIRFLOW_ADMIN_PASSWORD`. The bootstrap defaults are `admin`/`admin` (`docker/airflow/bootstrap.sh`). **Change the password in production** (see Secrets Manager).

Go to **Admin → Connections** and confirm all six exist. Use **Test** on each where the UI offers it. Connections that fail are the ones to fix before Step 5.

Check the DAG loads with no import errors:

```bash
docker exec ecommerce_airflow airflow dags list-import-errors
docker exec ecommerce_airflow airflow dags list | grep ecommerce_daily_pipeline
```

---

## 6. Step 5: initial data load

**Do not load fake data into production.** `scripts/generate_fake_data.py` takes no command-line options. Its volume is a module constant, `NUM_ORDERS = 100000`, and it writes to the staging databases. It is a development and smoke-test tool. Running the flags shown in earlier drafts (`--orders`, `--clickstream`, `--inventory`) fails, because the script does not read them.

For a production first load, extract real data from the sources (CSV drop into the watched folder, the API, or CDC). For a smoke test on a non-production environment only, run the generator as it is:

```bash
# NON-PRODUCTION ONLY
docker exec ecommerce_pipeline python scripts/generate_fake_data.py
```

Run the CSV ingestion. `ingest_csv.py` watches `data/raw/` and loads anything it finds:

```bash
docker exec ecommerce_pipeline python src/extract/ingest_csv.py
```

Trigger the DAG and watch it:

```bash
docker exec ecommerce_airflow airflow dags trigger ecommerce_daily_pipeline
docker exec ecommerce_airflow airflow dags list-runs -d ecommerce_daily_pipeline --no-backfill
```

Open the UI at `http://<airflow-ip>:8080`, click the DAG, and watch the Graph view.

**What success looks like:** every task in the DAG is green (success), with no task in `failed` or `up_for_retry`. The Phase 5 run on the development stack took about 2 minutes 11 seconds **[the task count and duration are from the Phase 5 measurement on the dev stack, not from production]**. The Spark jobs run on the cluster and the Great Expectations checks run after the loads.

Spark is run inside the pipeline container with this command (the same one the DAG uses for the orders job):

```bash
docker exec ecommerce_pipeline spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --driver-memory 2g --executor-memory 2g --executor-cores 2 \
  --conf "spark.driver.extraClassPath=/opt/spark/jars/*" \
  --conf "spark.executor.extraClassPath=/opt/spark/jars/*" \
  --conf "spark.serializer=org.apache.spark.serializer.JavaSerializer" \
  --jars "/opt/spark/jars/mysql-connector-java-8.0.33.jar,/opt/spark/jars/postgresql-42.6.0.jar" \
  /app/src/spark_jobs/transform_orders.py --batch-id manual_$(date +%Y%m%d) --env dev
```

---

## 7. Step 6: verify end to end

Run these against the production warehouse after the first DAG run. Use the `psql` connection from Step 3.

```sql
-- 1. Row counts across the pipeline
SELECT 'fact_orders' AS tbl, COUNT(*) AS rows FROM warehouse.fact_orders
UNION ALL SELECT 'fact_clickstream', COUNT(*) FROM warehouse.fact_clickstream
UNION ALL SELECT 'quarantine_orders', COUNT(*) FROM warehouse.quarantine_orders;
```

`stg_raw_orders` is in MySQL, not Postgres. Check it there:

```sql
-- run in MySQL (ecommerce_staging)
SELECT COUNT(*) AS stg_raw_orders_rows FROM stg_raw_orders;
```

```sql
-- 2. Freshness (newest order loaded)
SELECT MAX(created_at) AS newest_order FROM warehouse.fact_orders;
```

```sql
-- 3. Recent pipeline metrics
SELECT pipeline_name, status, rows_loaded, rows_quarantined, processing_time_seconds, recorded_at
FROM warehouse.pipeline_metrics
ORDER BY recorded_at DESC
LIMIT 5;
```

```sql
-- 4. Lineage edges written by the latest runs
SELECT target_table, COUNT(*) AS edges, MAX(recorded_at) AS last_seen
FROM warehouse.pipeline_lineage
GROUP BY target_table
ORDER BY target_table;
```

Expected: `fact_orders` and `fact_clickstream` are non-zero; `pipeline_metrics` has a `success` row for each job that ran; `pipeline_lineage` has an edge for `warehouse.fact_orders` and `warehouse.fact_clickstream`.

**Logs:**

```bash
aws logs tail /aws/ec2/ecommerce-data-platform-prod/airflow --follow
```

The log group name follows the Terraform module's `/aws/ec2/${name_prefix}/airflow` pattern (`terraform/modules/monitoring/main.tf`). Replace `ecommerce-data-platform-prod` with your `name_prefix` if it differs.

**Alert test:**

```bash
aws sns publish \
  --topic-arn "$(terraform -chdir=terraform output -raw sns_alert_topic_arn)" \
  --message "Test alert from the production runbook" \
  --subject "ecommerce-data-platform test"
```

Check that the subscribed email receives it. The SNS email subscription must be confirmed first; the subscription email has to be clicked once.

**Slack alerts** are sent by `dags/callbacks.py`, not by SNS. Test them by checking the Slack channel after a deliberate task failure in a non-production environment, or by posting to the webhook with `curl`. Do not post test messages to the production channel unless your team agrees to it.

---

## 8. Rollback procedure

Pick the scenario that matches what went wrong.

### 8.1 Redeploy the previous Docker image

Use this when a new image breaks Airflow or the pipeline container. Find the previous image tag in ECR (or in your image history), then pull and recreate.

```bash
# On the Airflow EC2 host
cd /opt/ecommerce-data-platform
export PREVIOUS_TAG=<previous-tag>                       # from ECR or your release notes
# Point docker-compose at the previous tag (edit the image: line, or override with an env var
# if your compose file uses one). **[UNVERIFIED: the compose file pins images by tag; check the image: lines.]**
docker pull <registry>/<image>:"$PREVIOUS_TAG"
docker compose up -d --force-recreate airflow
docker compose ps
```

Confirm the DAG parses again: `docker exec ecommerce_airflow airflow dags list-import-errors`.

### 8.2 Roll back a database migration

Migrations here are forward-only. **There is no DOWN script in the repository.** For migration `007_fix_fact_clickstream_schema.sql`:

- **Restore from snapshot (preferred).** Take a manual RDS snapshot before applying any migration in production, then restore to a new instance and repoint, or use point-in-time recovery.

  ```bash
  aws rds create-db-snapshot --db-instance-identifier <postgres-instance-id> \
    --db-snapshot-identifier pre-007-$(date +%Y%m%d%H%M)
  ```

- **Partial DOWN (columns only).** The three columns that 007 added can be dropped without data loss beyond themselves. Do not drop `device`, which the analytics queries use.

  ```sql
  -- DOWN for migration 007 (partial). Drops only columns added by 007.
  ALTER TABLE warehouse.fact_clickstream DROP COLUMN IF EXISTS os;
  ALTER TABLE warehouse.fact_clickstream DROP COLUMN IF EXISTS session_duration_seconds;
  ALTER TABLE warehouse.fact_clickstream DROP COLUMN IF EXISTS is_bot;
  ```

- **Not reversible: `event_id`.** Migration 007 changed `event_id` from `BIGINT` to `VARCHAR`. Converting back would fail, because the stored values are text such as `EVT_000000696`. Use a snapshot restore for this, never a DOWN script.

### 8.3 Re-run a failed backfill

Backfill the DAG for a date range after fixing the cause. `--reset-dagruns` clears existing runs in that range so they run again.

```bash
docker exec ecommerce_airflow airflow dags backfill \
  -s 2026-01-01 -e 2026-01-07 \
  --reset-dagruns \
  ecommerce_daily_pipeline
```

Before a backfill, check that the watermarks are not already past the range you want to reprocess. Incremental ingestion reads from the watermark, so a backfill of old dates alone may load nothing. **[UNVERIFIED: confirm the DAG's backfill semantics with the owner of `dags/ecommerce_pipeline_dag.py`.]**

---

## 9. Common issues and fixes

Each entry gives the symptom, a command to diagnose it, and the fix. Symptoms are taken from issues found while building the pipeline unless marked otherwise.

### 9.1 DAG does not appear in the Airflow UI

- **Symptom:** `ecommerce_daily_pipeline` is missing from the list.
- **Diagnose:**
  ```bash
  docker compose logs airflow | grep -i "dag"
  docker exec ecommerce_airflow airflow dags list-import-errors
  ```
- **Fix:** Fix the import error the command shows. Check that the `dags/` folder is mounted into the Airflow container (`docker compose config | grep dags`).

### 9.2 Spark cannot load the MySQL JDBC driver

- **Symptom:** `ClassNotFoundException: com.mysql.cj.jdbc.Driver` in the Spark job output.
- **Diagnose:**
  ```bash
  docker exec ecommerce_pipeline ls /opt/spark/jars | grep -i mysql
  docker exec ecommerce_pipeline env | grep SPARK_JARS_PATH
  ```
- **Fix:** The jar must exist at the path in `spark.jars`. The job reads `SPARK_JARS_PATH` (default `/opt/spark/jars`). Set the variable or fix the path. Background: this failed on a non-Docker host until the jars path was made configurable (Phase 8.2 Fix 3).

### 9.3 S3 access denied

- **Symptom:** `AccessDenied` from `boto3`, or a Lambda or Airflow task fails on an S3 call.
- **Diagnose:**
  ```bash
  aws sts get-caller-identity
  aws iam list-attached-role-policies --role-name <instance-role-name>
  ```
- **Fix:** The EC2 instance profile must have S3 access on the raw, processed and scripts buckets. Compare the role's policy with the bucket names in `terraform output`. Do not fix this by putting access keys on the instance.

### 9.4 Great Expectations checkpoint fails on the first run

- **Symptom:** `TypeError: ForwardRef._evaluate() missing 1 required keyword-only argument: 'recursive_guard'` when the checkpoint starts.
- **Diagnose:**
  ```bash
  python -c "import great_expectations, sys; print(great_expectations.__version__, sys.version)"
  ```
- **Fix:** `great_expectations==0.18.15` does not import on Python 3.12 in the development venv (observed in Phase 8.2). Run GE inside the pipeline container, which is built for the version it supports, not on an arbitrary host Python. If the container's Python is also 3.12, pin a compatible Python version for the GE step. This is a known open item.

### 9.5 Task stuck in `up_for_retry`

- **Symptom:** A task keeps retrying and the run does not finish.
- **Diagnose:**
  ```bash
  docker exec ecommerce_airflow airflow tasks states-for-dag-run ecommerce_daily_pipeline <run_id>
  ```
  The self-healing logic is in `dags/tasks/self_healing.py`. It counts consecutive failures and decides on a recovery action. The threshold is set in code and is not an Airflow Variable.
- **Fix:** Read the task log for the first real error (not the retry noise) and fix it. Then clear the failed task: `airflow tasks clear -t <task_id> -d ecommerce_daily_pipeline`. **[UNVERIFIED: the exact name of the threshold constant in `self_healing.py` was not checked; search that file for `consecutive`.]**

### 9.6 `JAVA_GATEWAY_EXITED` from PySpark

- **Symptom:** `Py4JError ... JAVA_GATEWAY_EXITED` when the Spark session starts.
- **Diagnose:**
  ```bash
  docker exec ecommerce_pipeline java -version
  ```
- **Fix:** PySpark 3.5.0 needs Java 11 or 17. Java 23 breaks it (Phase 3 finding). Install or point `JAVA_HOME` at a supported JDK inside the image. Do not install a newer Java on the host to "fix" the container.

### 9.7 Watermark not advancing, or rows being re-read

- **Symptom:** Each run re-stages the same boundary rows, so `stg_raw_orders` grows faster than the source. (The phrase "watermarks are ahead" is not an error this code produces.)
- **Diagnose:**
  ```bash
  mysql -h "$MYSQL_HOST" -u admin ecommerce_staging -e "SELECT pipeline_name, last_extracted_at FROM watermarks;"
  mysql -h "$MYSQL_HOST" -u admin ecommerce_staging -e "SHOW COLUMNS FROM watermarks LIKE 'last_extracted_at';"
  ```
- **Fix:** `last_extracted_at` must be `datetime(6)`. Migration `004_fix_watermark_precision.sql` makes that change. Apply it if the column shows `timestamp` or `datetime` without fractional seconds. Phase 8.2 found this defect.

### 9.8 `Lineage record failed` in logs

- **Symptom:** Warnings in the logs saying lineage was not recorded. The job itself succeeds.
- **Diagnose:**
  ```bash
  psql -h "$PG_HOST" -U admin -d postgres -c "\d warehouse.pipeline_lineage"
  ```
- **Fix:** The table is missing, so migration `006_create_pipeline_lineage.sql` has not been applied. Apply it. Lineage is best-effort by design, so the job did not fail.

### 9.9 Clickstream load fails with a missing or mismatched column

- **Symptom:** `column ... does not exist` or `column "event_id" is of type ... but expression is of type ...` in `transform_clickstream.py`.
- **Diagnose:**
  ```bash
  psql -h "$PG_HOST" -U admin -d postgres -c "\d warehouse.fact_clickstream"
  ```
  The transform expects `device`, `os`, `session_duration_seconds`, `is_bot`, `created_at`, and a `VARCHAR` `event_id`.
- **Fix:** Apply `007_fix_fact_clickstream_schema.sql`. It is written to work on both the old and the new table shape. Check the result with the command above.

### 9.10 `fact_orders` load fails on `user_segment` or `gross_margin`

- **Symptom:** `column "user_segment" of relation "fact_orders" does not exist`.
- **Diagnose:** the same `\d` command on `warehouse.fact_orders`.
- **Fix:** Apply `004_fact_orders_transform_columns.sql` (Postgres). It is idempotent.

### 9.11 RDS connection refused

- **Symptom:** `could not connect to server` or a timeout from Airflow or Spark to RDS.
- **Diagnose:**
  ```bash
  aws rds describe-db-instances --db-instance-identifier <id> \
    --query 'DBInstances[0].{Endpoint:Endpoint.Address,SG:VpcSecurityGroups[].VpcSecurityGroupId}'
  aws ec2 describe-security-groups --group-ids <rds-sg-id> \
    --query 'SecurityGroups[0].IpPermissions'
  ```
  Also check the connections in Airflow (section 5.2). If they still point at `postgres` or `mysql`, the hostnames will not resolve and the error is a DNS failure, not a firewall one.
- **Fix:** The RDS security group must allow the database port from the EC2 instance's security group. Fix the Airflow connection hosts to the RDS endpoints.

### 9.12 Connections point at the wrong hosts

- **Symptom:** Tasks fail with `Name or service not known` or `unknown host postgres`.
- **Diagnose:** `docker exec ecommerce_airflow airflow connections get postgres_dwh`
- **Fix:** Update the connection to the RDS endpoint (section 5.2). This is the blocker in Step 4.

---

## 10. Sign-off checklist

- [ ] Step 1: state bucket and lock table exist, and verification passes
- [ ] Step 2: `main.tf` has no fakecloud settings; `terraform apply` succeeded; outputs saved
- [ ] Step 3: all Postgres and MySQL migrations applied in order, verified with `\dt` and `SHOW COLUMNS`
- [ ] Step 4: connections point at production hosts (not Docker names); Airflow admin password changed
- [ ] Step 5: first DAG run finished with every task green
- [ ] Step 6: row counts, freshness, metrics and lineage checked; SNS test received
- [ ] Rollback snapshot taken before migrations
