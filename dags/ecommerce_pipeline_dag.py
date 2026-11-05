# isort: skip_file
# type: ignore
"""
Master Airflow DAG for the E-Commerce Data Pipeline

This DAG orchestrates the complete end-to-end data pipeline:
1. Ingest data from multiple sources (API, PostgreSQL CDC, CSV)
2. Validate raw data quality with Great Expectations
3. Transform data with PySpark (orders, clickstream)
4. Validate transformed data quality
5. Compute daily aggregates
6. Validate aggregates quality
7. Update pipeline manifest
8. Send notifications

Schedule: Daily at 01:00 UTC
"""
import logging

# Import configuration
import sys
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from callbacks import sla_miss_callback

from airflow import DAG

sys.path.insert(0, "/app/dags")
from airflow_config import DEFAULT_ARGS  # noqa: E402
from airflow_config import DAG_ID, SCHEDULE_INTERVAL, START_DATE  # noqa: E402

logger = logging.getLogger(__name__)

# ============================================
# TASK FUNCTIONS
# ============================================


def run_ingest_api(**context):
    import subprocess

    batch_id = context["ds"]
    cmd = [
        "python",
        "/opt/airflow/src/extract/ingest_api.py",
        "--batch-id",
        f"api_{batch_id}",
        "--env",
        "dev",
    ]
    logger.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"ingest_api failed: {result.stderr}")
    logger.info(f"ingest_api output: {result.stdout}")
    return result.stdout


def run_ingest_postgres_cdc(**context):
    """
    Ingest changed records from PostgreSQL using CDC.

    Calls ingest_postgres_cdc.py which:
    - Reads watermark for each table
    - Fetches records where updated_at > watermark
    - Writes to MySQL staging tables

    If this fails, check:
    - PostgreSQL source database is accessible
    - Watermark table exists
    - Source tables have updated_at column
    """
    import subprocess

    batch_id = context["ds"]
    cmd = [
        "python",
        "/opt/airflow/src/extract/ingest_postgres_cdc.py",
        "--batch-id",
        f"cdc_{batch_id}",
        "--env",
        "dev",
    ]
    logger.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"ingest_postgres_cdc failed: {result.stderr}")
    return result.stdout


def run_ingest_csv(**context):
    """
    Ingest CSV files from the watched folder.

    Calls ingest_csv.py which:
    - Finds new CSV files in /app/data/raw
    - Validates headers
    - Loads into MySQL staging
    - Moves processed files to /app/data/processed

    If this fails, check:
    - CSV files exist in /app/data/raw
    - Headers match expected schema
    - MySQL connection is working
    """
    import subprocess

    batch_id = context["ds"]
    cmd = [
        "python",
        "/opt/airflow/src/extract/ingest_csv.py",
        "--batch-id",
        f"csv_{batch_id}",
        "--env",
        "dev",
    ]
    logger.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"ingest_csv failed: {result.stderr}")
    return result.stdout


def run_dq_check_raw(**context):
    """
    Run Great Expectations data quality check on raw orders.

    Validates the raw_orders_suite against staging data.
    If any expectation fails, the DAG halts here.

    If this fails, check:
    - Expectation results in GE Data Docs
    - Which specific expectation failed
    - Source data quality
    """
    import subprocess

    cmd = [
        "python",
        "/opt/airflow/scripts/run_checkpoint.py",
        "--suite",
        "raw_orders_suite",
        "--run-date",
        context["ds"],
    ]
    logger.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"DQ check failed for raw_orders_suite: {result.stderr}")
    return result.stdout


def run_dq_check_warehouse(**context):
    """
    Run Great Expectations data quality check on fact_orders.

    If this fails, check:
    - Spark transformation completed successfully
    - Foreign keys exist in dimension tables
    - Data types are correct
    """
    import subprocess

    cmd = [
        "python",
        "/opt/airflow/scripts/run_checkpoint.py",
        "--suite",
        "fact_orders_suite",
        "--run-date",
        context["ds"],
    ]
    logger.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"DQ check failed for fact_orders_suite: {result.stderr}")
    return result.stdout


def run_dq_check_aggregates(**context):
    """
    Run Great Expectations data quality check on aggregates.

    If this fails, check:
    - Aggregate job completed successfully
    - Aggregation logic is correct
    - Date ranges are valid
    """
    import subprocess

    cmd = [
        "python",
        "/opt/airflow/scripts/run_checkpoint.py",
        "--suite",
        "agg_daily_revenue_suite",
        "--run-date",
        context["ds"],
    ]
    logger.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"DQ check failed for agg_daily_revenue_suite: {result.stderr}")
    return result.stdout


def update_pipeline_manifest(**context):
    """
    Update the pipeline_runs table with run metadata.

    Records:
    - run_id, pipeline_name, start_time, end_time
    - status, rows_processed, error_message

    If this fails, check:
    - MySQL staging database is accessible
    - pipeline_runs table exists
    - Database user has INSERT/UPDATE permissions
    """
    import subprocess

    batch_id = context["ds"]
    cmd = [
        "python",
        "/opt/airflow/scripts/update_pipeline_manifest.py",
        "--batch-id",
        batch_id,
        "--status",
        "success",
    ]
    logger.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.warning(f"Failed to update manifest: {result.stderr}")
        # Don't fail the DAG for manifest update issues
    return result.stdout


def send_success_notification(**context):
    """
    Send a success notification to Slack.

    Includes run statistics:
    - DAG run ID
    - Execution date
    - Duration
    - Rows processed
    """
    dag_run = context["dag_run"]
    execution_date = context["ds"]

    message = f"""
    ✅ E-Commerce Pipeline Completed Successfully

    DAG: {DAG_ID}
    Execution Date: {execution_date}
    Run ID: {dag_run.run_id}
    Duration: {dag_run.end_date - dag_run.start_date if dag_run.end_date else 'N/A'}

    All tasks completed successfully.
    """

    logger.info(f"Success notification: {message}")
    return message


def check_source_availability_task(**context):
    """
    Check if the mock API is healthy before starting ingestion.

    This uses the requests library directly instead of HttpSensor
    because the Airflow 2.8 HttpSensor has issues with connection config.

    If this fails, check:
    - Mock API is running: docker ps | grep mock-api
    - API is reachable: curl http://mock-api:8000/health
    - Network between airflow and mock-api containers
    """
    import requests

    url = "http://mock-api:8000/health"
    logger.info(f"Checking source API health at {url}")

    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            logger.info(f"✅ Source API is healthy: {data}")
            return {"status": "healthy", "response": data}
        else:
            raise Exception(
                f"Source API returned {response.status_code}: {response.text[:200]}"
            )
    except requests.exceptions.RequestException as e:
        raise Exception(f"Cannot reach source API at {url}: {e}")


# ============================================
# DAG DEFINITION
# ============================================

with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="End-to-end e-commerce data pipeline",
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=datetime.strptime(START_DATE, "%Y-%m-%d"),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "daily"],
    sla_miss_callback=sla_miss_callback,
) as dag:
    # ============================================
    # START MARKER
    # ============================================

    start = EmptyOperator(
        task_id="start",
        doc_md="""
        ## Start
        Marks the beginning of the pipeline.
        """,
    )

    # ============================================
    # TASK 1: Check Source Availability
    # ============================================

    # HTTP sensor task — ingestion SLA: 10 minutes
    check_source_availability = PythonOperator(
        task_id="check_source_availability",
        python_callable=check_source_availability_task,
        provide_context=True,
        doc_md="""
        ## Check Source Availability

        Verifies the mock API is healthy before starting ingestion.

        If this fails, check:
        - Mock API is running: `docker ps | grep mock-api`
        - API is reachable from Airflow container: `docker exec ecommerce_airflow curl http://mock-api:8000/health`
        """,
    )

    # ============================================
    # INGESTION TASK GROUP
    # ============================================

    with TaskGroup(
        group_id="ingestion", tooltip="Ingest data from all sources"
    ) as ingestion_group:
        ingest_api_orders = PythonOperator(
            task_id="ingest_api_orders",
            python_callable=run_ingest_api,
            provide_context=True,
            sla=timedelta(minutes=30),
            doc_md="""
            ## Ingest API Orders

            Fetches order data from the mock REST API.

            ### If This Fails
            - Verify API is responding: `curl http://mock-api:8000/orders`
            - Check API logs: `docker logs ecommerce_mock_api`
            - Verify watermark table: `SELECT * FROM watermarks;`
            """,
        )

        ingest_postgres_products = PythonOperator(
            task_id="ingest_postgres_products",
            python_callable=run_ingest_postgres_cdc,
            provide_context=True,
            doc_md="""
            ## Ingest PostgreSQL CDC

            Extracts changed records from PostgreSQL source.

            ### If This Fails
            - Check PostgreSQL connection: `docker exec ecommerce_postgres psql -U admin -c "SELECT 1"`
            - Verify source tables exist
            - Check CDC watermark table
            """,
        )

        ingest_csv_inventory = PythonOperator(
            task_id="ingest_csv_inventory",
            python_callable=run_ingest_csv,
            provide_context=True,
            doc_md="""
            ## Ingest CSV Inventory

            Loads CSV files from the raw folder.

            ### If This Fails
            - Check for new files: `ls /app/data/raw/`
            - Verify CSV headers match schema
            - Check quarantine folder for rejected files
            """,
        )

    # ============================================
    # DATA QUALITY CHECK: RAW
    # ============================================

    dq_check_raw_data = PythonOperator(
        task_id="dq_check_raw_data",
        python_callable=run_dq_check_raw,
        provide_context=True,
        doc_md="""
        ## Data Quality Check: Raw Orders

        Validates staging data using Great Expectations.

        **Suite:** `raw_orders_suite`

        **Checks:**
        - No null order_ids
        - Unique line_ids
        - Valid order statuses
        - Amounts in range

        ### If This Fails
        - Review GE Data Docs for details
        - Check for schema changes in source
        - Verify data isn't corrupted during ingestion
        """,
    )

    # ============================================
    # SPARK PROCESSING TASK GROUP
    # ============================================

    # ============================================
    # SPARK PROCESSING TASK GROUP
    # ============================================

    with TaskGroup(
        group_id="spark_processing", tooltip="Transform data with PySpark"
    ) as spark_group:
        spark_transform_orders = BashOperator(
            task_id="spark_transform_orders",
            bash_command="""
            docker exec ecommerce_pipeline spark-submit \
                --master spark://spark-master:7077 \
                --deploy-mode client \
                --driver-memory 2g \
                --executor-memory 2g \
                --executor-cores 2 \
                --conf "spark.driver.extraClassPath=/opt/spark/jars/*" \
                --conf "spark.executor.extraClassPath=/opt/spark/jars/*" \
                --conf "spark.serializer=org.apache.spark.serializer.JavaSerializer" \
                --jars "/opt/spark/jars/mysql-connector-java-8.0.33.jar,/opt/spark/jars/postgresql-42.6.0.jar" \
                /app/src/spark_jobs/transform_orders.py \
                --batch-id {{ ds }} --env dev
            """,
            doc_md="""
            ## Spark Transform Orders

            Transforms orders from MySQL staging to PostgreSQL warehouse.
            Runs via docker exec on the pipeline container (which has Spark + Java).
            """,
        )

        spark_transform_clickstream = BashOperator(
            task_id="spark_transform_clickstream",
            bash_command="""
            docker exec ecommerce_pipeline spark-submit \
                --master spark://spark-master:7077 \
                --deploy-mode client \
                --driver-memory 2g \
                --executor-memory 2g \
                --executor-cores 2 \
                --conf "spark.driver.extraClassPath=/opt/spark/jars/*" \
                --conf "spark.executor.extraClassPath=/opt/spark/jars/*" \
                --conf "spark.serializer=org.apache.spark.serializer.JavaSerializer" \
                --jars "/opt/spark/jars/mysql-connector-java-8.0.33.jar,/opt/spark/jars/postgresql-42.6.0.jar" \
                /app/src/spark_jobs/transform_clickstream.py \
                --batch-id {{ ds }} --env dev
            """,
            doc_md="""
            ## Spark Transform Clickstream

            Transforms clickstream events from MySQL to PostgreSQL.
            Runs via docker exec on the pipeline container.
            """,
        )

    # ============================================
    # DATA QUALITY CHECK: WAREHOUSE
    # ============================================

    dq_check_warehouse = PythonOperator(
        task_id="dq_check_warehouse",
        python_callable=run_dq_check_warehouse,
        provide_context=True,
        doc_md="""
        ## Data Quality Check: Warehouse

        Validates transformed data in fact_orders.

        **Suite:** `fact_orders_suite`

        ### If This Fails
        - Check Spark transformation logs
        - Verify foreign keys exist (dim_users, dim_products)
        - Check for data type mismatches
        """,
    )

    # ============================================
    # SPARK AGGREGATES
    # ============================================

    spark_compute_aggregates = BashOperator(
        task_id="spark_compute_aggregates",
        bash_command="""
        docker exec ecommerce_pipeline spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --driver-memory 2g \
            --executor-memory 2g \
            --executor-cores 2 \
            --conf "spark.driver.extraClassPath=/opt/spark/jars/*" \
            --conf "spark.executor.extraClassPath=/opt/spark/jars/*" \
            --conf "spark.serializer=org.apache.spark.serializer.JavaSerializer" \
            --jars "/opt/spark/jars/mysql-connector-java-8.0.33.jar,/opt/spark/jars/postgresql-42.6.0.jar" \
            /app/src/spark_jobs/compute_aggregates.py \
            --run-date {{ ds }} --env dev --lookback-days 30
        """,
        doc_md="""
        ## Spark Compute Aggregates

        Computes daily aggregates for analytics dashboards.
        Runs via docker exec on the pipeline container.
        """,
    )

    # ============================================
    # DATA QUALITY CHECK: AGGREGATES
    # ============================================

    dq_check_aggregates = PythonOperator(
        task_id="dq_check_aggregates",
        python_callable=run_dq_check_aggregates,
        provide_context=True,
        doc_md="""
        ## Data Quality Check: Aggregates

        Validates aggregate tables.

        **Suite:** `agg_daily_revenue_suite`

        ### If This Fails
        - Check aggregate computation logs
        - Verify no duplicate dates
        - Check avg_order_value is in valid range
        """,
    )

    # ============================================
    # PIPELINE MANIFEST
    # ============================================

    update_manifest = PythonOperator(
        task_id="update_pipeline_manifest",
        python_callable=update_pipeline_manifest,
        provide_context=True,
        doc_md="""
        ## Update Pipeline Manifest

        Records run metadata in the pipeline_runs table.

        ### If This Fails
        - Check MySQL connection
        - Verify pipeline_runs table exists
        - Non-critical failure - won't fail DAG
        """,
    )

    # ============================================
    # SUCCESS NOTIFICATION
    # ============================================

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=send_success_notification,
        provide_context=True,
        doc_md="""
        ## Notify Success

        Sends a Slack message with pipeline summary.

        ### If This Fails
        - Check Slack webhook URL
        - Verify network connectivity
        - Non-critical failure
        """,
    )

    # ============================================
    # END MARKER
    # ============================================

    end = EmptyOperator(
        task_id="end",
        doc_md="""
        ## End
        Marks the successful completion of the pipeline.
        """,
    )

    # ============================================
    # TASK DEPENDENCIES
    # ============================================

    # Start → Check source
    start >> check_source_availability

    # Check source → All ingestion tasks (parallel)
    check_source_availability >> ingestion_group

    # All ingestion → DQ check raw
    ingestion_group >> dq_check_raw_data

    # DQ raw → Spark processing (parallel)
    dq_check_raw_data >> spark_group

    # Spark processing → DQ warehouse
    spark_group >> dq_check_warehouse

    # DQ warehouse → Aggregates
    dq_check_warehouse >> spark_compute_aggregates

    # Aggregates → DQ aggregates
    spark_compute_aggregates >> dq_check_aggregates

    # DQ aggregates → Update manifest
    dq_check_aggregates >> update_manifest

    # Update manifest → Notify success
    update_manifest >> notify_success

    # Notify success → End
    notify_success >> end
