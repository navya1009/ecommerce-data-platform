"""
Alternative trigger pattern using Airflow S3KeySensor.

Instead of AWS Lambda, this sensor polls S3 for new files and triggers
the DAG when found. Use this when:
- You don't have Lambda access
- You want simpler setup
- Polling delay (1-5 minutes) is acceptable
"""

from datetime import datetime, timedelta

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from airflow import DAG


def process_s3_file(**context):
    """Process the S3 file once the sensor succeeds."""
    import logging

    logger = logging.getLogger(__name__)

    # Get S3 key from the sensor's XCom
    s3_key = context["ti"].xcom_pull(task_ids="wait_for_file")
    logger.info(f"Processing S3 file: {s3_key}")

    # Trigger the main pipeline DAG
    from airflow.api.common.trigger_dag import trigger_dag

    trigger_dag(
        dag_id="ecommerce_daily_pipeline",
        conf={"s3_key": s3_key, "trigger_source": "s3_sensor"},
        replace_microseconds=False,
    )


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="s3_file_trigger",
    default_args=default_args,
    description="Watch S3 for new files and trigger ecommerce pipeline",
    schedule_interval="*/5 * * * *",  # Check every 5 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["trigger", "s3", "sensor"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Wait for a new CSV file in the raw prefix
    wait_for_file = S3KeySensor(
        task_id="wait_for_file",
        bucket_name="ecommerce-raw-data",
        bucket_key="raw/orders/*.csv",
        wildcard_match=True,
        timeout=300,  # Fail after 5 minutes if no file
        poke_interval=30,  # Check every 30 seconds
        mode="poke",  # Keep worker occupied while waiting
        aws_conn_id="aws_default",
        soft_fail=True,  # Don't fail if no file - just skip
    )

    trigger_pipeline = PythonOperator(
        task_id="trigger_pipeline",
        python_callable=process_s3_file,
        provide_context=True,
    )

    end = EmptyOperator(task_id="end")

    start >> wait_for_file >> trigger_pipeline >> end
