#!/usr/bin/env python3
# isort: skip_file
"""
PySpark ETL Job: Transform Clickstream
Processes raw clickstream data from MySQL staging into PostgreSQL data warehouse
"""
import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Dict, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, date_format, lit
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import row_number, unix_timestamp, when
from pyspark.sql.window import Window

# ============================================
# LOGGING SETUP
# ============================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
# spark-submit does not put src/ on the path; utils/ lives beside spark_jobs/
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.lineage import record_lineage  # noqa: E402
from utils.logger import get_logger  # noqa: E402
from utils.metrics import track_metrics  # noqa: E402

logger = get_logger(__name__)


def resolve_run_id(args) -> str:
    """PIPELINE_RUN_ID, else the batch id, else a locally generated batch id."""
    return (
        os.getenv("PIPELINE_RUN_ID")
        or getattr(args, "batch_id", None)
        or f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )


# ============================================
# ARGUMENT PARSING
# ============================================


def parse_arguments():
    parser = argparse.ArgumentParser(description="Transform clickstream ETL job")
    parser.add_argument("--batch-id", required=True, help="Unique batch identifier")
    parser.add_argument(
        "--env", default="dev", choices=["dev", "prod"], help="Environment"
    )
    parser.add_argument(
        "--max-records", type=int, default=None, help="Maximum records for testing"
    )
    return parser.parse_args()


# ============================================
# CONFIGURATION
# ============================================


def _required_env(name: str) -> str:
    """Credentials have no built-in default. Fail with a clear message if unset."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"{name} must be set in the environment (no default is built in)"
        )
    return value


def get_config(env: str):
    # Host, port, user and password are all read from the environment (see docker-compose.yml).
    mysql_config = {
        "host": os.getenv("MYSQL_HOST", "mysql"),
        "port": int(os.getenv("MYSQL_PORT", 3306)),
        "database": "ecommerce_staging",
        "user": _required_env("MYSQL_USER"),
        "password": _required_env("MYSQL_PASSWORD"),
    }
    postgres_config = {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": int(os.getenv("POSTGRES_PORT", 5432)),
        "database": "postgres",
        "user": _required_env("POSTGRES_USER"),
        "password": _required_env("POSTGRES_PASSWORD"),
    }
    return {"mysql": mysql_config, "postgres": postgres_config, "batch_size": 10000}


def get_jdbc_url(db_type: str, config: Dict):
    if db_type == "mysql":
        cfg = config["mysql"]
        return f"jdbc:mysql://{cfg['host']}:{cfg['port']}/{cfg['database']}?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"  # noqa: E501
    else:
        cfg = config["postgres"]
        return f"jdbc:postgresql://{cfg['host']}:{cfg['port']}/{cfg['database']}"


def get_jdbc_properties(db_type: str, config: Dict):
    if db_type == "mysql":
        cfg = config["mysql"]
        return {
            "driver": "com.mysql.cj.jdbc.Driver",
            "user": cfg["user"],
            "password": cfg["password"],
        }
    else:
        cfg = config["postgres"]
        return {
            "driver": "org.postgresql.Driver",
            "user": cfg["user"],
            "password": cfg["password"],
        }


# ============================================
# USER AGENT PARSING
# ============================================


def parse_user_agent(df: DataFrame) -> DataFrame:
    """
    Parse user_agent string to extract device_type, browser, OS
    """
    logger.info("Parsing user agent strings...")

    # Device type detection
    df_parsed = df.withColumn(
        "device_type",
        when(col("browser_name").rlike("Mobile|Android|iPhone|iPad|iPod"), "mobile")
        .when(col("browser_name").rlike("Tablet|iPad"), "tablet")
        .otherwise("desktop"),
    )

    # Extract OS
    df_parsed = df_parsed.withColumn(
        "os",
        when(col("browser_name").rlike("Windows"), "Windows")
        .when(col("browser_name").rlike("Mac|OS X"), "macOS")
        .when(col("browser_name").rlike("Android"), "Android")
        .when(col("browser_name").rlike("iPhone|iOS"), "iOS")
        .otherwise("Other"),
    )

    # Extract browser (already have browser_name, just standardize)
    df_parsed = df_parsed.withColumn(
        "browser",
        when(col("browser_name").rlike("Chrome"), "Chrome")
        .when(col("browser_name").rlike("Firefox"), "Firefox")
        .when(col("browser_name").rlike("Safari"), "Safari")
        .when(col("browser_name").rlike("Edge|Internet Explorer"), "Edge")
        .otherwise("Other"),
    )

    return df_parsed


# ============================================
# SESSION ANALYSIS
# ============================================


def analyze_sessions(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Calculate session duration and flag bot traffic
    """
    logger.info("Analyzing sessions...")

    # Window specification for session analysis
    session_window = Window.partitionBy("session_id")

    # Calculate session metrics
    session_metrics = (
        df.withColumn(
            "first_event_ts", spark_min("event_timestamp").over(session_window)
        )
        .withColumn("last_event_ts", spark_max("event_timestamp").over(session_window))
        .withColumn("event_count", count("event_id").over(session_window))
        .withColumn(
            "session_duration_seconds",
            (
                unix_timestamp(col("last_event_ts"))
                - unix_timestamp(col("first_event_ts"))
            ),
        )
    )

    # Flag bot traffic: >200 events in <60 seconds
    session_metrics = session_metrics.withColumn(
        "is_bot",
        when(
            (col("event_count") > 200) & (col("session_duration_seconds") < 60), True
        ).otherwise(False),
    )

    # Separate bot sessions
    bot_events = session_metrics.filter(col("is_bot") == True)  # noqa: E712
    valid_events = session_metrics.filter(col("is_bot") == False)  # noqa: E712

    return valid_events, bot_events


def prepare_bot_traffic_table(bot_df: DataFrame) -> DataFrame:
    """
    Prepare bot traffic data for loading
    """
    bot_summary = (
        bot_df.select("session_id", "user_id", "event_id", "event_timestamp", "is_bot")
        .withColumn(
            "event_count", count("event_id").over(Window.partitionBy("session_id"))
        )
        .withColumn(
            "first_event_at",
            spark_min("event_timestamp").over(Window.partitionBy("session_id")),
        )
        .withColumn(
            "last_event_at",
            spark_max("event_timestamp").over(Window.partitionBy("session_id")),
        )
        .withColumn(
            "duration_seconds",
            (
                unix_timestamp(col("last_event_at"))
                - unix_timestamp(col("first_event_at"))
            ),
        )
        .withColumn("reason", lit("Too many events (>200) in under 60 seconds"))
        .select(
            "event_id",
            "session_id",
            "user_id",
            "event_count",
            "first_event_at",
            "last_event_at",
            "duration_seconds",
            "reason",
        )
        .dropDuplicates(["session_id"])
    )

    return bot_summary


# ============================================
# DEDUPLICATION
# ============================================


def deduplicate_events(df: DataFrame) -> DataFrame:
    """
    Deduplicate events by event_id, keeping the earliest occurrence
    """
    logger.info("Deduplicating events...")

    window = Window.partitionBy("event_id").orderBy("event_timestamp")
    df_dedup = (
        df.withColumn("row_num", row_number().over(window))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    return df_dedup


# ============================================
# MAIN ETL FUNCTION
# ============================================


def _run_etl(args, metrics):
    start_time = datetime.now()
    run_id = resolve_run_id(args)
    logger.info(
        "Starting clickstream ETL",
        extra={"batch_id": args.batch_id, "pipeline_run_id": run_id},
    )

    config = get_config(args.env)

    # Create Spark session. SPARK_JARS_PATH defaults to the Docker image location.
    jars_dir = os.getenv("SPARK_JARS_PATH", "/opt/spark/jars")
    jar_paths = [
        os.path.join(jars_dir, "mysql-connector-java-8.0.33.jar"),
        os.path.join(jars_dir, "postgresql-42.6.0.jar"),
    ]
    spark = (
        SparkSession.builder.appName(f"TransformClickstream_{args.batch_id}")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
        .config("spark.jars", ",".join(jar_paths))
        .config("spark.driver.extraClassPath", os.pathsep.join(jar_paths))
        .config("spark.executor.extraClassPath", os.pathsep.join(jar_paths))
        .getOrCreate()
    )

    spark.sparkContext.setJobDescription(f"ETL Clickstream - Batch {args.batch_id}")

    try:
        # 1. Read unprocessed clickstream from MySQL
        logger.info("Reading unprocessed clickstream from MySQL...")
        mysql_url = get_jdbc_url("mysql", config)
        mysql_props = get_jdbc_properties("mysql", config)

        query = """
            SELECT
                event_id,
                user_id,
                session_id,
                event_type,
                page_url,
                product_id,
                device_type,
                browser_name,
                event_timestamp,
                ingested_at,
                batch_id
            FROM stg_raw_clickstream
            WHERE is_processed = 0
        """
        if args.max_records:
            query += f" LIMIT {args.max_records}"

        source_df = spark.read.jdbc(
            url=mysql_url, table=f"({query}) as clickstream", properties=mysql_props
        )

        rows_read = source_df.count()
        metrics.rows_read = rows_read
        logger.info(
            "Read unprocessed clickstream",
            extra={"rows_read": rows_read, "batch_id": args.batch_id},
        )

        if rows_read == 0:
            logger.info("No new records to process")
            return {
                "rows_read": 0,
                "rows_loaded": 0,
                "bot_events": 0,
                "status": "success_no_data",
            }

        # 2. Deduplicate
        dedup_df = deduplicate_events(source_df)
        dedup_count = dedup_df.count()
        logger.info(f"After deduplication: {dedup_count} records")

        # 3. Parse user agent
        parsed_df = parse_user_agent(dedup_df)

        # 4. Analyze sessions
        valid_df, bot_df = analyze_sessions(parsed_df)
        bot_summary_df = prepare_bot_traffic_table(bot_df)

        valid_count = valid_df.count()
        bot_count = bot_df.count()
        bot_session_count = bot_summary_df.count()

        logger.info(
            "Clickstream sessions analysed",
            extra={
                "rows_cleaned": dedup_count,
                "rows_quarantined": bot_count,
                "valid_events": valid_count,
                "bot_sessions_detected": bot_session_count,
                "batch_id": args.batch_id,
            },
        )
        # Clickstream has no separate quarantine: bot-flagged events are diverted
        # out of the fact table, so they are what the quarantine metric counts.
        metrics.rows_transformed = dedup_count
        metrics.rows_quarantined = bot_count

        # 5. Prepare for loading to PostgreSQL
        pg_url = get_jdbc_url("postgres", config)
        pg_props = get_jdbc_properties("postgres", config)

        from pyspark.sql.types import IntegerType, LongType

        # Prepare fact_clickstream. Every deduplicated event with a user is written;
        # bot-flagged events carry is_bot = TRUE so bot traffic stays queryable and
        # the analytics queries filter them out. Anonymous events are skipped because
        # fact_clickstream.user_id is NOT NULL.
        fact_source = valid_df.unionByName(bot_df).filter(col("user_id").isNotNull())
        fact_total = fact_source.count()
        if fact_total > 0:
            fact_df = fact_source.select(
                col(
                    "event_id"
                ),  # source event id: the deployed table keys on it (varchar)
                col("user_id").cast(LongType()),  # Cast to BIGINT
                col("product_id").cast(LongType()),  # Cast to BIGINT
                # date_id comes from the event time; the dim_date foreign key requires it
                date_format(col("event_timestamp"), "yyyyMMdd")
                .cast(IntegerType())
                .alias("date_id"),
                col("session_id"),
                col("event_type"),
                col("page_url").alias("page"),
                col("device_type").alias("device"),  # canonical name (migration 007)
                col("browser"),
                col("os"),
                col("session_duration_seconds").cast(IntegerType()),
                col("is_bot"),
                col("event_timestamp").alias("created_at"),  # event time, not load time
            )

            # Write to fact_clickstream
            fact_df.write.mode("append").jdbc(
                url=pg_url, table="warehouse.fact_clickstream", properties=pg_props
            )
            logger.info(
                "Loaded events to fact_clickstream",
                extra={
                    "rows_loaded": fact_total,
                    "bot_events": bot_count,
                    "batch_id": args.batch_id,
                },
            )
            metrics.rows_loaded = fact_total

            # Freshness: how far the newest loaded event lags behind now (UTC).
            latest_event = fact_source.agg(spark_max("event_timestamp")).collect()[0][0]
            if latest_event is not None:
                now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
                metrics.data_freshness_lag_minutes = max(
                    0.0, (now_utc - latest_event).total_seconds() / 60
                )

            # Lineage is best-effort: a lineage DB failure must not fail the job
            try:
                record_lineage(
                    run_id=run_id,
                    source_system="mysql_staging",
                    source_table="stg_raw_clickstream",
                    target_table="warehouse.fact_clickstream",
                    rows_written=fact_total,
                    transformation_applied="clean+bot_detection+enrich",
                )
            except Exception as lineage_error:
                logger.warning(
                    "Lineage record failed, continuing",
                    extra={"error": str(lineage_error)},
                )

        # 6. Load bot traffic
        if bot_session_count > 0:
            bot_summary_df = bot_summary_df.select(
                col("event_id"),
                col("session_id"),
                col("user_id").cast(LongType()),  # Cast to BIGINT
                col("event_count").cast(IntegerType()),
                col("first_event_at"),
                col("last_event_at"),
                col("duration_seconds").cast(IntegerType()),
                col("reason"),
            )

        # 7. Update processed flag in MySQL
        if rows_read > 0:
            # Mark every deduplicated event (bots and anonymous included) so it is not re-read
            event_ids = [
                row.event_id
                for row in valid_df.unionByName(bot_df).select("event_id").collect()
            ]
            if event_ids:
                import mysql.connector

                mysql_config = config["mysql"]
                conn = mysql.connector.connect(
                    host=mysql_config["host"],
                    port=mysql_config["port"],
                    user=mysql_config["user"],
                    password=mysql_config["password"],
                    database=mysql_config["database"],
                )
                cursor = conn.cursor()

                # Update in batches
                batch_size = 1000
                for i in range(0, len(event_ids), batch_size):
                    batch = event_ids[i : i + batch_size]  # noqa: E203
                    placeholders = ",".join(["%s"] * len(batch))
                    update_sql = f"""
                        UPDATE stg_raw_clickstream
                        SET is_processed = 1, processed_at = NOW()
                        WHERE event_id IN ({placeholders})
                    """
                    cursor.execute(update_sql, batch)

                conn.commit()
                cursor.close()
                conn.close()
                logger.info(f"Marked {len(event_ids)} events as processed")

        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()

        summary = {
            "rows_read": rows_read,
            "rows_loaded": fact_total,
            "bot_events": bot_count,
            "bot_sessions": bot_session_count,
            "processing_time": processing_time,
            "batch_id": args.batch_id,
            "status": "success",
        }

        logger.info(f"ETL completed: {summary}")
        return summary

    except Exception as e:
        logger.error(f"ETL failed: {e}")
        import traceback

        traceback.print_exc()
        return {
            "rows_read": 0,
            "rows_loaded": 0,
            "bot_events": 0,
            "status": f"failed: {str(e)}",
        }
    finally:
        spark.stop()


# ============================================
# COMMAND LINE ENTRY POINT
# ============================================


def run_etl(args):
    """
    Run the clickstream ETL inside a metrics context: one pipeline_metrics row per
    run, plus CloudWatch when configured. Metrics failures never fail the job.
    """
    with track_metrics(
        pipeline_name="spark_transform_clickstream", run_id=resolve_run_id(args)
    ) as metrics:
        summary = _run_etl(args, metrics)
        metrics.status = (
            "failed" if summary["status"].startswith("failed") else "success"
        )
        return summary


if __name__ == "__main__":
    args = parse_arguments()
    result = run_etl(args)

    print("\n" + "=" * 60)
    print("ETL SUMMARY")
    print("=" * 60)
    print(f"Batch ID:       {result.get('batch_id', 'N/A')}")
    print(f"Rows Read:      {result.get('rows_read', 0)}")
    print(f"Rows Loaded:    {result.get('rows_loaded', 0)}")
    print(f"Bot Events:     {result.get('bot_events', 0)}")
    print(f"Bot Sessions:   {result.get('bot_sessions', 0)}")
    print(f"Processing Time: {result.get('processing_time', 0):.2f} seconds")
    print(f"Status:         {result.get('status', 'unknown')}")
    print("=" * 60)
