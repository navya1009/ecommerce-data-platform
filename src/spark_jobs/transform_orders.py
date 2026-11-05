#!/usr/bin/env python3
# isort: skip_file
"""
PySpark ETL Job: Transform Orders
Processes raw order data from MySQL staging into PostgreSQL data warehouse

Usage:
    spark-submit transform_orders.py --batch-id batch_20260101_120000 --env dev

Arguments:
    --batch-id    Unique batch identifier
    --env         Environment: dev or prod
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    lower,
    to_date,
    trim,
    when,
)
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

# spark-submit does not put src/ on the path; utils/ lives beside spark_jobs/
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.lineage import record_lineage  # noqa: E402
from utils.metrics import track_metrics  # noqa: E402

# ============================================
# LOGGING SETUP
# ============================================


def setup_logging():
    """Configure logging with timestamps and levels"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(__name__)


logger = setup_logging()

# ============================================
# ARGUMENT PARSING
# ============================================


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Transform orders ETL job")
    parser.add_argument("--batch-id", required=True, help="Unique batch identifier")
    parser.add_argument(
        "--env",
        default="dev",
        choices=["dev", "prod"],
        help="Environment (dev or prod)",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Maximum records to process (for testing)",
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


def get_config(env: str) -> Dict[str, Any]:
    """
    Get environment-specific configuration
    """
    # Host/port come from env vars so the job can target other stacks (e.g. the
    # integration-test containers). Defaults are the Docker service names/ports.
    # Credentials are read from MYSQL_USER/MYSQL_PASSWORD and POSTGRES_USER/POSTGRES_PASSWORD.

    # MySQL (source) configuration. Credentials come from the environment, with no
    # built-in default; the Airflow and pipeline containers set MYSQL_USER/PASSWORD.
    mysql_config = {
        "host": os.getenv("MYSQL_HOST", "mysql"),
        "port": int(os.getenv("MYSQL_PORT", 3306)),
        "database": "ecommerce_staging",
        "user": _required_env("MYSQL_USER"),
        "password": _required_env("MYSQL_PASSWORD"),
    }

    # PostgreSQL (target) configuration
    postgres_config = {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": int(os.getenv("POSTGRES_PORT", 5432)),
        "database": "postgres",
        "user": _required_env("POSTGRES_USER"),
        "password": _required_env("POSTGRES_PASSWORD"),
    }

    # For production, these would be different
    if env == "prod":
        # In production, these would come from environment variables
        # or a secrets manager
        pass

    return {
        "mysql": mysql_config,
        "postgres": postgres_config,
        "jdbc_drivers": {
            "mysql": "com.mysql.cj.jdbc.Driver",
            "postgres": "org.postgresql.Driver",
        },
        "batch_size": 10000,
        "fetch_size": 10000,
    }


def get_spark_jars_path() -> str:
    """Directory holding the JDBC driver jars (default: the Docker image path)"""
    return os.getenv("SPARK_JARS_PATH", "/opt/spark/jars")


def get_jdbc_url(db_type: str, config: Dict[str, Any]) -> str:
    """
    Build JDBC URL for MySQL or PostgreSQL
    """
    if db_type == "mysql":
        cfg = config["mysql"]
        # Add driver parameter to URL
        return f"jdbc:mysql://{cfg['host']}:{cfg['port']}/{cfg['database']}?useSSL=false&serverTimezone=UTC&driver=com.mysql.cj.jdbc.Driver"  # noqa: E501
    elif db_type == "postgres":
        cfg = config["postgres"]
        return f"jdbc:postgresql://{cfg['host']}:{cfg['port']}/{cfg['database']}"
    else:
        raise ValueError(f"Unknown database type: {db_type}")


def get_jdbc_properties(db_type: str, config: Dict[str, Any]) -> Dict[str, str]:
    """
    Get JDBC connection properties
    """
    if db_type == "mysql":
        cfg = config["mysql"]
        return {
            "driver": config["jdbc_drivers"]["mysql"],
            "user": cfg["user"],
            "password": cfg["password"],
            "fetchsize": str(config["fetch_size"]),
            "useSSL": "false",
            "serverTimezone": "UTC",
        }
    elif db_type == "postgres":
        cfg = config["postgres"]
        return {
            "driver": config["jdbc_drivers"]["postgres"],
            "user": cfg["user"],
            "password": cfg["password"],
            "fetchsize": str(config["fetch_size"]),
        }
    else:
        raise ValueError(f"Unknown database type: {db_type}")


# ============================================
# DATA READING FUNCTIONS
# ============================================


def read_unprocessed_orders(
    spark: SparkSession, config: Dict[str, Any], max_records: Optional[int] = None
) -> DataFrame:
    """
    Read unprocessed records from stg_raw_orders
    """
    logger.info("Reading unprocessed orders from MySQL...")

    # ============================================
    # FIX: Driver registration is handled by Spark config
    # No need for Class.forName() from Python
    # ============================================

    url = get_jdbc_url("mysql", config)
    properties = get_jdbc_properties("mysql", config)

    # Build query with optional limit
    query = """
    SELECT
        id,
        source_order_id,
        source_line_id,
        user_id,
        product_id,
        quantity,
        unit_price,
        discount,
        order_status,
        order_date,
        total_amount
    FROM stg_raw_orders
    WHERE is_processed = 0
"""

    if max_records:
        query += f" LIMIT {max_records}"

    # Read using query
    df = spark.read.jdbc(url=url, table=f"({query}) as orders", properties=properties)

    return df


def read_dimension_tables(
    spark: SparkSession, config: Dict[str, Any]
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Read dimension tables from PostgreSQL
    """
    logger.info("Reading dimension tables from PostgreSQL...")

    # ============================================
    # FIX: Explicitly register PostgreSQL driver
    # ============================================
    try:
        spark.sparkContext._jvm.Class.forName("org.postgresql.Driver")
        logger.info("PostgreSQL driver registered successfully")
    except Exception as e:
        logger.warning(f"Failed to register PostgreSQL driver: {e}")

    url = get_jdbc_url("postgres", config)
    properties = get_jdbc_properties("postgres", config)

    # Read dim_users
    dim_users = spark.read.jdbc(
        url=url, table="warehouse.dim_users", properties=properties
    )

    # Read dim_products
    dim_products = spark.read.jdbc(
        url=url, table="warehouse.dim_products", properties=properties
    )

    # Read dim_date
    dim_date = spark.read.jdbc(
        url=url, table="warehouse.dim_date", properties=properties
    )

    return dim_users, dim_products, dim_date


# ============================================
# DATA CLEANING FUNCTIONS
# ============================================


def clean_orders(df: DataFrame) -> DataFrame:
    """
    Clean the orders DataFrame:
    1. Drop duplicates
    2. Standardize status values
    3. Fill missing discounts
    4. Validate total amount
    5. Cast columns to correct types
    """
    logger.info("Cleaning orders data...")

    # 1. Drop exact duplicates (same source_order_id + source_line_id)
    df_cleaned = df.dropDuplicates(["source_order_id", "source_line_id"])

    duplicates_removed = df.count() - df_cleaned.count()
    logger.info(f"Removed {duplicates_removed} duplicate records")

    # 2. Standardize order_status values
    # Convert to lowercase, trim whitespace, map to standard values
    df_cleaned = df_cleaned.withColumn(
        "order_status_clean",
        when(
            lower(trim(col("order_status"))).isin(
                "completed", "pending", "cancelled", "returned"
            ),
            lower(trim(col("order_status"))),
        ).otherwise("pending"),
    )

    # 3. Fill missing discounts with 0.0
    df_cleaned = df_cleaned.withColumn(
        "discount_clean", when(col("discount").isNull(), 0.0).otherwise(col("discount"))
    )

    # 4. Calculate total_amount and validate
    df_cleaned = df_cleaned.withColumn(
        "calculated_total",
        col("quantity") * col("unit_price") * (1 - col("discount_clean") / 100),
    )

    # Flag rows where total_amount doesn't match
    # Use tolerance for floating point comparison
    df_cleaned = df_cleaned.withColumn(
        "is_valid_amount",
        when(
            spark_abs(col("calculated_total") - col("total_amount")) < 0.01, True
        ).otherwise(False),
    )

    # 5. Cast all columns to correct types
    df_cleaned = df_cleaned.select(
        col("source_order_id").cast(StringType()),
        col("source_line_id").cast(StringType()),
        col("user_id").cast(LongType()),
        col("product_id").cast(LongType()),
        col("quantity").cast(IntegerType()),
        col("unit_price").cast(DecimalType(10, 2)),
        col("discount_clean").alias("discount").cast(DecimalType(5, 2)),
        col("order_status_clean").alias("order_status").cast(StringType()),
        col("order_date").cast(TimestampType()),
        col("total_amount").cast(DecimalType(12, 2)),
        col("calculated_total"),
        col("is_valid_amount"),
        col("id").alias("staging_id"),
    )

    return df_cleaned


def enrich_orders(
    df: DataFrame, dim_users: DataFrame, dim_products: DataFrame, dim_date: DataFrame
) -> DataFrame:
    """
    Enrich orders with dimension data
    """
    logger.info("Enriching orders with dimension data...")

    # 1. Join with dim_users
    df_enriched = df.join(
        dim_users.select("user_id", "segment", "city", "country"),
        on="user_id",
        how="left",
    ).withColumnRenamed("segment", "user_segment")

    # 2. Join with dim_products
    df_enriched = df_enriched.join(
        dim_products.select(
            "product_id", "name", "category", "subcategory", "brand", "cost_price"
        ),
        on="product_id",
        how="left",
    )

    # 3. Join with dim_date (on order date)
    # Convert order_date to date and join
    df_enriched = df_enriched.withColumn("order_date_only", to_date(col("order_date")))

    df_enriched = df_enriched.join(
        dim_date.select("date_id", "date"),
        df_enriched.order_date_only == dim_date.date,
        "left",
    )

    # 4. Derive gross_margin = total_amount - (quantity * cost_price)
    df_enriched = df_enriched.withColumn(
        "gross_margin",
        when(
            col("cost_price").isNotNull(),
            col("total_amount") - (col("quantity") * col("cost_price")),
        ).otherwise(None),
    )

    # 5. Add metadata columns
    df_enriched = df_enriched.withColumn("etl_processed_at", current_timestamp())

    return df_enriched


# ============================================
# DATA VALIDATION
# ============================================


def validate_and_split_data(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Split data into valid and quarantine records
    """
    logger.info("Validating data and splitting...")

    # Define validation rules
    df_validated = df.withColumn(
        "quarantine_reason",
        when(col("source_order_id").isNull(), "Missing source_order_id")
        .when(col("source_line_id").isNull(), "Missing source_line_id")
        .when((col("user_id").isNull()) | (col("user_id") <= 0), "Invalid user_id")
        .when(
            (col("product_id").isNull()) | (col("product_id") <= 0),
            "Invalid product_id",
        )
        .when((col("quantity").isNull()) | (col("quantity") <= 0), "Invalid quantity")
        .when(
            (col("unit_price").isNull()) | (col("unit_price") <= 0),
            "Invalid unit_price",
        )
        .when(col("is_valid_amount") == False, "Total amount mismatch")  # noqa: E712
        .otherwise(None),
    )

    # Split into valid and quarantine
    valid_df = df_validated.filter(col("quarantine_reason").isNull())
    quarantine_df = df_validated.filter(col("quarantine_reason").isNotNull())

    return valid_df, quarantine_df


# ============================================
# LOAD FUNCTIONS
# ============================================


def load_to_fact_orders(df: DataFrame, config: Dict[str, Any], batch_id: str):
    """
    Load valid records to fact_orders using UPSERT
    """
    logger.info("Loading valid records to fact_orders...")

    if df.count() == 0:
        logger.info("No valid records to load")
        return 0

    url = get_jdbc_url("postgres", config)
    properties = get_jdbc_properties("postgres", config)

    # Prepare columns for fact_orders
    fact_df = df.select(
        col("source_order_id").alias("order_id"),
        col("source_line_id").alias("line_item_id"),
        col("user_id"),
        col("product_id"),
        col("date_id"),
        col("user_segment"),
        col("gross_margin"),
        col("quantity"),
        col("unit_price"),
        col("discount"),
        col("total_amount"),
        col("order_status"),
        col("order_date").alias("created_at"),
    )

    # Write to a temporary table first
    temp_table = f"temp_fact_orders_{batch_id.replace('-', '_')}"

    # Write to temp table
    fact_df.write.mode("overwrite").option("truncate", "true").jdbc(
        url=url, table=temp_table, properties=properties
    )

    # Execute UPSERT via JDBC
    # We'll do this using a separate connection
    import psycopg

    pg_config = config["postgres"]
    conn = psycopg.connect(
        host=pg_config["host"],
        port=pg_config["port"],
        dbname=pg_config["database"],
        user=pg_config["user"],
        password=pg_config["password"],
    )

    try:
        with conn.cursor() as cursor:
            # UPSERT: INSERT ... ON CONFLICT DO UPDATE
            upsert_sql = f"""
                INSERT INTO warehouse.fact_orders (
                    order_id, line_item_id, user_id, product_id,
                    date_id, user_segment, gross_margin,
                    quantity, unit_price, discount, total_amount,
                    status, created_at
                )
                SELECT
                    CAST(order_id AS BIGINT), CAST(line_item_id AS BIGINT),
                    user_id, product_id,
                    date_id, user_segment, gross_margin,
                    quantity, unit_price, discount, total_amount,
                    order_status, created_at
                FROM {temp_table}
                ON CONFLICT (order_id, line_item_id)
                DO UPDATE SET
                    user_id = EXCLUDED.user_id,
                    product_id = EXCLUDED.product_id,
                    date_id = EXCLUDED.date_id,
                    user_segment = EXCLUDED.user_segment,
                    gross_margin = EXCLUDED.gross_margin,
                    quantity = EXCLUDED.quantity,
                    unit_price = EXCLUDED.unit_price,
                    discount = EXCLUDED.discount,
                    total_amount = EXCLUDED.total_amount,
                    status = EXCLUDED.status,
                    created_at = EXCLUDED.created_at
            """
            cursor.execute(upsert_sql)

            # Drop temp table
            cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")

            conn.commit()

    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to load to fact_orders: {e}")
        raise
    finally:
        conn.close()

    return df.count()


def load_to_quarantine(df: DataFrame, config: Dict[str, Any], batch_id: str):
    """
    Load invalid records to quarantine table with reason
    """
    logger.info("Loading quarantine records...")

    if df.count() == 0:
        logger.info("No quarantine records to load")
        return 0

    url = get_jdbc_url("postgres", config)
    properties = get_jdbc_properties("postgres", config)

    # Prepare columns for quarantine
    quarantine_df = df.select(
        col("source_order_id"),
        col("source_line_id"),
        col("user_id"),
        col("product_id"),
        col("quantity"),
        col("unit_price"),
        col("discount"),
        col("order_status"),
        col("order_date"),
        col("total_amount"),
        col("calculated_total"),
        col("quarantine_reason"),
        col("staging_id"),
        lit(batch_id).alias("batch_id"),
    )

    # Create quarantine table if not exists
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS warehouse.quarantine_orders (
            id SERIAL PRIMARY KEY,
            source_order_id VARCHAR(100),
            source_line_id VARCHAR(100),
            user_id BIGINT,
            product_id BIGINT,
            quantity INTEGER,
            unit_price DECIMAL(10,2),
            discount DECIMAL(5,2),
            order_status VARCHAR(50),
            order_date TIMESTAMP,
            total_amount DECIMAL(12,2),
            calculated_total DECIMAL(12,2),
            quarantine_reason TEXT,
            staging_id BIGINT,
            batch_id VARCHAR(100),
            quarantined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """

    import psycopg

    pg_config = config["postgres"]
    conn = psycopg.connect(
        host=pg_config["host"],
        port=pg_config["port"],
        dbname=pg_config["database"],
        user=pg_config["user"],
        password=pg_config["password"],
    )

    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_sql)
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to create quarantine table: {e}")
        raise
    finally:
        conn.close()

    # Write to quarantine
    quarantine_df.write.mode("append").jdbc(
        url=url, table="warehouse.quarantine_orders", properties=properties
    )

    return df.count()


def update_processed_flag(df: DataFrame, config: Dict[str, Any]):
    """
    Mark processed rows in stg_raw_orders as is_processed = true
    """
    logger.info("Updating processed flag in MySQL...")

    if df.count() == 0:
        logger.info("No records to mark as processed")
        return 0

    # Get staging IDs from the valid records
    staging_ids = [row.staging_id for row in df.select("staging_id").collect()]

    if not staging_ids:
        return 0

    # Update using batch
    import mysql.connector

    mysql_config = config["mysql"]

    conn = mysql.connector.connect(
        host=mysql_config["host"],
        port=mysql_config["port"],
        user=mysql_config["user"],
        password=mysql_config["password"],
        database=mysql_config["database"],
    )

    try:
        cursor = conn.cursor()

        # Update in batches
        batch_size = 1000
        for i in range(0, len(staging_ids), batch_size):
            batch = staging_ids[i : i + batch_size]  # noqa: E203
            placeholders = ",".join(["%s"] * len(batch))
            update_sql = f"""
                UPDATE stg_raw_orders
                SET is_processed = 1, processed_at = NOW()
                WHERE id IN ({placeholders})
            """
            cursor.execute(update_sql, batch)

        conn.commit()
        affected_rows = cursor.rowcount
        cursor.close()

        logger.info(f"Marked {affected_rows} rows as processed")
        return affected_rows

    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to update processed flag: {e}")
        raise
    finally:
        conn.close()


# ============================================
# MAIN ETL FUNCTION
# ============================================


def _run_etl(args: argparse.Namespace, metrics) -> Dict[str, Any]:
    """
    Main ETL orchestration function
    """
    start_time = datetime.now()
    logger.info(f"Starting ETL job with batch-id: {args.batch_id}")

    # 1. Get configuration
    config = get_config(args.env)

    # 2. Create SparkSession
    jars_dir = get_spark_jars_path()
    jar_paths = [
        os.path.join(jars_dir, "mysql-connector-java-8.0.33.jar"),
        os.path.join(jars_dir, "postgresql-42.6.0.jar"),
    ]
    spark = (
        SparkSession.builder.appName(f"TransformOrders_{args.batch_id}")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
        .config("spark.jars", ",".join(jar_paths))
        .config("spark.driver.extraClassPath", os.pathsep.join(jar_paths))
        .config("spark.executor.extraClassPath", os.pathsep.join(jar_paths))
        .config("spark.kryo.registrationRequired", "false")
        .config("spark.sql.adaptive.coalescePartitions.parallelismFirst", "false")
        .getOrCreate()
    )

    # Set job description for Spark UI
    spark.sparkContext.setJobDescription(f"ETL Orders - Batch {args.batch_id}")

    try:
        # 3. Read data
        source_df = read_unprocessed_orders(spark, config, args.max_records)
        rows_read = source_df.count()
        logger.info(f"Read {rows_read} unprocessed records")

        if rows_read == 0:
            logger.info("No new records to process. Exiting gracefully.")
            return {
                "rows_read": 0,
                "rows_loaded": 0,
                "rows_quarantined": 0,
                "processing_time": (datetime.now() - start_time).total_seconds(),
                "batch_id": args.batch_id,
                "status": "success_no_data",
            }

        # 4. Read dimension tables
        dim_users, dim_products, dim_date = read_dimension_tables(spark, config)

        # 5. Clean data
        cleaned_df = clean_orders(source_df)

        # 6. Enrich data
        enriched_df = enrich_orders(cleaned_df, dim_users, dim_products, dim_date)

        # 7. Validate and split
        valid_df, quarantine_df = validate_and_split_data(enriched_df)
        rows_valid = valid_df.count()
        rows_quarantined = quarantine_df.count()
        logger.info(f"Valid: {rows_valid}, Quarantine: {rows_quarantined}")
        metrics.rows_read = rows_read

        # 8. Load data
        loaded_count = 0
        if rows_valid > 0:
            loaded_count = load_to_fact_orders(valid_df, config, args.batch_id)
            logger.info(f"Loaded {loaded_count} records to fact_orders")

            # Lineage is best-effort: a lineage DB failure must not fail the job
            try:
                record_lineage(
                    run_id=os.getenv("PIPELINE_RUN_ID") or args.batch_id,
                    source_system="mysql_staging",
                    source_table="stg_raw_orders",
                    target_table="warehouse.fact_orders",
                    rows_written=loaded_count,
                    transformation_applied="clean+dedupe+enrich+validate",
                )
            except Exception as lineage_error:
                logger.warning(f"Lineage record failed, continuing: {lineage_error}")

        quarantine_count = 0
        if rows_quarantined > 0:
            quarantine_count = load_to_quarantine(quarantine_df, config, args.batch_id)
            logger.info(f"Quarantined {quarantine_count} records")

        # 9. Update processed flag
        if rows_valid > 0:
            updated_count = update_processed_flag(valid_df, config)
            logger.info(f"Updated {updated_count} rows as processed")

        metrics.rows_transformed = rows_valid + rows_quarantined
        metrics.rows_loaded = loaded_count
        metrics.rows_quarantined = quarantine_count

        # 10. Calculate summary
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()

        summary = {
            "rows_read": rows_read,
            "rows_loaded": loaded_count,
            "rows_quarantined": quarantine_count,
            "processing_time": processing_time,
            "batch_id": args.batch_id,
            "env": args.env,
            "status": "success",
        }

        logger.info(f"ETL completed successfully: {summary}")
        return summary

    except Exception as e:
        logger.error(f"ETL failed: {e}")
        import traceback

        traceback.print_exc()
        return {
            "rows_read": 0,
            "rows_loaded": 0,
            "rows_quarantined": 0,
            "processing_time": (datetime.now() - start_time).total_seconds(),
            "batch_id": args.batch_id,
            "status": f"failed: {str(e)}",
        }
    finally:
        spark.stop()


# ============================================
# COMMAND LINE ENTRY POINT
# ============================================


def run_etl(args: argparse.Namespace) -> Dict[str, Any]:
    """
    Run the orders ETL inside a metrics context: one pipeline_metrics row per run,
    plus CloudWatch when configured. Metrics failures never fail the job.
    """
    run_id = os.getenv("PIPELINE_RUN_ID") or args.batch_id
    with track_metrics(
        pipeline_name="spark_transform_orders", run_id=run_id
    ) as metrics:
        summary = _run_etl(args, metrics)
        metrics.status = (
            "failed" if summary["status"].startswith("failed") else "success"
        )
        return summary


if __name__ == "__main__":
    args = parse_arguments()
    result = run_etl(args)

    # Print summary to console
    print("\n" + "=" * 60)
    print("ETL SUMMARY")
    print("=" * 60)
    print(f"Batch ID:       {result['batch_id']}")
    print(f"Rows Read:      {result['rows_read']}")
    print(f"Rows Loaded:    {result['rows_loaded']}")
    print(f"Rows Quarantined: {result['rows_quarantined']}")
    print(f"Processing Time: {result['processing_time']:.2f} seconds")
    print(f"Status:         {result['status']}")
    print("=" * 60)

    # Exit with appropriate code
    is_success = result.get("status", "").startswith("success")
    sys.exit(0 if is_success else 1)
