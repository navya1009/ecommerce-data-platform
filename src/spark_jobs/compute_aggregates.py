#!/usr/bin/env python3
# isort: skip_file
"""
PySpark ETL Job: Compute Daily Aggregates
Computes daily revenue, product performance, user cohorts, and inventory health
"""
import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg as spark_avg
from pyspark.sql.functions import col, countDistinct, date_format, first, lit
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import round
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import when
from pyspark.sql.types import IntegerType, LongType
from pyspark.sql.window import Window

# ============================================
# LOGGING
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

# ============================================
# ARGUMENT PARSING
# ============================================


def parse_arguments():
    parser = argparse.ArgumentParser(description="Compute daily aggregates")
    parser.add_argument(
        "--run-date", required=True, help="Date to compute aggregates for (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--env", default="dev", choices=["dev", "prod"], help="Environment"
    )
    parser.add_argument(
        "--lookback-days", type=int, default=30, help="Days to look back for averages"
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
    return {
        "postgres": {
            # Host/port/user/password all come from the environment (see docker-compose.yml)
            "host": os.getenv("POSTGRES_HOST", "postgres"),
            "port": int(os.getenv("POSTGRES_PORT", 5432)),
            "database": "postgres",
            "user": _required_env("POSTGRES_USER"),
            "password": _required_env("POSTGRES_PASSWORD"),
        }
    }


def get_jdbc_url(config: dict):
    pg = config["postgres"]
    return f"jdbc:postgresql://{pg['host']}:{pg['port']}/{pg['database']}"


def get_jdbc_properties(config: dict):
    pg = config["postgres"]
    return {
        "driver": "org.postgresql.Driver",
        "user": pg["user"],
        "password": pg["password"],
    }


# ============================================
# SPARK SESSION
# ============================================


def create_spark_session(batch_id: str):
    # SPARK_JARS_PATH defaults to the Docker image location
    jars_dir = os.getenv("SPARK_JARS_PATH", "/opt/spark/jars")
    jar_paths = [
        os.path.join(jars_dir, "mysql-connector-java-8.0.33.jar"),
        os.path.join(jars_dir, "postgresql-42.6.0.jar"),
    ]
    return (
        SparkSession.builder.appName(f"ComputeAggregates_{batch_id}")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
        .config("spark.jars", ",".join(jar_paths))
        .config("spark.driver.extraClassPath", os.path.join(jars_dir, "*"))
        .config("spark.executor.extraClassPath", os.path.join(jars_dir, "*"))
        .getOrCreate()
    )


# ============================================
# AGGREGATION FUNCTIONS
# ============================================


def compute_daily_revenue(
    fact_orders: DataFrame, dim_date: DataFrame, run_date: str
) -> DataFrame:
    """
    Compute daily revenue aggregates
    """
    logger.info("Computing daily revenue...")

    # Join fact_orders with dim_date to get the actual date
    orders_with_date = fact_orders.join(
        dim_date, fact_orders.date_id == dim_date.date_id, "inner"
    )

    # Filter to run date
    date_df = orders_with_date.filter(col("date") == run_date)

    # If no orders for this date, return empty
    if date_df.count() == 0:
        logger.info(f"No orders found for {run_date}")
        return fact_orders.sparkSession.createDataFrame(
            [],
            """
            date_id INTEGER,
            date DATE,
            total_orders INTEGER,
            total_revenue DECIMAL(15,2),
            avg_order_value DECIMAL(10,2),
            total_discount_given DECIMAL(15,2),
            unique_customers INTEGER
        """,
        )

    # Compute aggregates
    revenue_df = (
        date_df.groupBy("date")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            spark_sum("total_amount").alias("total_revenue"),
            spark_avg("total_amount").alias("avg_order_value"),
            spark_sum("discount").alias("total_discount_given"),
            countDistinct("user_id").alias("unique_customers"),
        )
        .withColumn("date_id", date_format(col("date"), "yyyyMMdd").cast(IntegerType()))
    )

    return revenue_df


def compute_product_performance(
    fact_orders: DataFrame, dim_date: DataFrame, clickstream: DataFrame, run_date: str
) -> DataFrame:
    """
    Compute product performance aggregates
    """
    logger.info("Computing product performance...")

    # Join fact_orders with dim_date to get actual date
    orders_with_date = fact_orders.join(
        dim_date, fact_orders.date_id == dim_date.date_id, "inner"
    )

    # Filter to run date
    orders_df = orders_with_date.filter(col("date") == run_date)
    clicks_df = clickstream.filter(col("event_timestamp").cast("date") == run_date)

    # Product sales
    sales_df = orders_df.groupBy("product_id").agg(
        spark_sum("quantity").alias("units_sold"),
        spark_sum("total_amount").alias("revenue"),
        spark_sum(when(col("status") == "returned", 1).otherwise(0)).alias("returns"),
    )

    # Product clicks and views
    clicks_agg = (
        clicks_df.filter(col("event_type").isin("page_view", "add_to_cart", "purchase"))
        .groupBy("product_id")
        .agg(
            spark_sum(when(col("event_type") == "page_view", 1).otherwise(0)).alias(
                "product_views"
            ),
            spark_sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias(
                "add_to_carts"
            ),
            spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias(
                "purchases"
            ),
        )
    )

    # Join and compute conversion rate
    performance_df = (
        sales_df.join(clicks_agg, "product_id", "full_outer")
        .fillna(0)
        .withColumn(
            "conversion_rate",
            when(
                col("product_views") > 0,
                round((col("purchases") / col("product_views")) * 100, 2),
            ).otherwise(0),
        )
        .select(
            col("product_id").cast(LongType()),  # Cast to BIGINT
            "units_sold",
            "revenue",
            "returns",
            "product_views",
            "add_to_carts",
            "conversion_rate",
        )
        .withColumn("date", lit(run_date).cast("date"))
        .withColumn("date_id", date_format(col("date"), "yyyyMMdd").cast(IntegerType()))
        .filter(col("product_id").isNotNull())
    )  # Remove NULL product_ids

    return performance_df


def compute_user_cohorts(
    users: DataFrame, fact_orders: DataFrame, dim_date: DataFrame
) -> DataFrame:
    """
    Compute user cohort retention analysis
    """
    logger.info("Computing user cohorts...")

    # Join fact_orders with dim_date
    orders_with_date = fact_orders.join(
        dim_date, fact_orders.date_id == dim_date.date_id, "inner"
    )

    # Get user signup month and orders
    user_cohorts = users.select(
        "user_id", date_format(col("created_at"), "yyyy-MM").alias("signup_month")
    )

    # Get orders with user signup month
    orders_with_cohort = orders_with_date.join(user_cohorts, "user_id").withColumn(
        "cohort_month", date_format(col("date"), "yyyy-MM")
    )

    # Compute cohorts
    cohort_df = (
        orders_with_cohort.groupBy("signup_month", "cohort_month")
        .agg(
            countDistinct("user_id").alias("active_users"),
            spark_sum("total_amount").alias("total_revenue"),
        )
        .withColumn(
            "revenue_per_user", round(col("total_revenue") / col("active_users"), 2)
        )
    )

    # Calculate retention rate relative to first month
    cohort_window = Window.partitionBy("signup_month").orderBy("cohort_month")
    first_month_users = (  # noqa: F841  (result not returned; see Phase 11 extension report)
        cohort_df.withColumn(
            "first_month_users", first("active_users").over(cohort_window)
        )
        .withColumn(
            "retention_rate",
            round((col("active_users") / col("first_month_users")) * 100, 2),
        )
        .select(
            "signup_month",
            "cohort_month",
            "active_users",
            "revenue_per_user",
            "retention_rate",
        )
    )

    return cohort_df


def compute_inventory_health(
    inventory: DataFrame,
    fact_orders: DataFrame,
    dim_date: DataFrame,
    run_date: str,
    lookback_days: int,
) -> DataFrame:
    """
    Compute inventory health metrics
    """
    logger.info(f"Computing inventory health with {lookback_days} day lookback...")

    # Join fact_orders with dim_date
    orders_with_date = fact_orders.join(
        dim_date, fact_orders.date_id == dim_date.date_id, "inner"
    )

    # Get last N days of sales per product
    start_date = (
        datetime.strptime(run_date, "%Y-%m-%d") - timedelta(days=lookback_days)
    ).strftime("%Y-%m-%d")

    daily_sales = (
        orders_with_date.filter(
            (col("date") >= start_date)
            & (col("date") <= run_date)
            & (col("status") != "returned")
        )
        .groupBy("product_id", "date")
        .agg(spark_sum("quantity").alias("daily_quantity"))
    )

    # Rest of the function remains the same...

    # Calculate average daily sales
    avg_sales = (
        daily_sales.groupBy("product_id")
        .agg(spark_avg("daily_quantity").alias("avg_daily_sales"))
        .fillna(0)
    )

    # Join with inventory
    inventory_health = (
        inventory.join(avg_sales, "product_id", "left")
        .fillna(0)
        .withColumn(
            "days_of_stock_remaining",
            when(
                col("avg_daily_sales") > 0,
                (col("quantity_on_hand") / col("avg_daily_sales")).cast(IntegerType()),
            ).otherwise(lit(None)),
        )
        .withColumn(
            "reorder_flag",
            when(col("days_of_stock_remaining") < 7, True).otherwise(False),
        )
        .withColumn("date", lit(run_date).cast("date"))
        .select(
            "product_id",
            "warehouse_id",
            "date",
            "quantity_on_hand",
            "avg_daily_sales",
            "days_of_stock_remaining",
            "reorder_flag",
        )
    )

    return inventory_health


# ============================================
# MAIN EXECUTION
# ============================================


def _run_aggregates(args, metrics):
    start_time = datetime.now()
    run_date = args.run_date
    batch_id = f"agg_{run_date.replace('-', '')}"
    run_id = os.getenv("PIPELINE_RUN_ID") or batch_id

    logger.info(f"Computing aggregates for {run_date}")

    config = get_config(args.env)
    spark = create_spark_session(batch_id)

    try:
        jdbc_url = get_jdbc_url(config)
        jdbc_props = get_jdbc_properties(config)

        # Read fact_orders
        fact_orders = spark.read.jdbc(
            url=jdbc_url, table="warehouse.fact_orders", properties=jdbc_props
        )

        # Read dim_users
        dim_users = spark.read.jdbc(
            url=jdbc_url, table="warehouse.dim_users", properties=jdbc_props
        )

        # Read dim_date
        dim_date = spark.read.jdbc(
            url=jdbc_url, table="warehouse.dim_date", properties=jdbc_props
        )

        # Read inventory from MySQL staging
        # Host/port env-driven with Docker defaults (same pattern as transform_orders)
        mysql_url = (
            f"jdbc:mysql://{os.getenv('MYSQL_HOST', 'mysql')}:{os.getenv('MYSQL_PORT', '3306')}"
            f"/ecommerce_staging?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"
        )
        mysql_props = {
            "driver": "com.mysql.cj.jdbc.Driver",
            "user": _required_env("MYSQL_USER"),
            "password": _required_env("MYSQL_PASSWORD"),
        }

        clickstream = spark.read.jdbc(
            url=mysql_url, table="stg_raw_clickstream", properties=mysql_props
        ).filter(col("is_processed") == 1)

        inventory = spark.read.jdbc(
            url=mysql_url, table="stg_raw_inventory", properties=mysql_props
        )

        # Rows read across all sources feed the metrics row
        metrics.rows_read = (
            fact_orders.count()
            + dim_users.count()
            + dim_date.count()
            + clickstream.count()
            + inventory.count()
        )

        # 1. Daily Revenue
        revenue_df = compute_daily_revenue(fact_orders, dim_date, run_date)
        revenue_count = revenue_df.count()
        if revenue_count > 0:
            revenue_df.write.mode("overwrite").option("truncate", "true").jdbc(
                url=jdbc_url, table="warehouse.agg_daily_revenue", properties=jdbc_props
            )
            logger.info(f"Updated daily revenue for {run_date}")

        # 2. Product Performance
        performance_df = compute_product_performance(
            fact_orders, dim_date, clickstream, run_date
        )
        performance_count = performance_df.count()
        if performance_count > 0:
            performance_df.write.mode("overwrite").option("truncate", "true").jdbc(
                url=jdbc_url,
                table="warehouse.agg_product_performance",
                properties=jdbc_props,
            )
            logger.info(f"Updated product performance for {run_date}")

        # 3. User Cohorts
        cohorts_df = compute_user_cohorts(dim_users, fact_orders, dim_date)
        cohorts_count = cohorts_df.count()
        if cohorts_count > 0:
            cohorts_df.write.mode("overwrite").jdbc(
                url=jdbc_url, table="warehouse.agg_user_cohorts", properties=jdbc_props
            )
            logger.info("Updated user cohorts")

        # 4. Inventory Health
        health_df = compute_inventory_health(
            inventory, fact_orders, dim_date, run_date, args.lookback_days
        )
        health_count = health_df.count()
        if health_count > 0:
            health_df.write.mode("overwrite").jdbc(
                url=jdbc_url,
                table="warehouse.agg_inventory_health",
                properties=jdbc_props,
            )
            logger.info(f"Updated inventory health for {run_date}")

        aggregates = [
            ("warehouse.agg_daily_revenue", revenue_count),
            ("warehouse.agg_product_performance", performance_count),
            ("warehouse.agg_user_cohorts", cohorts_count),
            ("warehouse.agg_inventory_health", health_count),
        ]
        metrics.rows_transformed = sum(count for _, count in aggregates)
        metrics.rows_loaded = metrics.rows_transformed

        # Freshness: how old the newest order in the warehouse is. Skipped if empty.
        latest_order = fact_orders.agg(spark_max("created_at")).collect()[0][0]
        if latest_order is not None:
            now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
            metrics.data_freshness_lag_minutes = max(
                0.0, (now_utc - latest_order).total_seconds() / 60
            )

        # Lineage: one edge per aggregate table written. Each is isolated so one
        # failure does not block the others.
        for target_table, rows_written in aggregates:
            if rows_written > 0:
                try:
                    record_lineage(
                        run_id=run_id,
                        source_system="postgres_warehouse",
                        source_table="warehouse.fact_orders",
                        target_table=target_table,
                        rows_written=rows_written,
                        transformation_applied="daily_aggregation",
                    )
                except Exception as lineage_error:
                    logger.warning(
                        "Lineage record failed, continuing",
                        extra={
                            "target_table": target_table,
                            "error": str(lineage_error),
                        },
                    )

        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(
            "Aggregates completed",
            extra={
                "aggregates_computed": sum(1 for _, count in aggregates if count > 0),
                "pipeline_run_id": run_id,
                "run_date": run_date,
                "lookback_days": args.lookback_days,
                "rows_loaded": metrics.rows_loaded,
                "elapsed_seconds": round(elapsed, 2),
            },
        )

        return {"status": "success", "run_date": run_date, "processing_time": elapsed}

    except Exception as e:
        logger.error(f"Aggregates failed: {e}")
        import traceback

        traceback.print_exc()
        return {"status": f"failed: {str(e)}"}
    finally:
        spark.stop()


def run_aggregates(args):
    """
    Compute the aggregates inside a metrics context: one pipeline_metrics row per
    run, plus CloudWatch when configured. Metrics failures never fail the job.
    """
    batch_id = f"agg_{args.run_date.replace('-', '')}"
    run_id = os.getenv("PIPELINE_RUN_ID") or batch_id
    with track_metrics(pipeline_name="compute_aggregates", run_id=run_id) as metrics:
        result = _run_aggregates(args, metrics)
        metrics.status = (
            "failed" if result["status"].startswith("failed") else "success"
        )
        return result


if __name__ == "__main__":
    args = parse_arguments()
    result = run_aggregates(args)

    print("\n" + "=" * 60)
    print("AGGREGATE SUMMARY")
    print("=" * 60)
    print(f"Run Date:       {args.run_date}")
    print(f"Lookback Days:  {args.lookback_days}")
    print(f"Status:         {result.get('status', 'unknown')}")
    print(f"Processing Time: {result.get('processing_time', 0):.2f} seconds")
    print("=" * 60)
