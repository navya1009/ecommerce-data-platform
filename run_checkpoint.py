#!/usr/bin/env python3
"""
Great Expectations Checkpoint Runner
"""
import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, "/app")
from src.utils.exceptions import PipelineDataQualityError  # noqa: E402

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

GE_DIR = Path("/app/great_expectations")


def get_ge_context():
    """Initialize Great Expectations DataContext"""
    from great_expectations.data_context import DataContext

    return DataContext(context_root_dir=str(GE_DIR))


def run_raw_orders_checkpoint(batch_size=1000):
    """Run data quality checks on raw orders"""
    logger.info("Running raw_orders_suite...")

    from great_expectations.core import ExpectationConfiguration
    from great_expectations.core.batch import RuntimeBatchRequest
    from great_expectations.core.expectation_suite import ExpectationSuite

    context = get_ge_context()
    suite_path = GE_DIR / "expectations" / "raw_orders_suite.json"

    with open(suite_path, "r") as f:
        suite_config = json.load(f)

    suite = ExpectationSuite(
        expectation_suite_name=suite_config["expectation_suite_name"],
        expectations=[
            ExpectationConfiguration(**exp) for exp in suite_config["expectations"]
        ],
    )

    batch_request = RuntimeBatchRequest(
        datasource_name="mysql_datasource",
        data_connector_name="default_runtime_data_connector",
        data_asset_name="stg_raw_orders",
        runtime_parameters={
            "query": f"SELECT * FROM stg_raw_orders LIMIT {batch_size}"
        },
        batch_identifiers={"default_identifier_name": "default_identifier"},
    )

    validator = context.get_validator(
        batch_request=batch_request, expectation_suite=suite
    )
    results = validator.validate()

    return results


def run_fact_orders_checkpoint(run_date=None):
    """Run data quality checks on fact_orders"""
    logger.info(f"Running fact_orders_suite for {run_date or 'all data'}...")

    from great_expectations.core import ExpectationConfiguration
    from great_expectations.core.batch import RuntimeBatchRequest
    from great_expectations.core.expectation_suite import ExpectationSuite

    context = get_ge_context()
    suite_path = GE_DIR / "expectations" / "fact_orders_suite.json"

    with open(suite_path, "r") as f:
        suite_config = json.load(f)

    suite = ExpectationSuite(
        expectation_suite_name=suite_config["expectation_suite_name"],
        expectations=[
            ExpectationConfiguration(**exp) for exp in suite_config["expectations"]
        ],
    )

    query = "SELECT * FROM warehouse.fact_orders"
    if run_date:
        query = f"""
            SELECT fo.*
            FROM warehouse.fact_orders fo
            JOIN warehouse.dim_date dd ON fo.date_id = dd.date_id
            WHERE dd.date = '{run_date}'
        """

    batch_request = RuntimeBatchRequest(
        datasource_name="postgres_datasource",
        data_connector_name="default_runtime_data_connector",
        data_asset_name="fact_orders",
        runtime_parameters={"query": query},
        batch_identifiers={"default_identifier_name": "default_identifier"},
    )

    validator = context.get_validator(
        batch_request=batch_request, expectation_suite=suite
    )
    results = validator.validate()

    return results


def run_agg_daily_revenue_checkpoint():
    """Run data quality checks on agg_daily_revenue"""
    logger.info("Running agg_daily_revenue_suite...")

    from great_expectations.core import ExpectationConfiguration
    from great_expectations.core.batch import RuntimeBatchRequest
    from great_expectations.core.expectation_suite import ExpectationSuite

    context = get_ge_context()
    suite_path = GE_DIR / "expectations" / "agg_daily_revenue_suite.json"

    with open(suite_path, "r") as f:
        suite_config = json.load(f)

    suite = ExpectationSuite(
        expectation_suite_name=suite_config["expectation_suite_name"],
        expectations=[
            ExpectationConfiguration(**exp) for exp in suite_config["expectations"]
        ],
    )

    batch_request = RuntimeBatchRequest(
        datasource_name="postgres_datasource",
        data_connector_name="default_runtime_data_connector",
        data_asset_name="agg_daily_revenue",
        runtime_parameters={"query": "SELECT * FROM warehouse.agg_daily_revenue"},
        batch_identifiers={"default_identifier_name": "default_identifier"},
    )

    validator = context.get_validator(
        batch_request=batch_request, expectation_suite=suite
    )
    results = validator.validate()

    return results


def run_all_checkpoints(run_date=None, raise_on_error=True):
    """Run all data quality checkpoints"""
    logger.info("=" * 60)
    logger.info("STARTING DATA QUALITY CHECKS")
    logger.info("=" * 60)

    results_summary = {}
    all_passed = True

    # Run raw orders checkpoint
    try:
        logger.info("\n Running raw_orders_suite...")
        results = run_raw_orders_checkpoint()
        results_summary["raw_orders_suite"] = results

        if results["success"]:
            stats = results.get("statistics", {})
            passed = stats.get("successful_expectations", 0)
            total = stats.get("evaluated_expectations", 0)
            logger.info(f"raw_orders_suite: {passed}/{total} passed")
        else:
            logger.error("raw_orders_suite: Data quality check failed")
            all_passed = False
            if raise_on_error:
                raise PipelineDataQualityError("raw_orders_suite failed")
    except Exception as e:
        logger.error(f"raw_orders_suite: {e}")
        all_passed = False
        if raise_on_error:
            raise

    # Run fact orders checkpoint
    try:
        logger.info("\nRunning fact_orders_suite...")
        results = run_fact_orders_checkpoint(run_date)
        results_summary["fact_orders_suite"] = results

        if results["success"]:
            stats = results.get("statistics", {})
            passed = stats.get("successful_expectations", 0)
            total = stats.get("evaluated_expectations", 0)
            logger.info(f"fact_orders_suite: {passed}/{total} passed")
        else:
            logger.error("fact_orders_suite: Data quality check failed")
            all_passed = False
            if raise_on_error:
                raise PipelineDataQualityError("fact_orders_suite failed")
    except Exception as e:
        logger.error(f"fact_orders_suite: {e}")
        all_passed = False
        if raise_on_error:
            raise

    # Run agg daily revenue checkpoint
    try:
        logger.info("\nRunning agg_daily_revenue_suite...")
        results = run_agg_daily_revenue_checkpoint()
        results_summary["agg_daily_revenue_suite"] = results

        if results["success"]:
            stats = results.get("statistics", {})
            passed = stats.get("successful_expectations", 0)
            total = stats.get("evaluated_expectations", 0)
            logger.info(f"agg_daily_revenue_suite: {passed}/{total} passed")
        else:
            logger.error("agg_daily_revenue_suite: Data quality check failed")
            all_passed = False
            if raise_on_error:
                raise PipelineDataQualityError("agg_daily_revenue_suite failed")
    except Exception as e:
        logger.error(f"agg_daily_revenue_suite: {e}")
        all_passed = False
        if raise_on_error:
            raise

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("DATA QUALITY CHECKS COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Overall Status: {'ALL PASSED' if all_passed else 'SOME FAILED'}")

    return results_summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run Great Expectations data quality checks"
    )
    parser.add_argument(
        "--run-date", help="Date to validate (YYYY-MM-DD) for fact_orders"
    )
    parser.add_argument(
        "--no-raise", action="store_true", help="Do not raise exception on failure"
    )
    args = parser.parse_args()

    try:
        results = run_all_checkpoints(
            run_date=args.run_date, raise_on_error=not args.no_raise
        )
        sys.exit(0)
    except PipelineDataQualityError as e:
        print(f"\nData Quality Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
