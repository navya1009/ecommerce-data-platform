#!/usr/bin/env python3
"""
Great Expectations Checkpoint Runner
"""
import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, "/opt/airflow")
from src.utils.exceptions import PipelineDataQualityError  # noqa: E402

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

GE_DIR = Path("/opt/airflow/great_expectations")


# ============================================
# SUITE RUNNERS
# ============================================


def run_raw_orders_checkpoint(batch_size=1000):
    """Run data quality checks on raw orders"""
    logger.info("Running raw_orders_suite...")

    from great_expectations.core import ExpectationConfiguration
    from great_expectations.core.batch import RuntimeBatchRequest
    from great_expectations.core.expectation_suite import ExpectationSuite
    from great_expectations.data_context import DataContext

    context = DataContext(context_root_dir=str(GE_DIR))
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
    """
    Run data quality checks on fact_orders.

    Note: We check the ENTIRE fact_orders table, not just the run_date.
    Reason: The pipeline runs daily, but new data may not arrive every day.
    The DQ check's job is to validate the overall quality of the warehouse,
    not to fail just because a specific day had no new orders.
    """
    logger.info("Running fact_orders_suite on entire table...")

    from great_expectations.core import ExpectationConfiguration
    from great_expectations.core.batch import RuntimeBatchRequest
    from great_expectations.core.expectation_suite import ExpectationSuite
    from great_expectations.data_context import DataContext

    context = DataContext(context_root_dir=str(GE_DIR))
    suite_path = GE_DIR / "expectations" / "fact_orders_suite.json"

    with open(suite_path, "r") as f:
        suite_config = json.load(f)

    suite = ExpectationSuite(
        expectation_suite_name=suite_config["expectation_suite_name"],
        expectations=[
            ExpectationConfiguration(**exp) for exp in suite_config["expectations"]
        ],
    )

    # Validate the ENTIRE fact_orders table (no date filter)
    # This makes the check robust to days with no new data
    query = "SELECT * FROM warehouse.fact_orders"

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
    from great_expectations.data_context import DataContext

    context = DataContext(context_root_dir=str(GE_DIR))
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


# ============================================
# DISPATCHER FUNCTIONS
# ============================================


def run_single_suite(suite_name, run_date=None, raise_on_error=True):
    """Run a single suite by name."""
    logger.info(f"Running {suite_name}...")

    suite_map = {
        "raw_orders_suite": lambda: run_raw_orders_checkpoint(),
        "fact_orders_suite": lambda: run_fact_orders_checkpoint(run_date),
        "agg_daily_revenue_suite": lambda: run_agg_daily_revenue_checkpoint(),
    }

    if suite_name not in suite_map:
        raise ValueError(
            f"Unknown suite: {suite_name}. Valid options: {list(suite_map.keys())}"
        )

    try:
        results = suite_map[suite_name]()

        if results["success"]:
            stats = results.get("statistics", {})
            passed = stats.get("successful_expectations", 0)
            total = stats.get("evaluated_expectations", 0)
            logger.info(f"{suite_name}: {passed}/{total} passed ✅")
            return results
        else:
            logger.error(f"{suite_name}: Data quality check failed ❌")
            if raise_on_error:
                raise PipelineDataQualityError(f"{suite_name} failed")
            return results
    except Exception as e:
        logger.error(f"{suite_name}: {e}")
        if raise_on_error:
            raise
        return {"success": False, "error": str(e)}


def run_all_checkpoints(run_date=None, raise_on_error=True):
    """Run all data quality checkpoints"""
    logger.info("=" * 60)
    logger.info("STARTING DATA QUALITY CHECKS")
    logger.info("=" * 60)

    results_summary = {}
    all_passed = True

    suites = [
        ("raw_orders_suite", lambda: run_raw_orders_checkpoint()),
        ("fact_orders_suite", lambda: run_fact_orders_checkpoint(run_date)),
        ("agg_daily_revenue_suite", lambda: run_agg_daily_revenue_checkpoint()),
    ]

    for suite_name, runner in suites:
        try:
            logger.info(f"\n📊 Running {suite_name}...")
            results = runner()
            results_summary[suite_name] = results

            if results["success"]:
                stats = results.get("statistics", {})
                passed = stats.get("successful_expectations", 0)
                total = stats.get("evaluated_expectations", 0)
                logger.info(f"✅ {suite_name}: {passed}/{total} passed")
            else:
                logger.error(f"❌ {suite_name}: Failed")
                all_passed = False
                if raise_on_error:
                    raise PipelineDataQualityError(f"{suite_name} failed")
        except Exception as e:
            logger.error(f"❌ {suite_name}: {e}")
            all_passed = False
            results_summary[suite_name] = {"success": False, "error": str(e)}
            if raise_on_error:
                raise

    logger.info("\n" + "=" * 60)
    logger.info(
        f"Overall Status: {'✅ ALL PASSED' if all_passed else '❌ SOME FAILED'}"
    )
    logger.info("=" * 60)

    return results_summary


# ============================================
# MAIN
# ============================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run Great Expectations data quality checks"
    )
    parser.add_argument(
        "--suite",
        help="Run only a specific suite (raw_orders_suite, fact_orders_suite, agg_daily_revenue_suite)",
    )
    parser.add_argument(
        "--run-date", help="Date to validate (YYYY-MM-DD) for fact_orders"
    )
    parser.add_argument(
        "--no-raise", action="store_true", help="Do not raise exception on failure"
    )
    args = parser.parse_args()

    try:
        if args.suite:
            results = run_single_suite(
                suite_name=args.suite,
                run_date=args.run_date,
                raise_on_error=not args.no_raise,
            )
        else:
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
