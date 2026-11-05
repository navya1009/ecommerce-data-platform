"""
Self-healing logic for the e-commerce pipeline.

Detects consecutive pipeline failures and automatically triggers a full reload.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List

import mysql.connector
from airflow.utils.session import provide_session

logger = logging.getLogger(__name__)

# ============================================
# CONSTANTS
# ============================================

CONSECUTIVE_FAILURES_THRESHOLD = 3
PIPELINE_NAME = "ecommerce_daily_pipeline"


# ============================================
# DATABASE HELPERS
# ============================================


def _required_env(name: str) -> str:
    """Credentials have no built-in default. Fail with a clear message if unset."""
    import os

    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"{name} must be set in the environment (no default is built in)"
        )
    return value


def get_mysql_connection():
    """Get connection to MySQL staging database."""
    import os

    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "mysql"),
        port=int(os.getenv("MYSQL_PORT", 3306)),
        user=os.getenv("MYSQL_USER", "root"),
        password=_required_env("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE", "ecommerce_staging"),
    )


def get_consecutive_failed_dates() -> List[str]:
    """
    Check the pipeline_runs table for consecutive failures.

    Returns:
        List of date strings (YYYY-MM-DD) with consecutive failures
    """
    conn = get_mysql_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        # Query recent runs ordered by date
        cursor.execute(
            """
            SELECT DATE(start_time) as run_date, status
            FROM pipeline_runs
            WHERE pipeline_name = %s
            ORDER BY start_time DESC
            LIMIT 10
        """,
            (PIPELINE_NAME,),
        )

        runs = cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

    # Find consecutive failed dates
    failed_dates = []
    consecutive_count = 0

    for run in runs:
        if run["status"] == "failed":
            consecutive_count += 1
            failed_dates.append(
                run["run_date"].isoformat()
                if hasattr(run["run_date"], "isoformat")
                else str(run["run_date"])
            )
        else:
            # Success breaks the streak
            break

    logger.info(f"Consecutive failures: {consecutive_count} ({failed_dates})")

    if consecutive_count >= CONSECUTIVE_FAILURES_THRESHOLD:
        return failed_dates

    return []


# ============================================
# TRIGGER RELOAD
# ============================================


def trigger_backfill(dates: List[str]) -> Dict[str, Any]:
    """
    Trigger a full reload (backfill) for the given dates.

    Args:
        dates: List of date strings (YYYY-MM-DD)

    Returns:
        Result dict with triggered run IDs
    """
    from airflow.api.common.trigger_dag import trigger_dag

    triggered_runs = []

    for date_str in dates:
        try:
            # Parse date and use as logical_date
            logical_date = datetime.strptime(date_str, "%Y-%m-%d")

            # Trigger DAG with backfill flag
            dag_run = trigger_dag(
                dag_id=PIPELINE_NAME,
                conf={
                    "backfill": True,
                    "reason": "self_healing_consecutive_failures",
                    "original_date": date_str,
                },
                execution_date=logical_date,
                replace_microseconds=False,
            )

            triggered_runs.append(
                {
                    "date": date_str,
                    "dag_run_id": dag_run.run_id if dag_run else None,
                }
            )
            logger.info(
                f"Triggered backfill for {date_str}: {dag_run.run_id if dag_run else 'unknown'}"
            )

        except Exception as e:
            logger.error(f"Failed to trigger backfill for {date_str}: {e}")
            triggered_runs.append(
                {
                    "date": date_str,
                    "error": str(e),
                }
            )

    return {"triggered_runs": triggered_runs}


# ============================================
# SLACK NOTIFICATION
# ============================================


def notify_self_healing(dates: List[str], triggered_runs: Dict) -> None:
    """Send Slack notification about self-healing action."""
    from callbacks import send_slack_message

    date_list = "\n".join([f"  • {d}" for d in dates])
    run_list = "\n".join(
        [
            f"  • {r.get('date')}: `{r.get('dag_run_id', r.get('error'))}`"
            for r in triggered_runs.get("triggered_runs", [])
        ]
    )

    message = (
        f":robot_face: *Self-Healing Triggered*\n"
        f"*Reason:* {CONSECUTIVE_FAILURES_THRESHOLD} consecutive pipeline failures\n"
        f"*Failed dates:*\n{date_list}\n"
        f"*Backfill triggered for:*\n{run_list}\n\n"
        f"Investigate why these days failed before the auto-recovery completes."
    )

    send_slack_message(message)


# ============================================
# MAIN SELF-HEALING FUNCTION
# ============================================


@provide_session
def check_and_heal(session=None) -> Dict[str, Any]:
    """
    Main self-healing function called as an Airflow task.

    Steps:
    1. Check for consecutive failures in pipeline_runs
    2. If threshold reached, trigger backfill for failed dates
    3. Notify via Slack
    """
    logger.info("Starting self-healing check...")

    # Step 1: Check for consecutive failures
    failed_dates = get_consecutive_failed_dates()

    if not failed_dates:
        logger.info(
            f"No self-healing needed. Fewer than {CONSECUTIVE_FAILURES_THRESHOLD} "
            f"consecutive failures detected."
        )
        return {
            "self_healing_triggered": False,
            "reason": "no_consecutive_failures",
            "consecutive_failures": 0,
        }

    logger.warning(f"Consecutive failures threshold reached: {len(failed_dates)} dates")

    # Step 2: Trigger backfill
    triggered = trigger_backfill(failed_dates)

    # Step 3: Notify
    notify_self_healing(failed_dates, triggered)

    return {
        "self_healing_triggered": True,
        "consecutive_failures": len(failed_dates),
        "failed_dates": failed_dates,
        "triggered_runs": triggered,
    }
