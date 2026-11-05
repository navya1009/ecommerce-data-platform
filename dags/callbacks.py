"""
Airflow callbacks for alerting and monitoring.

This module provides:
- on_failure_callback: Slack alert when a task fails
- on_retry_callback: Log warning when a task is retried
- on_success_callback: Slack summary when a task succeeds
- sla_miss_callback: Alert when a task exceeds its SLA
"""

import logging
import os
from typing import Any, Optional

import requests
from airflow.models import DagRun, TaskInstance
from airflow.utils.context import Context

logger = logging.getLogger(__name__)

# ============================================
# SLACK CONFIGURATION
# ============================================


# Read from Airflow Variable first, fall back to env var
def get_slack_webhook_url() -> Optional[str]:
    """Get Slack webhook URL from Airflow Variable or env var."""
    try:
        from airflow.models import Variable

        url = Variable.get("slack_webhook_url", default_var=None)
        if url:
            return url
    except Exception:
        pass
    return os.getenv("SLACK_WEBHOOK_URL")


def send_slack_message(message: str, blocks: list = None) -> bool:
    """
    Send a message to Slack via webhook.

    Args:
        message: Fallback plain text message
        blocks: Optional Slack Block Kit blocks for rich formatting

    Returns:
        True if sent successfully, False otherwise
    """
    webhook_url = get_slack_webhook_url()
    if not webhook_url:
        logger.warning("No Slack webhook URL configured, skipping notification")
        return False

    payload = {"text": message}
    if blocks:
        payload["blocks"] = blocks

    try:
        response = requests.post(
            webhook_url,
            json=payload,
            timeout=10,
        )
        if response.status_code == 200:
            logger.info("Slack notification sent")
            return True
        else:
            logger.error(
                f"Slack notification failed: {response.status_code} - {response.text}"
            )
            return False
    except Exception as e:
        logger.error(f"Failed to send Slack notification: {e}")
        return False


# ============================================
# CALLBACK: ON FAILURE
# ============================================


def on_failure_callback(context: Context) -> None:
    """
    Called when a task fails (after all retries exhausted).

    Sends a Slack alert with:
    - DAG name
    - Task name
    - Execution date
    - Log URL
    - Exception message
    """
    task_instance: TaskInstance = context.get("task_instance")
    exception: Optional[Exception] = context.get("exception")

    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    execution_date = context.get("execution_date")
    try_number = task_instance.try_number
    log_url = task_instance.log_url

    # Get exception message
    error_message = "Unknown error"
    if exception:
        error_message = f"{type(exception).__name__}: {str(exception)}"
    elif task_instance.state == "upstream_failed":
        error_message = "Upstream task failed"

    logger.error(
        f"TASK FAILED: {dag_id}.{task_id} at {execution_date} "
        f"(try {try_number}): {error_message}"
    )

    # Slack message
    slack_message = (
        f":red_circle: *Task Failed*\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Task:* `{task_id}`\n"
        f"*Execution Date:* `{execution_date}`\n"
        f"*Try Number:* `{try_number}`\n"
        f"*Error:* ```{error_message}```\n"
        f"*Logs:* <{log_url}|View logs>"
    )

    send_slack_message(slack_message)


# ============================================
# CALLBACK: ON RETRY
# ============================================


def on_retry_callback(context: Context) -> None:
    """
    Called when a task is about to be retried.

    Logs:
    - Task name
    - Current try number
    - Next retry time
    """
    task_instance: TaskInstance = context.get("task_instance")
    exception: Optional[Exception] = context.get("exception")

    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    try_number = task_instance.try_number
    max_tries = task_instance.max_tries

    error_message = "Unknown error"
    if exception:
        error_message = f"{type(exception).__name__}: {str(exception)}"

    logger.warning(
        f"TASK RETRY: {dag_id}.{task_id} "
        f"(attempt {try_number}/{max_tries}): {error_message}"
    )

    # Optional: Send a lightweight Slack message for retries (can be noisy)
    # Uncomment to enable:
    # slack_message = (
    #     f":warning: *Task Retry*\n"
    #     f"*DAG:* `{dag_id}`\n"
    #     f"*Task:* `{task_id}`\n"
    #     f"*Attempt:* `{try_number}/{max_tries}`\n"
    #     f"*Error:* {error_message[:200]}"
    # )
    # send_slack_message(slack_message)


# ============================================
# CALLBACK: ON SUCCESS
# ============================================


def on_success_callback(context: Context) -> None:
    """
    Called when a task succeeds.

    Sends a lightweight Slack summary with key metrics pulled from XCom.
    Only sends for the FINAL task in the DAG to avoid spamming.
    """
    task_instance: TaskInstance = context.get("task_instance")
    dag_run: DagRun = context.get("dag_run")

    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    execution_date = context.get("execution_date")

    # Only notify for the "end" task or specified final tasks
    FINAL_TASKS = ["end", "notify_success"]
    if task_id not in FINAL_TASKS:
        logger.debug(f"Task {task_id} succeeded (no Slack notification)")
        return

    # Pull metrics from XCom (if available)
    metrics = {}
    try:
        ti = context.get("ti")
        if ti:
            for key in [
                "rows_loaded",
                "rows_read",
                "rows_quarantined",
                "processing_time",
            ]:
                val = ti.xcom_pull(task_ids="update_pipeline_manifest", key=key)
                if val is not None:
                    metrics[key] = val
    except Exception as e:
        logger.debug(f"Could not pull XCom metrics: {e}")

    logger.info(f"FINAL TASK SUCCESS: {dag_id}.{task_id} at {execution_date}")

    # Build success message
    metrics_text = ""
    if metrics:
        metrics_text = "\n*Metrics:*\n"
        for k, v in metrics.items():
            metrics_text += f"  • {k}: `{v}`\n"

    duration = ""
    if dag_run and dag_run.start_date and dag_run.end_date:
        duration_sec = (dag_run.end_date - dag_run.start_date).total_seconds()
        duration = f"\n*Duration:* `{duration_sec:.1f} seconds`"

    slack_message = (
        f":large_green_circle: *Pipeline Succeeded*\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Execution Date:* `{execution_date}`"
        f"{duration}"
        f"{metrics_text}"
    )

    send_slack_message(slack_message)


# ============================================
# CALLBACK: SLA MISS
# ============================================


def sla_miss_callback(
    dag: Any, task_list: list, blocking_task_list: list, slas: list, blocking_tis: list
) -> None:
    """
    Called when one or more tasks miss their SLA.

    SLA = Service Level Agreement (max duration for a task).

    Args:
        dag: The DAG that contains the tasks
        task_list: List of tasks that missed SLA
        blocking_task_list: Tasks blocking the SLA-missing tasks
        slas: The SLA objects
        blocking_tis: TaskInstances blocking the SLAs
    """
    dag_id = dag.dag_id if dag else "unknown"

    logger.error(f"SLA MISS: {dag_id} - {len(task_list)} task(s) missed SLA")

    # Build details
    task_details = []
    for sla in slas:
        task_id = sla.task_id if hasattr(sla, "task_id") else "unknown"
        sla_time = sla.timestamp if hasattr(sla, "timestamp") else "unknown"
        task_details.append(f"  • `{task_id}` (SLA deadline: {sla_time})")

    task_text = "\n".join(task_details) if task_details else "  • No details available"

    slack_message = (
        f":warning: *SLA Miss Detected*\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Number of tasks:* `{len(task_list)}`\n"
        f"*Details:*\n{task_text}\n\n"
        f"The pipeline is taking longer than expected. Check for slow source systems or data volume spikes."
    )

    send_slack_message(slack_message)
