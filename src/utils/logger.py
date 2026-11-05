"""
Structured logging for the pipeline.

get_logger(name) is an additive API. It does not touch
ingestion_helpers.setup_logging, which existing code keeps using.

Every record carries pipeline context (pipeline_run_id, dag_id, task_id,
environment, git_commit_sha). Console output is colored text locally and JSON
elsewhere. In dev/staging/prod with AWS_REGION set, records are also shipped
to CloudWatch Logs. CloudWatch problems are logged and never raised.
"""

import logging
import os
import subprocess
import sys
from functools import lru_cache
from pathlib import Path

import colorlog
import watchtower
from pythonjsonlogger import jsonlogger

CONTEXT_FIELDS = (
    "pipeline_run_id",
    "dag_id",
    "task_id",
    "environment",
    "git_commit_sha",
)

LEVEL_BY_ENVIRONMENT = {
    "local": logging.DEBUG,
    "dev": logging.INFO,
    "staging": logging.WARNING,
    "prod": logging.ERROR,
}
CLOUDWATCH_ENVIRONMENTS = {"dev", "staging", "prod"}

# Marks handlers this module created, so reconfiguring replaces only them.
_OWNED_HANDLER_FLAG = "_ecommerce_observability"
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def current_environment() -> str:
    return os.getenv("ENVIRONMENT", "local").lower()


@lru_cache(maxsize=1)
def _git_head_sha() -> str:
    """`git rev-parse HEAD`, run once per process. 'unknown' if git is unavailable."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            cwd=_PROJECT_ROOT,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        pass
    return "unknown"


def git_commit_sha() -> str:
    return os.getenv("GIT_COMMIT_SHA") or _git_head_sha()


class PipelineContextFilter(logging.Filter):
    """Adds the pipeline context fields to every record it sees."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.pipeline_run_id = os.getenv("PIPELINE_RUN_ID")
        record.dag_id = os.getenv("AIRFLOW_CTX_DAG_ID")
        record.task_id = os.getenv("AIRFLOW_CTX_TASK_ID")
        record.environment = current_environment()
        record.git_commit_sha = git_commit_sha()
        return True


def _json_formatter() -> jsonlogger.JsonFormatter:
    return jsonlogger.JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s")


def _console_handler(environment: str, level: int) -> logging.Handler:
    if environment == "local":
        handler = colorlog.StreamHandler(sys.stderr)
        handler.setFormatter(
            colorlog.ColoredFormatter(
                "%(log_color)s%(asctime)s %(levelname)-8s %(name)s %(message)s"
                "%(reset)s [run=%(pipeline_run_id)s]",
                log_colors={
                    "DEBUG": "cyan",
                    "INFO": "green",
                    "WARNING": "yellow",
                    "ERROR": "red",
                    "CRITICAL": "bold_red",
                },
            )
        )
    else:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(_json_formatter())
    handler.setLevel(level)
    setattr(handler, _OWNED_HANDLER_FLAG, True)
    return handler


def _cloudwatch_handler(environment: str, level: int):
    """
    Build a watchtower handler, or return None. Any failure is reported on
    stderr and swallowed: CloudWatch must never break the pipeline.
    """
    region = os.getenv("AWS_REGION")
    if environment not in CLOUDWATCH_ENVIRONMENTS or not region:
        return None
    try:
        import boto3

        client_kwargs = {"region_name": region}
        endpoint_url = os.getenv("AWS_ENDPOINT_URL")  # fakecloud locally
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url

        handler = watchtower.CloudWatchLogHandler(
            log_group_name=f"/aws/ecommerce-data-platform/{environment}",
            log_stream_name=os.getenv("PIPELINE_RUN_ID") or "local",
            boto3_client=boto3.client("logs", **client_kwargs),
        )
        handler.setFormatter(_json_formatter())
        handler.setLevel(level)
        setattr(handler, _OWNED_HANDLER_FLAG, True)
        return handler
    except Exception as exc:  # noqa: BLE001 - CloudWatch must never fail the pipeline
        print(f"[logger] CloudWatch handler disabled: {exc}", file=sys.stderr)
        return None


def get_logger(name: str) -> logging.Logger:
    """
    Return a configured logger for `name`. Safe to call repeatedly: handlers
    from an earlier call are replaced, not duplicated.
    """
    environment = current_environment()
    level = LEVEL_BY_ENVIRONMENT.get(environment, logging.DEBUG)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False  # avoid double output through the root logger

    for handler in list(logger.handlers):
        if getattr(handler, _OWNED_HANDLER_FLAG, False):
            logger.removeHandler(handler)
            handler.close()
    if not any(isinstance(f, PipelineContextFilter) for f in logger.filters):
        logger.addFilter(PipelineContextFilter())

    logger.addHandler(_console_handler(environment, level))
    cloudwatch = _cloudwatch_handler(environment, level)
    if cloudwatch is not None:
        logger.addHandler(cloudwatch)
    return logger
