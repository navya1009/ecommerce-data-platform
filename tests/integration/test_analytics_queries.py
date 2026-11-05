# tests/integration/test_analytics_queries.py
# -----------------------------------------------------------------------------
# Phase 10.1: the SQL analytics queries run against the test warehouse, and a few
# of their results are checked for correctness on seeded history.
# Uses docker_services / pg_conn / seeded_analytics_data from conftest.py.
# -----------------------------------------------------------------------------
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).parent.parent.parent
RUNNER = PROJECT_ROOT / "analytics" / "run_all_queries.py"
QUERIES_DIR = PROJECT_ROOT / "analytics" / "queries"
SQL_FILES = [
    "revenue_analytics.sql",
    "funnel_analysis.sql",
    "inventory_alerts.sql",
    "customer_analytics.sql",
]
EXPECTED_QUERY_COUNT = 13

ROW_COUNT = re.compile(r":: (\d+) rows ::")


def run_runner(*args: str) -> subprocess.CompletedProcess:
    """Run analytics/run_all_queries.py against the test DB (env set by conftest)."""
    return subprocess.run(
        [sys.executable, str(RUNNER), *args],
        capture_output=True,
        text=True,
        timeout=300,
        cwd=PROJECT_ROOT,
        env=os.environ.copy(),
    )


def query_result(conn, sql_file: str, name_prefix: str) -> list:
    """Run one named query from a file and return its rows as dicts."""
    sys.path.insert(0, str(PROJECT_ROOT))
    from analytics.run_all_queries import load_queries

    queries = dict(load_queries(QUERIES_DIR / sql_file))
    name = next(n for n in queries if n.startswith(name_prefix))
    with conn.cursor() as cur:
        cur.execute(queries[name])
        columns = [col.name for col in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


# =============================================================================
# Every SQL file runs without failure (one test per file)
# =============================================================================
@pytest.mark.parametrize("sql_file", SQL_FILES)
def test_queries_in_file_run_without_failure(docker_services, sql_file):
    result = run_runner("--file", sql_file)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "[FAIL]" not in result.stdout, result.stdout

    pass_lines = [
        line for line in result.stdout.splitlines() if line.startswith("[PASS]")
    ]
    assert pass_lines, f"no queries ran from {sql_file}"
    for line in pass_lines:
        match = ROW_COUNT.search(line)
        assert match is not None, f"no row count in: {line}"
        assert int(match.group(1)) >= 0


def test_all_queries_run_and_none_dropped(docker_services):
    """Guards against a query block silently disappearing from its file."""
    result = run_runner()

    assert result.returncode == 0, result.stdout + result.stderr
    assert "[FAIL]" not in result.stdout, result.stdout
    passed = result.stdout.count("[PASS]")
    assert (
        passed == EXPECTED_QUERY_COUNT
    ), f"ran {passed} queries, expected {EXPECTED_QUERY_COUNT}"


# =============================================================================
# Data correctness on seeded history
# =============================================================================
def test_top_products_revenue_sums_to_total(seeded_analytics_data, pg_conn):
    """Query 1.3: shares of the top 20 products cannot add up to more than 100%."""
    rows = query_result(pg_conn, "revenue_analytics.sql", "Query 1.3")

    assert rows, "Query 1.3 returned no rows for the current month"
    share_sum = sum(float(r["pct_of_total_revenue"]) for r in rows)
    # Each share is rounded to 2 dp, so allow the rounding error across 20 rows
    assert share_sum <= 100 + 0.1, f"shares sum to {share_sum}"


def test_cohort_retention_month_0_is_100_pct(seeded_analytics_data, pg_conn):
    """Query 4.1: every customer is retained in their own cohort month."""
    rows = query_result(pg_conn, "customer_analytics.sql", "Query 4.1")

    month_zero = [r for r in rows if r["month_number"] == 0]
    assert month_zero, "Query 4.1 returned no month-0 rows"
    for row in month_zero:
        assert (
            float(row["retention_pct"]) == 100.0
        ), f"cohort {row['cohort_month']} month 0 retention is {row['retention_pct']}"


def test_churned_users_have_last_order_90_to_180_days_ago(
    seeded_analytics_data, pg_conn
):
    """Query 4.3: every churned customer's last order falls in the 90-180 day window."""
    rows = query_result(pg_conn, "customer_analytics.sql", "Query 4.3")

    assert rows, "Query 4.3 returned no churned customers (seed includes 5)"
    for row in rows:
        assert 90 <= row["days_since_last_order"] <= 180, row


# =============================================================================
# Funnel values on seeded clickstream (Phase 10.1 extension)
# =============================================================================
def test_funnel_query_returns_nonzero_rows(seeded_analytics_with_clickstream, pg_conn):
    """Query 2.1: the device funnel has rows once clickstream is loaded."""
    rows = query_result(pg_conn, "funnel_analysis.sql", "Query 2.1")

    assert rows, "Query 2.1 returned no rows: fact_clickstream is still empty"
    assert sum(r["page_views"] for r in rows) > 0


def test_funnel_pct_values_in_valid_range(seeded_analytics_with_clickstream, pg_conn):
    """Query 2.1: every percentage is between 0 and 100, or NULL when undefined."""
    rows = query_result(pg_conn, "funnel_analysis.sql", "Query 2.1")

    assert rows, "Query 2.1 returned no rows"
    pct_columns = [
        "pct_add_to_cart_from_view",
        "pct_purchase_from_cart",
        "pct_purchase_from_view",
    ]
    for row in rows:
        for column in pct_columns:
            value = row[column]
            assert (
                value is None or 0 <= float(value) <= 100
            ), f"{column}={value} in {row}"


def test_session_analysis_has_sessions(seeded_analytics_with_clickstream, pg_conn):
    """Query 2.3: session analysis finds non-bot sessions in the seeded clickstream."""
    rows = query_result(pg_conn, "funnel_analysis.sql", "Query 2.3")

    assert rows, "Query 2.3 returned no rows"
    assert sum(r["total_sessions"] for r in rows) > 0


# =============================================================================
# Inventory values on seeded snapshots (Priority 3)
# =============================================================================
def test_stockout_alerts_return_low_stock_products(seeded_inventory, pg_conn):
    """Query 3.1: products 1..4 (2 days of cover, 1 unit) are alerted, using the latest snapshot."""
    rows = query_result(pg_conn, "inventory_alerts.sql", "Query 3.1")

    assert rows, "Query 3.1 returned no rows for the seeded low-stock products"
    for row in rows:
        assert row["days_of_stock_remaining"] < 7, row
        assert row["current_stock"] > 0, row
    # The older snapshot (9999 units, 999 days) must not be used
    assert all(row["current_stock"] == 1 for row in rows), rows


def test_overstock_alerts_return_high_cover_products(seeded_inventory, pg_conn):
    """Query 3.2: products 5..8 (400 days of cover) are flagged as overstock."""
    rows = query_result(pg_conn, "inventory_alerts.sql", "Query 3.2")

    assert rows, "Query 3.2 returned no rows for the seeded overstock products"
    for row in rows:
        assert row["days_of_stock_remaining"] > 180, row
    assert all(row["current_stock"] == 5000 for row in rows), rows


def test_reorder_suggestions_match_their_formula(seeded_inventory, pg_conn):
    """Query 3.3: each suggestion is below 14 days of stock and orders 30 days of sales."""
    import math

    rows = query_result(pg_conn, "inventory_alerts.sql", "Query 3.3")

    for row in rows:
        avg = float(row["avg_daily_sales"])
        assert avg > 0, row
        assert float(row["current_stock"]) < avg * 14, row
        assert (
            row["recommended_reorder_quantity"] == math.ceil(avg * 30)
            or abs(row["recommended_reorder_quantity"] - math.ceil(avg * 30)) <= 1
        ), row
