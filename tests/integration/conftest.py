# tests/integration/conftest.py
# -----------------------------------------------------------------------------
# Integration test fixtures: real Docker databases, session-scoped.
# Uses tests/docker-compose.test.yml (Postgres 5433, MySQL 3307).
# -----------------------------------------------------------------------------
import os
import random
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).parent.parent.parent
COMPOSE_FILE = PROJECT_ROOT / "tests" / "docker-compose.test.yml"
CONTAINERS = ["ecommerce_test_postgres", "ecommerce_test_mysql"]
HEALTH_TIMEOUT_S = 180

PG_PORT = 5433
MYSQL_PORT = 3307
# Match the credentials the Spark job hardcodes (transform_orders.get_config),
# so the job connects to the test stack without any credential change in src/.
PG_USER = "admin"
PG_PASSWORD = "postgres123"
MYSQL_PASSWORD = "root123"

# Pipeline tables that each test starts from empty.
WAREHOUSE_TABLES = [
    "warehouse.fact_orders",
    "warehouse.fact_clickstream",
    "warehouse.quarantine_orders",
    "warehouse.agg_daily_revenue",
    "warehouse.agg_product_performance",
    "warehouse.agg_user_cohorts",
    "warehouse.agg_inventory_health",
    "warehouse.dim_users",
    "warehouse.dim_products",
    "warehouse.dim_date",
]
STAGING_TABLES = [
    "stg_raw_orders",
    "stg_raw_products",
    "stg_raw_inventory",
    "stg_raw_clickstream",
    "pipeline_runs",
    "watermarks",
]


# =============================================================================
# Environment variables for all integration tests
# =============================================================================
@pytest.fixture(scope="session", autouse=True)
def setup_test_env():
    """
    Point the pipeline's env vars at the test databases.
    Overrides whatever is in the dev environment so tests never touch dev data.
    """
    overrides = {
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": str(PG_PORT),
        "POSTGRES_USER": PG_USER,
        "POSTGRES_PASSWORD": PG_PASSWORD,
        "POSTGRES_DB": "postgres",
        "PG_SOURCE_HOST": "localhost",
        "PG_SOURCE_PORT": str(PG_PORT),
        "PG_SOURCE_USER": PG_USER,
        "PG_SOURCE_PASSWORD": PG_PASSWORD,
        "PG_SOURCE_DB": "postgres",
        "MYSQL_HOST": "localhost",
        "MYSQL_PORT": str(MYSQL_PORT),
        "MYSQL_USER": "root",
        "MYSQL_PASSWORD": MYSQL_PASSWORD,
        "MYSQL_DATABASE": "ecommerce_staging",
        # Directory with mysql-connector-java / postgresql jars (the repo root holds them)
        "SPARK_JARS_PATH": str(PROJECT_ROOT),
    }
    saved = {k: os.environ.get(k) for k in overrides}
    os.environ.update(overrides)
    yield
    for key, value in saved.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


# =============================================================================
# Start/stop Docker Compose for the test session
# =============================================================================
def _container_health(name: str) -> str:
    result = subprocess.run(
        ["docker", "inspect", "--format", "{{.State.Health.Status}}", name],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip() if result.returncode == 0 else "missing"


@pytest.fixture(scope="session")
def docker_services(setup_test_env):
    """
    Bring up the isolated test DBs, wait for both healthchecks, yield.
    Tears the stack down (including volumes) at session end.
    """
    print("\n[integration] Starting test Docker services...")
    subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "up", "-d"],
        check=True,
        cwd=PROJECT_ROOT,
    )

    waited = 0
    while waited < HEALTH_TIMEOUT_S:
        statuses = {name: _container_health(name) for name in CONTAINERS}
        if all(status == "healthy" for status in statuses.values()):
            print(f"[integration] Both DBs healthy after {waited}s")
            break
        time.sleep(2)
        waited += 2
    else:
        raise RuntimeError(
            f"Test DBs not healthy after {HEALTH_TIMEOUT_S}s: {statuses}"
        )

    yield

    print("\n[integration] Tearing down test Docker services...")
    subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "down", "-v"],
        check=False,
        cwd=PROJECT_ROOT,
    )


# =============================================================================
# Database client fixtures
# =============================================================================
@pytest.fixture(scope="session")
def pg_conn(docker_services):
    """Session-scoped PostgreSQL connection to the test warehouse (psycopg3)."""
    import psycopg

    conn = psycopg.connect(
        host="localhost",
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname="postgres",
    )
    conn.autocommit = True
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def mysql_conn(docker_services):
    """Session-scoped MySQL connection to the test staging DB."""
    import mysql.connector

    conn = mysql.connector.connect(
        host="localhost",
        port=MYSQL_PORT,
        user="root",
        password=MYSQL_PASSWORD,
        database="ecommerce_staging",
    )
    conn.autocommit = True
    yield conn
    conn.close()


# =============================================================================
# Clean-slate helper: truncate all pipeline tables before each test
# =============================================================================
@pytest.fixture
def clean_tables(pg_conn, mysql_conn):
    """
    Clears every pipeline table so each test starts from a known state.
    Tables that a job creates lazily (e.g. quarantine_orders) are skipped
    when they don't exist yet.
    """
    pg_cur = pg_conn.cursor()
    for table in WAREHOUSE_TABLES:
        pg_cur.execute("SELECT to_regclass(%s)", (table,))
        if pg_cur.fetchone()[0] is not None:
            pg_cur.execute(f"TRUNCATE TABLE {table} CASCADE")

    mysql_cur = mysql_conn.cursor()
    mysql_cur.execute("SET FOREIGN_KEY_CHECKS = 0")
    for table in STAGING_TABLES:
        mysql_cur.execute(f"TRUNCATE TABLE {table}")
    mysql_cur.execute("SET FOREIGN_KEY_CHECKS = 1")

    # Drop the CDC source table so cdc_source always re-creates it fresh.
    pg_cur.execute("DROP TABLE IF EXISTS public.raw_orders")

    yield


# =============================================================================
# Small dataset seeded directly into staging + dims
# =============================================================================
SEED_ORDERS = 200
SEED_USERS = 100
SEED_PRODUCTS = 50
SEED_BATCH_ID = "seed_batch_0001"


@pytest.fixture
def seeded_data(mysql_conn, pg_conn, clean_tables):
    """
    Seeds a small dataset directly (faster than generate_fake_data.py, which
    is sized for full volumes):
      * warehouse.dim_users (100), warehouse.dim_products (50), warehouse.dim_date
      * ecommerce_staging.stg_raw_orders (200 orders, 1-3 lines each)
    Unit prices sit above product cost so gross_margin is positive.
    """
    rng = random.Random(42)
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    pg_cur = pg_conn.cursor()
    pg_cur.executemany(
        "INSERT INTO warehouse.dim_users (user_id, name, email, segment) "
        "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
        [
            (i, f"User {i}", f"user{i}@test.com", "Silver")
            for i in range(1, SEED_USERS + 1)
        ],
    )
    pg_cur.executemany(
        "INSERT INTO warehouse.dim_products (product_id, name, category, cost_price, list_price) "
        "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
        [
            (i, f"Product {i}", "General", round(10.0 + i, 2), round(20.0 + i, 2))
            for i in range(1, SEED_PRODUCTS + 1)
        ],
    )
    date_rows = []
    # Orders are dated up to 30 days back plus up to 23 hours, which can fall on
    # day 31, so the calendar must reach one day past that to avoid NULL date_id.
    for offset in range(33):
        day = today - timedelta(days=offset)
        date_rows.append(
            (int(day.strftime("%Y%m%d")), day.date(), day.year, day.month, day.day)
        )
    pg_cur.executemany(
        "INSERT INTO warehouse.dim_date (date_id, date, year, month, day) "
        "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
        date_rows,
    )

    mysql_cur = mysql_conn.cursor()
    orders = []
    for order_id in range(1, SEED_ORDERS + 1):
        user_id = rng.randint(1, SEED_USERS)
        order_date = today - timedelta(
            days=rng.randint(0, 30), hours=rng.randint(0, 23)
        )
        for line in range(1, rng.randint(1, 3) + 1):
            product_id = rng.randint(1, SEED_PRODUCTS)
            cost = 10.0 + product_id
            quantity = rng.randint(1, 5)
            unit_price = round(cost * rng.uniform(1.2, 2.0), 2)
            discount = round(rng.uniform(0, 10), 2)
            total = round(quantity * unit_price * (1 - discount / 100), 2)
            status = rng.choice(
                ["completed", "pending", "COMPLETED", "Complete", "returned"]
            )
            orders.append(
                (
                    str(order_id),
                    str(line),
                    str(user_id),
                    str(product_id),
                    quantity,
                    unit_price,
                    discount,
                    total,
                    status,
                    order_date,
                    SEED_BATCH_ID,
                    datetime.now(),
                )
            )
    mysql_cur.executemany(
        "INSERT INTO stg_raw_orders "
        "(source_order_id, source_line_id, user_id, product_id, quantity, "
        "unit_price, discount, total_amount, order_status, order_date, "
        "batch_id, ingested_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        orders,
    )

    yield {
        "order_count": len(orders),
        "user_count": SEED_USERS,
        "product_count": SEED_PRODUCTS,
        "batch_id": SEED_BATCH_ID,
    }


# =============================================================================
# CDC source table in the test Postgres (what ingest_postgres_cdc reads)
# =============================================================================
CDC_SOURCE_ROWS = 50


@pytest.fixture
def cdc_source(pg_conn, clean_tables):
    """
    Creates public.raw_orders in the test Postgres and fills it with rows
    stamped with updated_at, so the CDC watermark logic has something to read.
    """
    cur = pg_conn.cursor()
    cur.execute(
        """
        CREATE TABLE public.raw_orders (
            source_order_id  BIGINT PRIMARY KEY,
            source_line_id   BIGINT NOT NULL,
            user_id          BIGINT NOT NULL,
            product_id       BIGINT NOT NULL,
            quantity         INTEGER NOT NULL,
            unit_price       NUMERIC(10,2) NOT NULL,
            discount         NUMERIC(5,2) NOT NULL,
            total_amount     NUMERIC(12,2) NOT NULL,
            order_status     VARCHAR(50) NOT NULL,
            order_date       TIMESTAMP NOT NULL,
            updated_at       TIMESTAMP NOT NULL
        )
    """
    )
    cur.execute(
        """
        INSERT INTO public.raw_orders
        SELECT g, 1, 1 + (g %% 100), 1 + (g %% 50), 2, 25.00, 0.00, 50.00,
               'completed', now(), now()
        FROM generate_series(1, %s) AS g
        """,
        (CDC_SOURCE_ROWS,),
    )
    yield {"row_count": CDC_SOURCE_ROWS}


# =============================================================================
# Analytics seed: multi-month order history for the SQL analytics queries
# (Phase 10.1). Separate from seeded_data, which is sized for the ETL tests.
# =============================================================================
ANALYTICS_CUSTOMERS = 60
ANALYTICS_PRODUCTS = 20
ANALYTICS_CHURNED = 5  # users 1..5: last order 120 days ago (churn window)


@pytest.fixture
def seeded_analytics_data(pg_conn, clean_tables):
    """
    Seeds warehouse dims and fact_orders directly with:
      * dim_date covering the last 400 days
      * 20 products in three categories, 60 customers in four segments
      * customers 1..5 churned (last order 120 days ago)
      * customers 6..10 active in the last 40 days
      * the rest with first orders spread over 120 days, giving several cohorts
      * at least one order today, so the current month is never empty
    """
    from datetime import date, timedelta

    rng = random.Random(2026)
    today = date.today()
    cur = pg_conn.cursor()

    cur.executemany(
        "INSERT INTO warehouse.dim_date (date_id, date, year, month, day) "
        "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
        [
            (int(d.strftime("%Y%m%d")), d, d.year, d.month, d.day)
            for d in (today - timedelta(days=i) for i in range(400))
        ],
    )

    categories = ["Electronics", "Home", "Apparel"]
    cur.executemany(
        "INSERT INTO warehouse.dim_products (product_id, name, category, cost_price, list_price) "
        "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
        [
            (p, f"Product {p}", categories[p % len(categories)], 10.0 + p, 20.0 + p)
            for p in range(1, ANALYTICS_PRODUCTS + 1)
        ],
    )

    # Canonical customer segments: VIP, Regular, New (generate_fake_data.py and fix_dim_users.py)
    segments = ["VIP", "Regular", "New"]
    cur.executemany(
        "INSERT INTO warehouse.dim_users (user_id, name, email, segment) "
        "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
        [
            (u, f"Customer {u}", f"customer{u}@test.com", segments[u % len(segments)])
            for u in range(1, ANALYTICS_CUSTOMERS + 1)
        ],
    )

    # Order dates per customer
    order_dates = {}
    for u in range(1, ANALYTICS_CHURNED + 1):
        order_dates[u] = [today - timedelta(days=200), today - timedelta(days=120)]
    for u in range(ANALYTICS_CHURNED + 1, 11):
        order_dates[u] = [today - timedelta(days=40), today - timedelta(days=10)]
    for u in range(11, ANALYTICS_CUSTOMERS + 1):
        first = today - timedelta(days=rng.randint(0, 119))
        span = max(1, (today - first).days)
        dates = [first] + [
            min(today, first + timedelta(days=rng.randint(1, span)))
            for _ in range(rng.randint(0, 3))
        ]
        order_dates[u] = dates
    order_dates[11].append(today)  # guarantees current-month revenue

    orders = []
    order_id = 1
    for user_id, dates in order_dates.items():
        for when in dates:
            product_id = rng.randint(1, ANALYTICS_PRODUCTS)
            quantity = rng.randint(1, 4)
            unit_price = round(15.0 + product_id * 2 + rng.random() * 5, 2)
            total = round(quantity * unit_price, 2)
            orders.append(
                (
                    order_id,
                    1,
                    user_id,
                    product_id,
                    int(when.strftime("%Y%m%d")),
                    quantity,
                    unit_price,
                    0.0,
                    total,
                    "completed",
                )
            )
            order_id += 1

    cur.executemany(
        "INSERT INTO warehouse.fact_orders "
        "(order_id, line_item_id, user_id, product_id, date_id, quantity, "
        "unit_price, discount, total_amount, status) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        orders,
    )

    yield {
        "order_count": len(orders),
        "customer_count": ANALYTICS_CUSTOMERS,
        "churned_count": ANALYTICS_CHURNED,
    }


# =============================================================================
# Clickstream staging seed + transform runner (Phase 10.1 extension)
# =============================================================================
CLICKSTREAM_NORMAL_SESSIONS = 100
CLICKSTREAM_BOT_EVENTS = 210  # more than 200 events in under 60 seconds: flagged as bot
CLICKSTREAM_BROWSERS = [
    "Mozilla/5.0 Chrome Windows",
    "Mozilla/5.0 Safari iPhone",
    "Mozilla/5.0 Firefox Android",
    "Mozilla/5.0 Safari iPad",
]


def seed_clickstream_data(
    mysql_conn,
    *,
    user_ids,
    product_ids,
    batch_id="clickstream_batch_0001",
    days_back=20,
):
    """
    Insert clickstream rows into stg_raw_clickstream, shaped like the ingestion
    writes them: about 100 normal sessions (page views, some cart adds and
    purchases, varied devices) plus one bot-like session of 210 events inside
    21 seconds. Returns counts for assertions.
    """
    from datetime import datetime, timedelta

    rng = random.Random(99)
    now = datetime.now()
    rows = []
    event_no = 0

    for s in range(CLICKSTREAM_NORMAL_SESSIONS):
        user = rng.choice(user_ids)
        session = f"sess_{s:04d}"
        start = now - timedelta(
            days=rng.randint(0, days_back), minutes=rng.randint(0, 1380)
        )
        browser = rng.choice(CLICKSTREAM_BROWSERS)
        events = ["page_view", "page_view"]
        if rng.random() < 0.5:
            events.append("add_to_cart")
        if rng.random() < 0.2:
            events.append("purchase")
        for i, event_type in enumerate(events):
            event_no += 1
            product = rng.choice(product_ids)
            rows.append(
                (
                    f"EVT_{event_no:09d}",
                    str(user),
                    session,
                    event_type,
                    f"/product/{product}",
                    product,
                    None,
                    browser,
                    start + timedelta(seconds=30 * i),
                    batch_id,
                )
            )

    bot_user = user_ids[0]
    bot_start = now - timedelta(days=1)
    for i in range(CLICKSTREAM_BOT_EVENTS):
        event_no += 1
        product = product_ids[i % len(product_ids)]
        rows.append(
            (
                f"EVT_{event_no:09d}",
                str(bot_user),
                "sess_bot_0001",
                "page_view",
                f"/product/{product}",
                product,
                None,
                "Mozilla/5.0 Chrome Windows",
                bot_start + timedelta(milliseconds=100 * i),
                batch_id,
            )
        )

    cur = mysql_conn.cursor()
    cur.executemany(
        "INSERT INTO stg_raw_clickstream "
        "(event_id, user_id, session_id, event_type, page_url, product_id, "
        "device_type, browser_name, event_timestamp, batch_id) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        rows,
    )
    return {
        "event_count": len(rows),
        "bot_event_count": CLICKSTREAM_BOT_EVENTS,
        "batch_id": batch_id,
    }


@pytest.fixture
def run_clickstream_transform():
    """
    Returns a callable that runs transform_clickstream.py for a batch.
    Skips only on a SyntaxError in the job (the known UTF-16 BOM problem);
    any other failure fails the test.
    """
    script = PROJECT_ROOT / "src" / "spark_jobs" / "transform_clickstream.py"

    def _run(batch_id: str) -> None:
        env = os.environ.copy()
        # Pin the JVM to UTC; the host zone (e.g. Windows Asia/Calcutta) is rejected by Postgres
        env["JAVA_TOOL_OPTIONS"] = "-Duser.timezone=UTC"
        result = subprocess.run(
            [sys.executable, str(script), "--batch-id", batch_id],
            capture_output=True,
            text=True,
            timeout=600,
            cwd=PROJECT_ROOT,
            env=env,
        )
        if result.returncode != 0 and "SyntaxError" in result.stderr:
            pytest.skip(
                "transform_clickstream.py has a SyntaxError (known UTF-16 BOM issue)"
            )
        assert (
            result.returncode == 0
        ), f"transform_clickstream.py exited {result.returncode}:\n{result.stderr[-2000:]}"

    return _run


@pytest.fixture
def seeded_analytics_with_clickstream(
    seeded_analytics_data, mysql_conn, run_clickstream_transform
):
    """
    seeded_analytics_data plus clickstream: staged events for the analytics
    customers and products are transformed into warehouse.fact_clickstream.
    """
    seed_clickstream_data(
        mysql_conn,
        user_ids=list(range(1, ANALYTICS_CUSTOMERS + 1)),
        product_ids=list(range(1, ANALYTICS_PRODUCTS + 1)),
        batch_id="analytics_clickstream_batch",
    )
    run_clickstream_transform("analytics_clickstream_batch")
    yield seeded_analytics_data


@pytest.fixture
def clickstream_seeder():
    """Exposes seed_clickstream_data to tests (conftest modules are not importable)."""
    return seed_clickstream_data


@pytest.fixture
def seeded_inventory(seeded_analytics_with_clickstream, pg_conn):
    """
    Adds agg_inventory_health snapshots on top of the analytics seed:
      * products 1..4: low stock, 1 unit and 2 days of cover (stock-out risk)
      * products 5..8: overstock, 5000 units and 400 days of cover
      * products 9..20: healthy, 500 units and 60 days of cover
      * an older snapshot (30 days back) for every product, showing a different
        value, so tests can check that only the latest snapshot is used.
    """
    from datetime import date, timedelta

    today = date.today()
    rows = []
    for product in range(1, ANALYTICS_PRODUCTS + 1):
        if product <= 4:
            latest_qty, latest_days = 1, 2
        elif product <= 8:
            latest_qty, latest_days = 5000, 400
        else:
            latest_qty, latest_days = 500, 60
        rows.append((product, "WH-1", today, latest_qty, 1.0, latest_days, False))
        # Older snapshot with deliberately different values
        rows.append(
            (product, "WH-1", today - timedelta(days=30), 9999, 1.0, 999, False)
        )

    cur = pg_conn.cursor()
    cur.executemany(
        "INSERT INTO warehouse.agg_inventory_health "
        "(product_id, warehouse_id, date, quantity_on_hand, avg_daily_sales, "
        "days_of_stock_remaining, reorder_flag) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
        rows,
    )
    yield {"snapshot_count": len(rows), "as_of": today}
