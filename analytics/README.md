# Analytics queries

Read-only SQL over the PostgreSQL warehouse (`warehouse` schema). Intended for BI
dashboards, ad-hoc analysis, and future API endpoints.

## Files

| File | Queries | Purpose |
|---|---|---|
| `queries/revenue_analytics.sql` | 1.1 to 1.4 | Daily revenue with a 7-day moving average; revenue by category, current month vs last year; top 20 products this month; revenue by customer segment |
| `queries/funnel_analysis.sql` | 2.1 to 2.3 | Page view, add-to-cart and purchase funnel by device; categories with high cart volume; session duration, pages per session and bounce rate |
| `queries/inventory_alerts.sql` | 3.1 to 3.3 | Stock-out risk (under 7 days), overstock (over 180 days), reorder suggestions |
| `queries/customer_analytics.sql` | 4.1 to 4.3 | Monthly cohort retention (long format), customer lifetime value by cohort, churned customers (last order 90 to 180 days ago) |
| `run_all_queries.py` | - | Runs every query and prints a pass or fail line for each |

Each query block has a header with Purpose, Inputs, Output columns, Assumptions and
Performance notes. Blocks are separated by `-- ====` lines, which the runner uses to split them.

## Running a query

Against the dev stack (port 5432):

```bash
psql -h localhost -p 5432 -U admin -d postgres -f analytics/queries/revenue_analytics.sql
```

Against the integration-test stack (port 5433):

```bash
psql -h localhost -p 5433 -U admin -d postgres -f analytics/queries/revenue_analytics.sql
```

(The test stack uses the same credentials as the Spark job: `admin` / `postgres123`.)

If `psql` is not installed on the host, run it inside the container:

```bash
docker exec -i ecommerce_postgres psql -U admin -d postgres < analytics/queries/revenue_analytics.sql
```

## Running all queries

```bash
# Syntax check only (EXPLAIN, nothing executes)
python analytics/run_all_queries.py --dry-run

# Execute everything
python analytics/run_all_queries.py

# One file
python analytics/run_all_queries.py --file funnel_analysis.sql
```

The runner reads `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
and `POSTGRES_DB`. Each output line is `[PASS]` or `[FAIL]`, with the row count and
duration for passing queries. The exit code is 0 only if every query passed.

The runner opens connections read-only, so it is safe to run against a read replica.

## Safety

- Queries are read-only. The runner sets `read_only` on its connection.
- `customer_analytics.sql` Query 4.3 returns customer email addresses. Restrict access to that output.

## Assumptions: verified against the schema

Checked with `\d` on the test database and against the migrations.

- `warehouse.dim_products.category` exists and is used directly. Missing values appear as `Uncategorized`.
- `warehouse.agg_inventory_health` has `quantity_on_hand`, so `current_stock` is a real column. The table holds one row per date, so "current" means the most recent `date` per product and warehouse.
- `warehouse.fact_clickstream` has `device`, not `device_type`. The queries use `device`.
- `warehouse.dim_date` provides `date` and `date_id`. Queries rely on it being complete (every calendar day).
- `fact_orders` holds only valid rows; quarantined rows go to `quarantine_orders`. The queries still add a defensive filter (`quantity > 0`, `total_amount >= 0`).

## Assumptions: not verified against production data

- **Segment values.** The canonical vocabulary is `VIP`, `Regular`, `New`, as used by `scripts/generate_fake_data.py` and `scripts/fix_dim_users.py`. Migration 008 corrects the `dim_users.segment` column comment in the database. The comment inside migration 001 still says `Gold`, `Silver`, `Bronze`, `New`; migration 001 is not edited, because existing migrations are not changed. The queries lowercase and group whatever values exist, so they work with either vocabulary.
- **Clickstream event time.** `fact_clickstream` has no event timestamp. Its `created_at` is the load time, so session durations (Query 2.3) are indicative only.
- **Clickstream event volume.** The clickstream load writes columns that do not match the table (for example `device_type`, `os`, `is_bot`). Until that is fixed, `fact_clickstream` may be sparse or empty, and Queries 2.1 to 2.3 will return few rows.
- **Purchases.** Purchases are taken from `fact_clickstream` `purchase` events, as specified, not from `fact_orders`.

## Performance

No new indexes were added. The existing indexes on `fact_orders` (`date_id`, `user_id`,
`product_id`, `status`) and `fact_clickstream` (`date_id`, `user_id`, `session_id`,
`event_type`) cover the joins and filters these queries use. Clickstream queries that
join on `product_id` (Query 2.2) have no index in migration 001, so they scan more
than the others. Revisit if
query times grow past a few seconds at production volume.
