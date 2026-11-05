-- =====================================================================
-- inventory_alerts.sql
-- Inventory stock-out, overstock and reorder queries (read-only).
-- Run one query:  psql -h localhost -p 5432 -U admin -d postgres -f <file>
-- Run all:         python analytics/run_all_queries.py
-- Schema facts (verified against migrations/postgres/002_create_analytics_tables.sql):
--   agg_inventory_health has product_id, warehouse_id, date, quantity_on_hand,
--   avg_daily_sales, days_of_stock_remaining, reorder_flag. It holds one row per
--   (product_id, warehouse_id, date). quantity_on_hand IS the current stock.
-- =====================================================================

-- ============================================
-- Query 3.1 - Products that will stock out in under 7 days
-- Purpose:      Early warning: products with positive stock whose remaining
--               days of cover are below one week.
-- Inputs:       warehouse.agg_inventory_health (latest snapshot per product and
--               warehouse), warehouse.dim_products (name)
-- Output columns:
--   product_id               BIGINT   product identifier
--   product_name             TEXT     dim_products.name ('Unknown' if no product row)
--   warehouse_id             VARCHAR  warehouse identifier
--   current_stock            INTEGER  quantity_on_hand in the latest snapshot (a real column)
--   days_of_stock_remaining  INTEGER  as recorded in the latest snapshot
--   reorder_flag             BOOLEAN  as recorded in the latest snapshot
-- Assumptions:
--   - "Current" means the most recent `date` per (product_id, warehouse_id).
--   - Rows with NULL days_of_stock_remaining are excluded (cannot be assessed).
-- Performance notes: ROW_NUMBER over agg_inventory_health partitioned by
--   (product_id, warehouse_id) keeps only the latest snapshot before the filter.
-- ORDER BY: days_of_stock_remaining ASC, product_id ASC, warehouse_id ASC.
WITH ranked_snapshots AS (
    SELECT
        h.product_id,
        h.warehouse_id,
        h.quantity_on_hand,
        h.days_of_stock_remaining,
        h.reorder_flag,
        ROW_NUMBER() OVER (
            PARTITION BY h.product_id, h.warehouse_id
            ORDER BY h.date DESC
        ) AS rn
    FROM warehouse.agg_inventory_health h
),
current_snapshot AS (
    SELECT * FROM ranked_snapshots WHERE rn = 1
)
SELECT
    cs.product_id,
    COALESCE(p.name, 'Unknown') AS product_name,
    cs.warehouse_id,
    cs.quantity_on_hand         AS current_stock,
    cs.days_of_stock_remaining,
    cs.reorder_flag
FROM current_snapshot cs
LEFT JOIN warehouse.dim_products p ON p.product_id = cs.product_id
WHERE cs.days_of_stock_remaining < 7
  AND cs.quantity_on_hand > 0
ORDER BY cs.days_of_stock_remaining ASC, cs.product_id ASC, cs.warehouse_id ASC;

-- ============================================
-- Query 3.2 - Products with more than 180 days of stock (overstock risk)
-- Purpose:      Capital tied up in slow-moving stock: products with more than six
--               months of cover at the current sales rate.
-- Inputs:       warehouse.agg_inventory_health (latest snapshot), warehouse.dim_products
-- Output columns: same as Query 3.1.
-- Assumptions:  same as Query 3.1.
-- Performance notes: same ROW_NUMBER pattern as Query 3.1.
-- ORDER BY: days_of_stock_remaining DESC, product_id ASC, warehouse_id ASC.
WITH ranked_snapshots AS (
    SELECT
        h.product_id,
        h.warehouse_id,
        h.quantity_on_hand,
        h.days_of_stock_remaining,
        h.reorder_flag,
        ROW_NUMBER() OVER (
            PARTITION BY h.product_id, h.warehouse_id
            ORDER BY h.date DESC
        ) AS rn
    FROM warehouse.agg_inventory_health h
),
current_snapshot AS (
    SELECT * FROM ranked_snapshots WHERE rn = 1
)
SELECT
    cs.product_id,
    COALESCE(p.name, 'Unknown') AS product_name,
    cs.warehouse_id,
    cs.quantity_on_hand         AS current_stock,
    cs.days_of_stock_remaining,
    cs.reorder_flag
FROM current_snapshot cs
LEFT JOIN warehouse.dim_products p ON p.product_id = cs.product_id
WHERE cs.days_of_stock_remaining > 180
ORDER BY cs.days_of_stock_remaining DESC, cs.product_id ASC, cs.warehouse_id ASC;

-- ============================================
-- Query 3.3 - Reorder suggestions
-- Purpose:      Products selling now whose stock covers less than two weeks, with
--               the quantity needed for one month of cover.
-- Inputs:       warehouse.agg_inventory_health (latest snapshot per warehouse),
--               warehouse.fact_orders (quantity, last 30 days),
--               warehouse.dim_date, warehouse.dim_products
-- Output columns:
--   product_id                   BIGINT   product identifier
--   product_name                 TEXT     dim_products.name ('Unknown' if no product row)
--   current_stock                BIGINT   quantity_on_hand summed over warehouses (latest snapshots)
--   avg_daily_sales              NUMERIC  units sold in the trailing 30 days / 30, 2 dp
--   recommended_reorder_quantity BIGINT   CEIL(avg_daily_sales * 30): one month of stock
-- Assumptions:
--   - avg_daily_sales is recomputed from fact_orders (SUM(quantity) over the last
--     30 calendar days / 30). The agg table's avg_daily_sales is not used, because
--     its window is not documented.
--   - Trailing window is CURRENT_DATE - 29 .. CURRENT_DATE (30 days inclusive).
--   - Products with no inventory snapshot are excluded (no stock figure to compare).
--   - Filter: avg_daily_sales > 0 and current_stock < avg_daily_sales * 14.
-- Performance notes: the sales CTE aggregates one 30-day window; the stock CTE
--   reads only the latest snapshot per (product, warehouse); the two are joined
--   on product_id.
-- ORDER BY: avg_daily_sales DESC, product_id ASC.
WITH ranked_snapshots AS (
    SELECT
        h.product_id,
        h.warehouse_id,
        h.quantity_on_hand,
        ROW_NUMBER() OVER (
            PARTITION BY h.product_id, h.warehouse_id
            ORDER BY h.date DESC
        ) AS rn
    FROM warehouse.agg_inventory_health h
),
current_stock AS (
    SELECT
        product_id,
        SUM(quantity_on_hand) AS current_stock
    FROM ranked_snapshots
    WHERE rn = 1
    GROUP BY product_id
),
recent_sales AS (
    SELECT
        f.product_id,
        SUM(f.quantity) / 30.0 AS avg_daily_sales
    FROM warehouse.fact_orders f
    JOIN warehouse.dim_date d ON d.date_id = f.date_id
    WHERE f.quantity > 0
      AND f.total_amount >= 0
      AND d.date BETWEEN CURRENT_DATE - INTERVAL '29 days' AND CURRENT_DATE
    GROUP BY f.product_id
),
reorder_candidates AS (
    SELECT
        rs.product_id,
        ROUND(rs.avg_daily_sales, 2) AS avg_daily_sales,
        cs.current_stock,
        rs.avg_daily_sales AS avg_daily_sales_exact
    FROM recent_sales rs
    JOIN current_stock cs ON cs.product_id = rs.product_id
    WHERE rs.avg_daily_sales > 0
      AND cs.current_stock < rs.avg_daily_sales * 14
)
SELECT
    rc.product_id,
    COALESCE(p.name, 'Unknown')                          AS product_name,
    rc.current_stock,
    rc.avg_daily_sales,
    CEIL(rc.avg_daily_sales_exact * 30)::bigint          AS recommended_reorder_quantity
FROM reorder_candidates rc
LEFT JOIN warehouse.dim_products p ON p.product_id = rc.product_id
ORDER BY rc.avg_daily_sales DESC, rc.product_id ASC;
