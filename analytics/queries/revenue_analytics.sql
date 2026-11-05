-- =====================================================================
-- revenue_analytics.sql
-- Revenue queries over the star schema (read-only).
-- Run one query:  psql -h localhost -p 5432 -U admin -d postgres -f <file>
-- Run all:         python analytics/run_all_queries.py
-- =====================================================================

-- ============================================
-- Query 1.1 - Daily revenue trend with 7-day moving average (last 90 days)
-- Purpose:      Daily revenue, orders and AOV for the last 90 days, with a
--               7-day trailing moving average of revenue. Days with no orders
--               are kept as zero.
-- Inputs:       warehouse.dim_date (driving table), warehouse.fact_orders
--               (date_id, order_id, total_amount, quantity)
-- Output columns:
--   revenue_date            DATE     calendar day
--   daily_revenue           NUMERIC  sum of total_amount that day (0 if none)
--   daily_orders            BIGINT   distinct orders that day (0 if none)
--   avg_order_value         NUMERIC  daily_revenue / daily_orders, NULL if no orders
--   moving_avg_7d_revenue   NUMERIC  mean daily_revenue over the 7 days ending today
-- Assumptions:
--   - dim_date holds every calendar day (the moving average is row-based).
--   - Base rows must have quantity > 0 and total_amount >= 0. fact_orders only
--     holds valid rows (quarantined rows go to quarantine_orders), so this is a
--     defensive filter.
-- Performance notes: one scan of fact_orders over a 96-day window, joined on
--   date_id (indexed). The window function runs over at most ~97 rows.
-- ORDER BY: revenue_date DESC (unique per row).
WITH spine AS (
    -- 96 days: 90 reported plus 6 days of history for the 7-day window
    SELECT d.date AS revenue_date
    FROM warehouse.dim_date d
    WHERE d.date BETWEEN CURRENT_DATE - INTERVAL '95 days' AND CURRENT_DATE
),
daily AS (
    SELECT
        d.date                         AS order_date,
        SUM(f.total_amount)            AS daily_revenue,
        COUNT(DISTINCT f.order_id)     AS daily_orders
    FROM warehouse.fact_orders f
    JOIN warehouse.dim_date d ON d.date_id = f.date_id
    WHERE f.quantity > 0
      AND f.total_amount >= 0
      AND d.date BETWEEN CURRENT_DATE - INTERVAL '95 days' AND CURRENT_DATE
    GROUP BY d.date
),
filled AS (
    -- Driving from dim_date keeps zero-order days
    SELECT
        s.revenue_date,
        COALESCE(dy.daily_revenue, 0) AS daily_revenue,
        COALESCE(dy.daily_orders, 0)  AS daily_orders
    FROM spine s
    LEFT JOIN daily dy ON dy.order_date = s.revenue_date
),
windowed AS (
    SELECT
        revenue_date,
        daily_revenue,
        daily_orders,
        AVG(daily_revenue) OVER (
            ORDER BY revenue_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS moving_avg_7d_revenue
    FROM filled
)
SELECT
    revenue_date,
    daily_revenue,
    daily_orders,
    CASE WHEN daily_orders > 0
         THEN ROUND(daily_revenue / daily_orders, 2)
    END AS avg_order_value,
    ROUND(moving_avg_7d_revenue, 2) AS moving_avg_7d_revenue
FROM windowed
WHERE revenue_date >= CURRENT_DATE - INTERVAL '89 days'
ORDER BY revenue_date DESC;

-- ============================================
-- Query 1.2 - Revenue by product category: current month vs same month last year
-- Purpose:      Compare this calendar month's revenue per category with the same
--               calendar month one year earlier.
-- Inputs:       warehouse.fact_orders, warehouse.dim_date, warehouse.dim_products
-- Output columns:
--   category                       TEXT     dim_products.category ('Uncategorized' if NULL)
--   current_month_revenue          NUMERIC  revenue this month (0 if none)
--   last_year_same_month_revenue   NUMERIC  revenue same month last year (0 if none)
--   pct_change                     NUMERIC  (current - last) / last * 100; NULL if last = 0
-- Assumptions:
--   - dim_products.category exists in the schema and is used directly (verified).
--   - Month boundaries use CURRENT_DATE; "same month last year" is the calendar
--     month shifted back 12 months.
-- Performance notes: one join pass over fact_orders limited to two month windows
--   (date_id index on fact_orders). The bounds CTE is a single row cross-joined.
-- ORDER BY: current_month_revenue DESC, category ASC.
WITH bounds AS (
    SELECT
        DATE_TRUNC('month', CURRENT_DATE)::date                                    AS cur_start,
        (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month')::date             AS cur_end,
        (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 year')::date              AS ly_start,
        (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 year' + INTERVAL '1 month')::date AS ly_end
),
order_lines AS (
    SELECT
        COALESCE(p.category, 'Uncategorized') AS category,
        d.date                                AS order_date,
        f.total_amount
    FROM warehouse.fact_orders f
    JOIN warehouse.dim_date d     ON d.date_id = f.date_id
    JOIN warehouse.dim_products p ON p.product_id = f.product_id
    CROSS JOIN bounds b
    WHERE f.quantity > 0
      AND f.total_amount >= 0
      AND d.date >= b.ly_start
      AND d.date <  b.cur_end
      AND (
            (d.date >= b.cur_start AND d.date < b.cur_end)
         OR (d.date >= b.ly_start  AND d.date < b.ly_end)
      )
),
by_category AS (
    SELECT
        ol.category,
        COALESCE(SUM(ol.total_amount) FILTER (
            WHERE ol.order_date >= b.cur_start AND ol.order_date < b.cur_end), 0) AS current_month_revenue,
        COALESCE(SUM(ol.total_amount) FILTER (
            WHERE ol.order_date >= b.ly_start AND ol.order_date < b.ly_end), 0)   AS last_year_same_month_revenue
    FROM order_lines ol
    CROSS JOIN bounds b
    GROUP BY ol.category
)
SELECT
    category,
    current_month_revenue,
    last_year_same_month_revenue,
    CASE WHEN last_year_same_month_revenue = 0 THEN NULL
         ELSE ROUND((current_month_revenue - last_year_same_month_revenue)
                    / last_year_same_month_revenue * 100, 2)
    END AS pct_change
FROM by_category
ORDER BY current_month_revenue DESC, category ASC;

-- ============================================
-- Query 1.3 - Top 20 products by revenue this month, with contribution %
-- Purpose:      Best-selling products this calendar month and each product's share
--               of total revenue this month.
-- Inputs:       warehouse.fact_orders, warehouse.dim_date, warehouse.dim_products
-- Output columns:
--   product_id             BIGINT   product identifier
--   product_name           TEXT     dim_products.name ('Unknown' if no product row)
--   revenue                NUMERIC  sum of total_amount this month
--   units_sold             BIGINT   sum of quantity this month
--   pct_of_total_revenue   NUMERIC  revenue / total revenue this month * 100, 2 dp
-- Assumptions:
--   - The denominator is total revenue across ALL products this month, computed
--     before the LIMIT, so the top-20 shares are true shares of the whole month.
-- Performance notes: aggregates a single month of fact_orders, then sorts. The
--   window SUM runs over the grouped product rows only.
-- ORDER BY: revenue DESC, product_id ASC (deterministic on ties). LIMIT 20.
WITH month_sales AS (
    SELECT
        f.product_id,
        SUM(f.total_amount) AS revenue,
        SUM(f.quantity)     AS units_sold
    FROM warehouse.fact_orders f
    JOIN warehouse.dim_date d ON d.date_id = f.date_id
    WHERE f.quantity > 0
      AND f.total_amount >= 0
      AND d.date >= DATE_TRUNC('month', CURRENT_DATE)::date
      AND d.date <  (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month')::date
    GROUP BY f.product_id
),
ranked AS (
    SELECT
        m.product_id,
        COALESCE(p.name, 'Unknown')  AS product_name,
        m.revenue,
        m.units_sold,
        ROUND(m.revenue / NULLIF(SUM(m.revenue) OVER (), 0) * 100, 2) AS pct_of_total_revenue
    FROM month_sales m
    LEFT JOIN warehouse.dim_products p ON p.product_id = m.product_id
)
SELECT
    product_id,
    product_name,
    revenue,
    units_sold,
    pct_of_total_revenue
FROM ranked
ORDER BY revenue DESC, product_id ASC
LIMIT 20;

-- ============================================
-- Query 1.4 - Revenue by customer segment
-- Purpose:      Revenue, orders, AOV and unique customers per customer segment.
-- Inputs:       warehouse.fact_orders, warehouse.dim_users
-- Output columns:
--   segment            TEXT     dim_users.segment, lowercased ('unknown' if NULL)
--   total_revenue      NUMERIC  sum of total_amount
--   total_orders       BIGINT   distinct order_id count
--   avg_order_value    NUMERIC  total_revenue / total_orders
--   unique_customers   BIGINT   distinct user_id count
-- Assumptions:
--   - Segment values are NOT hardcoded. The schema comment on dim_users.segment
--     canonical values are 'VIP', 'Regular', 'New' (see analytics/README.md and
--     migration 008, which corrects the column comment). Whatever values exist
--     are grouped, lowercased, so the query works with either vocabulary.
--   - The fact table is line-level, so total_orders counts distinct order_id.
-- Performance notes: one join of fact_orders to dim_users on user_id (indexed).
-- ORDER BY: total_revenue DESC, segment ASC.
WITH order_base AS (
    SELECT
        LOWER(COALESCE(u.segment, 'unknown')) AS segment,
        f.order_id,
        f.user_id,
        f.total_amount
    FROM warehouse.fact_orders f
    JOIN warehouse.dim_users u ON u.user_id = f.user_id
    WHERE f.quantity > 0
      AND f.total_amount >= 0
)
SELECT
    segment,
    SUM(total_amount)                                       AS total_revenue,
    COUNT(DISTINCT order_id)                                AS total_orders,
    ROUND(SUM(total_amount) / NULLIF(COUNT(DISTINCT order_id), 0), 2) AS avg_order_value,
    COUNT(DISTINCT user_id)                                 AS unique_customers
FROM order_base
GROUP BY segment
ORDER BY total_revenue DESC, segment ASC;
