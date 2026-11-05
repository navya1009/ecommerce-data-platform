-- =====================================================================
-- customer_analytics.sql
-- Cohort retention, lifetime value and churn queries (read-only).
-- Run one query:  psql -h localhost -p 5432 -U admin -d postgres -f <file>
-- Run all:         python analytics/run_all_queries.py
-- Customer "first order month" = the month of the customer's earliest order in
-- fact_orders. All three queries share that definition.
-- =====================================================================

-- ============================================
-- Query 4.1 - Monthly cohort retention (cohorts x month offsets 0-11), long format
-- Purpose:      For each cohort (month of first order), the share of its customers
--               who ordered again N months later, for N = 0..11.
-- Inputs:       warehouse.fact_orders (user_id, order_id, date_id, quantity, total_amount),
--               warehouse.dim_date
-- Output columns:
--   cohort_month        DATE     first day of the cohort month
--   month_number        INTEGER  0..11, months after the cohort month
--   retained_customers  BIGINT   cohort customers with an order in that month
--   cohort_size         BIGINT   customers whose first order was in the cohort month
--   retention_pct       NUMERIC  retained_customers / cohort_size * 100, 2 dp
-- Assumptions:
--   - "Retained in month N" = at least one order in the calendar month that is
--     N months after the cohort month (N = 0 is the cohort month itself).
--   - Future months (after the current calendar month) are omitted, not shown as 0%.
--   - Long format was requested for downstream pivoting.
-- Performance notes: order months are reduced to one row per (user, month) before
--   joining, so the join is on user_id against a small set. The 12 offsets are a
--   CROSS JOIN against the cohort list, not a correlated subquery.
-- ORDER BY: cohort_month DESC, month_number ASC.
WITH valid_orders AS (
    SELECT f.user_id, f.order_id, d.date AS order_date
    FROM warehouse.fact_orders f
    JOIN warehouse.dim_date d ON d.date_id = f.date_id
    WHERE f.quantity > 0
      AND f.total_amount >= 0
),
order_months AS (
    SELECT DISTINCT
        user_id,
        DATE_TRUNC('month', order_date)::date AS order_month
    FROM valid_orders
),
first_orders AS (
    SELECT user_id, MIN(order_month) AS cohort_month
    FROM order_months
    GROUP BY user_id
),
cohort_sizes AS (
    SELECT cohort_month, COUNT(*) AS cohort_size
    FROM first_orders
    GROUP BY cohort_month
),
month_offsets AS (
    SELECT generate_series(0, 11) AS month_number
),
retention_raw AS (
    SELECT
        fo.cohort_month,
        (  (EXTRACT(YEAR  FROM om.order_month) * 12 + EXTRACT(MONTH FROM om.order_month))
         - (EXTRACT(YEAR  FROM fo.cohort_month) * 12 + EXTRACT(MONTH FROM fo.cohort_month))
        )::int AS month_number,
        COUNT(DISTINCT om.user_id) AS retained_customers
    FROM first_orders fo
    JOIN order_months om ON om.user_id = fo.user_id
    GROUP BY fo.cohort_month, 2
)
SELECT
    cs.cohort_month,
    mo.month_number,
    COALESCE(r.retained_customers, 0)                                      AS retained_customers,
    cs.cohort_size,
    ROUND(COALESCE(r.retained_customers, 0)::numeric / cs.cohort_size * 100, 2) AS retention_pct
FROM cohort_sizes cs
CROSS JOIN month_offsets mo
LEFT JOIN retention_raw r
       ON r.cohort_month = cs.cohort_month
      AND r.month_number = mo.month_number
WHERE (cs.cohort_month + make_interval(months => mo.month_number))::date
      <= DATE_TRUNC('month', CURRENT_DATE)::date
ORDER BY cs.cohort_month DESC, mo.month_number ASC;

-- ============================================
-- Query 4.2 - Customer lifetime value (LTV) by cohort
-- Purpose:      Lifetime revenue, order frequency and LTV per cohort, including the
--               median customer (robust to a few very large customers).
-- Inputs:       warehouse.fact_orders, warehouse.dim_date
-- Output columns:
--   cohort_month              DATE     first day of the cohort month
--   cohort_size               BIGINT   customers in the cohort
--   total_revenue             NUMERIC  lifetime revenue of the cohort to date
--   avg_ltv_per_customer      NUMERIC  total_revenue / cohort_size, 2 dp
--   avg_orders_per_customer   NUMERIC  mean distinct orders per customer, 2 dp
--   median_ltv_per_customer   NUMERIC  median of per-customer lifetime revenue, 2 dp
-- Assumptions:
--   - LTV is revenue to date (no discounting, no horizon).
-- Performance notes: customer_base is computed once and used by both the mean and
--   the median aggregation. PERCENTILE_CONT needs a sort per cohort, which is cheap
--   at the cohort count.
-- ORDER BY: cohort_month DESC.
WITH customer_base AS (
    -- One row per customer: first-order month, lifetime revenue, distinct orders
    SELECT
        f.user_id,
        MIN(DATE_TRUNC('month', d.date))::date AS cohort_month,
        SUM(f.total_amount)                    AS customer_revenue,
        COUNT(DISTINCT f.order_id)             AS customer_orders
    FROM warehouse.fact_orders f
    JOIN warehouse.dim_date d ON d.date_id = f.date_id
    WHERE f.quantity > 0
      AND f.total_amount >= 0
    GROUP BY f.user_id
),
cohort_totals AS (
    SELECT
        cohort_month,
        COUNT(*)                          AS cohort_size,
        SUM(customer_revenue)             AS total_revenue,
        AVG(customer_orders::numeric)     AS avg_orders_per_customer
    FROM customer_base
    GROUP BY cohort_month
),
cohort_median AS (
    SELECT
        cohort_month,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY customer_revenue) AS median_ltv_per_customer
    FROM customer_base
    GROUP BY cohort_month
)
SELECT
    ct.cohort_month,
    ct.cohort_size,
    ROUND(ct.total_revenue, 2)                                  AS total_revenue,
    ROUND(ct.total_revenue / ct.cohort_size, 2)                 AS avg_ltv_per_customer,
    ROUND(ct.avg_orders_per_customer, 2)                        AS avg_orders_per_customer,
    ROUND(cm.median_ltv_per_customer::numeric, 2)               AS median_ltv_per_customer
FROM cohort_totals ct
JOIN cohort_median cm ON cm.cohort_month = ct.cohort_month
ORDER BY ct.cohort_month DESC;

-- ============================================
-- Query 4.3 - Churned customers (last order 90 to 180 days ago)
-- Purpose:      Customers who have gone quiet: their most recent order is 90 to 180
--               days old. A candidate list for win-back campaigns.
-- Inputs:       warehouse.fact_orders, warehouse.dim_date, warehouse.dim_users
-- Output columns:
--   user_id                 BIGINT   customer identifier
--   email                   VARCHAR  dim_users.email (PII: restrict access)
--   segment                 TEXT     dim_users.segment, lowercased ('unknown' if NULL)
--   last_order_date         DATE     most recent order date
--   days_since_last_order   INTEGER  CURRENT_DATE - last_order_date
--   lifetime_revenue        NUMERIC  total_amount summed over all orders
--   total_orders            BIGINT   distinct orders
-- Assumptions:
--   - "Most recent order" is the maximum order date in fact_orders.
--   - Bounds are inclusive: 90 and 180 days are churned.
-- Performance notes: one aggregation per customer, then a range filter on the
--   aggregate. The dim_users join runs only on churned customers.
-- ORDER BY: days_since_last_order DESC, user_id ASC.
WITH customer_activity AS (
    SELECT
        f.user_id,
        MAX(d.date)                AS last_order_date,
        SUM(f.total_amount)        AS lifetime_revenue,
        COUNT(DISTINCT f.order_id) AS total_orders
    FROM warehouse.fact_orders f
    JOIN warehouse.dim_date d ON d.date_id = f.date_id
    WHERE f.quantity > 0
      AND f.total_amount >= 0
    GROUP BY f.user_id
),
churned AS (
    SELECT
        ca.user_id,
        ca.last_order_date,
        (CURRENT_DATE - ca.last_order_date)::int AS days_since_last_order,
        ca.lifetime_revenue,
        ca.total_orders
    FROM customer_activity ca
    WHERE (CURRENT_DATE - ca.last_order_date) BETWEEN 90 AND 180
)
SELECT
    c.user_id,
    u.email,
    LOWER(COALESCE(u.segment, 'unknown')) AS segment,
    c.last_order_date,
    c.days_since_last_order,
    ROUND(c.lifetime_revenue, 2)          AS lifetime_revenue,
    c.total_orders
FROM churned c
JOIN warehouse.dim_users u ON u.user_id = c.user_id
ORDER BY c.days_since_last_order DESC, c.user_id ASC;
