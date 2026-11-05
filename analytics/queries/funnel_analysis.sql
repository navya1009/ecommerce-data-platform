-- =====================================================================
-- funnel_analysis.sql
-- Clickstream funnel and session queries (read-only).
-- Run one query:  psql -h localhost -p 5432 -U admin -d postgres -f <file>
-- Run all:         python analytics/run_all_queries.py
-- Schema facts (after migration 007_fix_fact_clickstream_schema.sql):
--   fact_clickstream has `device`, `session_id`, `event_type`, `product_id`
--   (nullable), `created_at` (event time for current rows), `is_bot`.
--   Bot-flagged events are stored with is_bot = TRUE and excluded here.
-- =====================================================================

-- ============================================
-- Query 2.1 - Conversion funnel: page_view -> add_to_cart -> purchase, by device
-- Purpose:      Event counts at each funnel step per device type, with the
--               conversion rate between steps.
-- Inputs:       warehouse.fact_clickstream (event_type, device)
-- Output columns:
--   device_type                  TEXT     fact_clickstream.device ('unknown' if NULL)
--   page_views                   BIGINT   count of page_view events
--   add_to_carts                 BIGINT   count of add_to_cart events
--   purchases                    BIGINT   count of purchase events
--   pct_add_to_cart_from_view    NUMERIC  add_to_carts / page_views * 100, 2 dp
--   pct_purchase_from_cart       NUMERIC  purchases / add_to_carts * 100, 2 dp
--   pct_purchase_from_view       NUMERIC  purchases / page_views * 100, 2 dp
-- Assumptions:
--   - Purchases are counted from fact_clickstream purchase events, not from
--     fact_orders (event-level attribution, as specified).
--   - Counts are events, not sessions.
--   - Percentages are NULL when the denominator is zero.
-- Performance notes: single scan with event_type filter; conditional COUNT
--   FILTER avoids three separate passes.
-- ORDER BY: device_type ASC.
WITH funnel_events AS (
    SELECT
        COALESCE(c.device, 'unknown') AS device_type,
        c.event_type
    FROM warehouse.fact_clickstream c
    WHERE NOT c.is_bot
      AND c.event_type IN ('page_view', 'add_to_cart', 'purchase')
),
funnel_counts AS (
    SELECT
        device_type,
        COUNT(*) FILTER (WHERE event_type = 'page_view')   AS page_views,
        COUNT(*) FILTER (WHERE event_type = 'add_to_cart') AS add_to_carts,
        COUNT(*) FILTER (WHERE event_type = 'purchase')    AS purchases
    FROM funnel_events
    GROUP BY device_type
)
SELECT
    device_type,
    page_views,
    add_to_carts,
    purchases,
    ROUND(add_to_carts::numeric / NULLIF(page_views, 0) * 100, 2) AS pct_add_to_cart_from_view,
    ROUND(purchases::numeric    / NULLIF(add_to_carts, 0) * 100, 2) AS pct_purchase_from_cart,
    ROUND(purchases::numeric    / NULLIF(page_views, 0) * 100, 2) AS pct_purchase_from_view
FROM funnel_counts
ORDER BY device_type ASC;

-- ============================================
-- Query 2.2 - Product categories with high add-to-cart volume and their purchase rate
-- Purpose:      Categories that attract cart activity, and how many of those
--               carts convert to purchases. Small categories are excluded.
-- Inputs:       warehouse.fact_clickstream, warehouse.dim_products (category)
-- Output columns:
--   category                   TEXT     dim_products.category ('Uncategorized' if NULL)
--   add_to_carts               BIGINT   add_to_cart events for the category
--   purchases                  BIGINT   purchase events for the category
--   cart_to_purchase_rate_pct  NUMERIC  purchases / add_to_carts * 100, 2 dp
-- Assumptions:
--   - Only events with a product_id are attributed to a category.
--   - Categories with fewer than 100 add_to_cart events are dropped (noise filter).
-- Performance notes: join on product_id (indexed on fact_clickstream), one
--   aggregation pass, HAVING-equivalent filter applied after aggregation.
-- ORDER BY: add_to_carts DESC, category ASC. LIMIT 20.
WITH category_events AS (
    SELECT
        COALESCE(p.category, 'Uncategorized') AS category,
        c.event_type
    FROM warehouse.fact_clickstream c
    JOIN warehouse.dim_products p ON p.product_id = c.product_id
    WHERE NOT c.is_bot
      AND c.event_type IN ('add_to_cart', 'purchase')
),
category_counts AS (
    SELECT
        category,
        COUNT(*) FILTER (WHERE event_type = 'add_to_cart') AS add_to_carts,
        COUNT(*) FILTER (WHERE event_type = 'purchase')    AS purchases
    FROM category_events
    GROUP BY category
)
SELECT
    category,
    add_to_carts,
    purchases,
    ROUND(purchases::numeric / NULLIF(add_to_carts, 0) * 100, 2) AS cart_to_purchase_rate_pct
FROM category_counts
WHERE add_to_carts >= 100
ORDER BY add_to_carts DESC, category ASC
LIMIT 20;

-- ============================================
-- Query 2.3 - Session analysis: duration, pages per session, bounce rate, by device
-- Purpose:      Per-device session health: how long sessions last, how many pages
--               they view, and how many sessions bounce after a single event.
-- Inputs:       warehouse.fact_clickstream (session_id, event_type, device, created_at)
-- Output columns:
--   device_type                  TEXT     fact_clickstream.device ('unknown' if NULL)
--   total_sessions               BIGINT   distinct sessions
--   avg_session_duration_seconds NUMERIC  mean of (last event - first event), seconds
--   avg_pages_per_session        NUMERIC  mean page_view events per session
--   bounce_rate_pct              NUMERIC  % of sessions with fewer than 2 events
-- Assumptions:
--   - A session is one session_id. Its device is the device of its events.
--   - Event time is fact_clickstream.created_at. The loader does not store a
--     separate event timestamp, so created_at is the load time. Durations are
--     therefore only as accurate as the load cadence. Treat them as indicative.
--   - A bounce is a session with exactly one event (the brief's "< 2 events").
-- Performance notes: one grouping pass over fact_clickstream by session_id;
--   the session rollup is then aggregated by device.
-- ORDER BY: device_type ASC.
WITH session_stats AS (
    SELECT
        c.session_id,
        COALESCE(c.device, 'unknown')                       AS device_type,
        MIN(c.created_at)                                   AS first_event_at,
        MAX(c.created_at)                                   AS last_event_at,
        COUNT(*)                                            AS total_events,
        COUNT(*) FILTER (WHERE c.event_type = 'page_view')  AS page_views
    FROM warehouse.fact_clickstream c
    WHERE NOT c.is_bot
    GROUP BY c.session_id, COALESCE(c.device, 'unknown')
)
SELECT
    device_type,
    COUNT(*)                                                       AS total_sessions,
    ROUND(AVG(EXTRACT(EPOCH FROM (last_event_at - first_event_at))), 2) AS avg_session_duration_seconds,
    ROUND(AVG(page_views::numeric), 2)                             AS avg_pages_per_session,
    ROUND(COUNT(*) FILTER (WHERE total_events < 2)::numeric
          / NULLIF(COUNT(*), 0) * 100, 2)                          AS bounce_rate_pct
FROM session_stats
GROUP BY device_type
ORDER BY device_type ASC;
