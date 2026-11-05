-- =====================================================================
-- 008: correct the dim_users.segment comment
-- =====================================================================
-- Migration 001 documented the segment as 'Gold', 'Silver', 'Bronze', 'New'.
-- The data generator, scripts/fix_dim_users.py and the analytics queries use
-- 'VIP', 'Regular', 'New'. That is the canonical vocabulary (decided in the
-- Phase 10.1 extension). Migration 001 is not edited, so the correction is made
-- here as a comment-only change. It is idempotent and changes no data.
-- =====================================================================

COMMENT ON COLUMN warehouse.dim_users.segment IS
    'Customer segment: VIP (high value), Regular (repeat buyers), New (first 30 days). '
    'Canonical vocabulary is VIP, Regular, New. Migration 001 originally listed Gold, Silver, Bronze, New; that list is superseded.';
