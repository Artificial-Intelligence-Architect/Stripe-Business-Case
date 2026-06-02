-- ============================================================
-- PostgreSQL — CDC Prerequisites for Debezium
-- Run once as superuser BEFORE deploying the connector
-- ============================================================

-- ── 1. Enable logical replication (requires restart) ─────────
-- In postgresql.conf (or via ALTER SYSTEM):
--   wal_level = logical
--   max_replication_slots = 5   (1 per connector + headroom)
--   max_wal_senders = 5

ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_replication_slots = 5;
ALTER SYSTEM SET max_wal_senders = 5;
-- Reload required: SELECT pg_reload_conf();
-- Full restart required for wal_level change.

-- ── 2. Dedicated CDC role (least-privilege) ───────────────────
CREATE ROLE cdc_user WITH
    LOGIN
    REPLICATION                   -- required for replication slots
    PASSWORD '${CDC_PASSWORD}';   -- inject via secrets manager

-- Read access on captured tables
GRANT SELECT ON public.transactions TO cdc_user;
GRANT SELECT ON public.merchants    TO cdc_user;
GRANT SELECT ON public.customers    TO cdc_user;

-- Access to replication catalog
GRANT USAGE ON SCHEMA public TO cdc_user;

-- ── 3. Publication (pgoutput plugin) ─────────────────────────
-- Scoped to the three tables — no full-database publication.
-- Captures INSERT, UPDATE, DELETE (not TRUNCATE).
CREATE PUBLICATION stripe_cdc_pub
    FOR TABLE public.transactions,
              public.merchants,
              public.customers
    WITH (publish = 'insert, update, delete');

-- ── 4. Replication slot (created by Debezium on first start) ─
-- Listed here for documentation; Debezium creates it automatically
-- using slot.name = 'stripe_debezium_slot' in the connector config.
-- To create manually for pre-validation:
-- SELECT pg_create_logical_replication_slot(
--     'stripe_debezium_slot', 'pgoutput'
-- );

-- ── 5. Verify setup ──────────────────────────────────────────
-- Check publication:
SELECT pubname, pubtables FROM pg_publication_tables
WHERE pubname = 'stripe_cdc_pub';

-- Check replication slot (after connector first start):
SELECT slot_name, plugin, active, restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots
WHERE slot_name = 'stripe_debezium_slot';

-- Check WAL lag (monitor in production — alert if > 100 MB):
SELECT slot_name,
       pg_size_pretty(
           pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)
       ) AS wal_lag
FROM pg_replication_slots
WHERE slot_name = 'stripe_debezium_slot';
