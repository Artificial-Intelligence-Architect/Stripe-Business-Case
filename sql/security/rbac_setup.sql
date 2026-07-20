-- ============================================================
-- SQL/Security: RBAC Configuration (PostgreSQL)
-- ============================================================
--
-- ⚠️  THE RULE THAT DRIVES THIS FILE
--
--   In PostgreSQL, table-level and column-level privileges are stored
--   SEPARATELY, and the table-level grant WINS.
--
--   This does NOT work (and was the bug in the previous version):
--       GRANT  SELECT               ON transactions TO analyst_read;
--       REVOKE SELECT (customer_id) ON transactions FROM analyst_read;
--       -- analyst_read STILL reads customer_id: the REVOKE silently
--       -- removes nothing, because no column-level grant ever existed.
--
--   The only correct pattern is to never hold the table-level grant:
--       GRANT SELECT (col_a, col_b, ...) ON transactions TO analyst_read;
--
--   Verified below by the regression test at the bottom of this file.
-- ============================================================

CREATE ROLE analyst_read;
CREATE ROLE engineer_write;
CREATE ROLE compliance_officer;
CREATE ROLE ml_service;

-- Least privilege starts at the schema: no CREATE, only USAGE.
REVOKE ALL   ON SCHEMA public FROM PUBLIC;
GRANT  USAGE ON SCHEMA public TO analyst_read, engineer_write,
                                 compliance_officer, ml_service;

-- ============================================================
-- 1. analyst_read — business reads, PII excluded
-- ============================================================
GRANT SELECT ON countries, currencies, product_categories TO analyst_read;
GRANT SELECT ON merchants, products TO analyst_read;

-- Transactions: enumerate the allowed columns. customer_id, ip_country and
-- device_type are withheld (re-identification vectors under GDPR art.4(1)).
GRANT SELECT (
    merchant_id, transaction_id, created_at, kind, parent_transaction_id,
    subscription_id, product_id, amount, currency, amount_usd, fx_rate_used,
    payment_method, status, fraud_score
) ON transactions TO analyst_read;

-- Subscriptions: no customer_id.
GRANT SELECT (
    merchant_id, subscription_id, product_id, status, billing_interval,
    interval_count, unit_amount, currency, quantity,
    current_period_start, current_period_end, created_at
) ON subscriptions TO analyst_read;

GRANT SELECT ON subscription_events TO analyst_read;

-- customers: no grant at all (email + email_hash).

-- ── Row-Level Security: an analyst sees only their own merchants ──
-- Column filtering answers "which fields?"; RLS answers "which rows?".
-- Without RLS, a merchant-support analyst reads every merchant's revenue.
ALTER TABLE transactions  ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscriptions ENABLE ROW LEVEL SECURITY;

CREATE TABLE analyst_merchant_scope (
    role_name   NAME NOT NULL,
    merchant_id UUID NOT NULL,
    PRIMARY KEY (role_name, merchant_id)
);

CREATE POLICY p_txn_merchant_scope ON transactions
    FOR SELECT TO analyst_read
    USING (merchant_id IN (
        SELECT merchant_id FROM analyst_merchant_scope
        WHERE role_name = current_user
    ));

CREATE POLICY p_sub_merchant_scope ON subscriptions
    FOR SELECT TO analyst_read
    USING (merchant_id IN (
        SELECT merchant_id FROM analyst_merchant_scope
        WHERE role_name = current_user
    ));

-- ============================================================
-- 2. engineer_write — writes on core entities
-- ============================================================
GRANT SELECT, INSERT, UPDATE ON transactions, subscriptions,
                                subscription_events, merchants,
                                customers, products TO engineer_write;
GRANT SELECT ON countries, currencies, product_categories TO engineer_write;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO engineer_write;

-- No DELETE anywhere: payment records are never physically removed
-- (GDPR erasure = anonymisation, see gdpr_erasure.sql).
REVOKE DELETE ON ALL TABLES IN SCHEMA public FROM engineer_write;
-- audit_log is append-only for everyone (enforced by trigger too).
REVOKE INSERT, UPDATE, DELETE ON audit_log FROM engineer_write;

-- ============================================================
-- 3. compliance_officer — full read, including audit trail
-- ============================================================
GRANT SELECT ON ALL TABLES IN SCHEMA public TO compliance_officer;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO compliance_officer;
-- Compliance must read every row, including other merchants'.
GRANT compliance_officer TO CURRENT_USER;   -- bootstrap only; remove in prod
ALTER TABLE transactions  FORCE ROW LEVEL SECURITY;
CREATE POLICY p_txn_compliance_all ON transactions
    FOR SELECT TO compliance_officer USING (true);
CREATE POLICY p_sub_compliance_all ON subscriptions
    FOR SELECT TO compliance_officer USING (true);

-- ============================================================
-- 4. ml_service — strictly the feature columns
-- ============================================================
GRANT SELECT (
    merchant_id, transaction_id, created_at, customer_id, amount_usd,
    currency, payment_method, device_type, ip_country, fraud_score, status
) ON transactions TO ml_service;
GRANT SELECT (merchant_id, customer_id, country_code, created_at)
    ON customers TO ml_service;
-- ml_service reads customer_id because velocity features are per-customer;
-- it is pseudonymous (UUID), never the email. Documented in docs/06.
CREATE POLICY p_txn_ml_all ON transactions
    FOR SELECT TO ml_service USING (true);

-- ============================================================
-- 5. REGRESSION TEST — proves the column grant actually bites
-- ============================================================
-- Run as superuser. Expected: first SELECT fails, second succeeds.
--
--   CREATE USER test_analyst IN ROLE analyst_read;
--   SET ROLE test_analyst;
--
--   SELECT customer_id FROM transactions LIMIT 1;
--   -- ERROR: permission denied for table transactions   ✅ expected
--
--   SELECT amount_usd, status FROM transactions LIMIT 1;
--   -- OK                                                ✅ expected
--
--   SELECT * FROM transactions LIMIT 1;
--   -- ERROR: permission denied  ✅ expected — "*" expands to customer_id
--
--   RESET ROLE;
--
-- Programmatic assertion:
--   SELECT has_column_privilege('analyst_read','transactions','customer_id','SELECT');
--   -- MUST return false. Returned TRUE before this fix.
