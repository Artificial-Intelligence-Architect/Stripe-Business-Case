-- ============================================================
-- SQL/Security: RBAC Configuration (PostgreSQL)
-- ============================================================

-- Create group roles (without login)
-- These serve as permission templates for end users.
CREATE ROLE analyst_read;
CREATE ROLE engineer_write;
CREATE ROLE compliance_officer;
CREATE ROLE ml_service;

-- (Optional) Example of creating users inheriting the roles
-- CREATE USER john_analyst WITH PASSWORD 'change_me' IN ROLE analyst_read;
-- CREATE USER jane_engineer WITH PASSWORD 'change_me' IN ROLE engineer_write;

-- ============================================================
-- 1. analyst_read role: business reads (excluding PII)
-- ============================================================
GRANT SELECT ON countries, currencies TO analyst_read;

-- Merchants: authorised for consultation
GRANT SELECT ON merchants TO analyst_read;

-- Transactions: global access, but the sensitive customer_id column is removed
GRANT SELECT ON transactions TO analyst_read;
REVOKE SELECT (customer_id) ON transactions FROM analyst_read;

-- Customers: no direct access (contains emails, hash)
-- (no permissions granted on this table)

-- ============================================================
-- 2. engineer_write role: write operations on core business entities
-- ============================================================
GRANT SELECT, INSERT, UPDATE ON transactions, merchants, customers TO engineer_write;
GRANT SELECT ON countries, currencies TO engineer_write;

-- ============================================================
-- 3. compliance_officer role: full read access
--    (including audit logs)
-- ============================================================
GRANT SELECT ON ALL TABLES IN SCHEMA public TO compliance_officer;
-- Ensure the role can read future tables (via configuration)
-- ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO compliance_officer;

-- ============================================================
-- 4. ml_service role: strictly necessary access for ML
-- ============================================================
-- For the transactions table, only the transaction_id and fraud_score columns are exposed.
GRANT SELECT (transaction_id, fraud_score) ON transactions TO ml_service;
-- No other access (no merchants, customers, or other columns).

-- ============================================================
-- Checks (optional): to test the permissions
-- ============================================================
-- SET ROLE analyst_read;
-- SELECT * FROM transactions LIMIT 1; -- should fail on customer_id
-- RESET ROLE;