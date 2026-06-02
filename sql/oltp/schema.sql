-- ============================================================
-- SQL/OLTP: Stripe Transactional Model (PostgreSQL)
-- ============================================================

-- 1. REFERENCE TABLES
CREATE TABLE countries (
    code        CHAR(2)      PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    region      VARCHAR(50)
);

CREATE TABLE currencies (
    code        CHAR(3)      PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    usd_rate    NUMERIC(18,6) NOT NULL,
    updated_at  TIMESTAMPTZ  DEFAULT now()
);

-- 2. MAIN ENTITIES
CREATE TABLE merchants (
    merchant_id     UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(255) NOT NULL,
    country_code    CHAR(2)      REFERENCES countries(code),
    tier            VARCHAR(20)  CHECK (tier IN ('startup','growth','enterprise')),
    created_at      TIMESTAMPTZ  DEFAULT now(),
    is_active       BOOLEAN      DEFAULT true
);

CREATE TABLE customers (
    customer_id     UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    email           VARCHAR(255) UNIQUE NOT NULL,
    email_hash      CHAR(64),            -- SHA-256 (for analytics without PII)
    country_code    CHAR(2)      REFERENCES countries(code),
    created_at      TIMESTAMPTZ  DEFAULT now()
);

-- 3. TRANSACTIONS FACT TABLE (partitioned)
CREATE TABLE transactions (
    transaction_id  UUID         NOT NULL DEFAULT gen_random_uuid(),
    merchant_id     UUID         NOT NULL REFERENCES merchants(merchant_id),
    customer_id     UUID         NOT NULL REFERENCES customers(customer_id),
    amount          NUMERIC(18,4) NOT NULL CHECK (amount > 0),
    currency        CHAR(3)      REFERENCES currencies(code),
    amount_usd      NUMERIC(18,4),       -- converted by trigger (not detailed here)
    payment_method  VARCHAR(50)  NOT NULL,
    status          VARCHAR(20)  NOT NULL
                    CHECK (status IN ('pending','success','failed','refunded','chargeback')),
    device_type     VARCHAR(20),
    ip_country      CHAR(2),
    fraud_score     NUMERIC(5,4) CHECK (fraud_score BETWEEN 0 AND 1),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (transaction_id, created_at)  -- composite key for partitioning
) PARTITION BY RANGE (created_at);

-- Manual creation of monthly partitions (example for January 2024)
CREATE TABLE transactions_2024_01 PARTITION OF transactions
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
-- (in production, automate with pg_partman or Cron)

-- 4. STRATEGIC INDEXES
-- Merchant queries
CREATE INDEX idx_txn_merchant_date ON transactions (merchant_id, created_at DESC);
-- Customer queries
CREATE INDEX idx_txn_customer_date ON transactions (customer_id, created_at DESC);
-- Pending/failed status monitoring
CREATE INDEX idx_txn_status_pending ON transactions (status, created_at DESC)
    WHERE status IN ('pending', 'failed');
-- Fraud detection: only high scores (partial index)
CREATE INDEX idx_txn_fraud_high ON transactions (fraud_score DESC, created_at DESC)
    WHERE fraud_score > 0.7;

-- 5. IMMUTABLE AUDIT LOG
CREATE TABLE audit_log (
    log_id          BIGSERIAL    PRIMARY KEY,
    table_name      VARCHAR(50)  NOT NULL,
    operation       CHAR(1)      CHECK (operation IN ('I','U','D')),
    record_id       UUID         NOT NULL,
    changed_by      VARCHAR(100),
    changed_at      TIMESTAMPTZ  DEFAULT now(),
    old_values      JSONB,
    new_values      JSONB
);

-- Generic audit trigger for the transactions table
CREATE OR REPLACE FUNCTION fn_audit_trigger()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO audit_log (table_name, operation, record_id, old_values, new_values)
    VALUES (
        TG_TABLE_NAME,
        LEFT(TG_OP, 1),
        COALESCE(NEW.transaction_id, OLD.transaction_id),
        row_to_json(OLD)::jsonb,
        row_to_json(NEW)::jsonb
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply the trigger to the transactions table
CREATE TRIGGER trg_transactions_audit
    AFTER INSERT OR UPDATE OR DELETE ON transactions
    FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger();