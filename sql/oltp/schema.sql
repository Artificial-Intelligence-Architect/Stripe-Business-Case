-- ============================================================
-- SQL/OLTP: Stripe Transactional Model (PostgreSQL 15 + Citus 12)
-- ============================================================
--
-- DESIGN CONTRACT
-- ---------------
-- Distribution key : merchant_id  (every distributed table)
-- Partition key    : created_at   (time-series tables only)
-- Colocation group : merchants, customers, products, subscriptions,
--                    subscription_events, transactions, audit_log
--
-- Citus rule that drives the whole schema:
--   "Every UNIQUE / PRIMARY KEY constraint on a distributed table
--    MUST contain the distribution column."
--   => PK(transactions) = (merchant_id, transaction_id, created_at)
--                          ^^^^^^^^^^^  distribution   ^^^^^^^^^^ partition
--
-- Consequence for FKs:
--   distributed -> reference      : allowed
--   distributed -> distributed    : allowed ONLY if colocated AND the FK
--                                   carries the distribution column
--   => every FK below starts with merchant_id.
--
-- Domain justification for distributing customers by merchant_id:
--   In Stripe's model a Customer object belongs to an Account (merchant);
--   the same physical person paying two merchants is two Customer objects.
--   Sharding customers by merchant_id is therefore not a hack — it mirrors
--   the domain, and it makes merchant-scoped joins single-shard.
-- ============================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;   -- gen_random_uuid()
-- CREATE EXTENSION IF NOT EXISTS citus;   -- enable on the coordinator node

-- ============================================================
-- 1. REFERENCE TABLES  (replicated to every node)
-- ============================================================

CREATE TABLE countries (
    code        CHAR(2)      PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    region      VARCHAR(50)
);

CREATE TABLE currencies (
    code        CHAR(3)       PRIMARY KEY,
    name        VARCHAR(100)  NOT NULL,
    usd_rate    NUMERIC(18,6) NOT NULL,
    updated_at  TIMESTAMPTZ   DEFAULT now()
);

CREATE TABLE product_categories (
    category_code   VARCHAR(30)  PRIMARY KEY,
    label           VARCHAR(100) NOT NULL,
    mcc             CHAR(4)                    -- Merchant Category Code (ISO 18245)
);

-- SELECT create_reference_table('countries');
-- SELECT create_reference_table('currencies');
-- SELECT create_reference_table('product_categories');

-- ============================================================
-- 2. MAIN ENTITIES  (distributed by merchant_id)
-- ============================================================

CREATE TABLE merchants (
    merchant_id     UUID         NOT NULL DEFAULT gen_random_uuid(),
    name            VARCHAR(255) NOT NULL,
    country_code    CHAR(2)      REFERENCES countries(code),
    tier            VARCHAR(20)  CHECK (tier IN ('startup','growth','enterprise')),
    created_at      TIMESTAMPTZ  DEFAULT now(),
    is_active       BOOLEAN      DEFAULT true,
    PRIMARY KEY (merchant_id)
);

CREATE TABLE customers (
    merchant_id     UUID         NOT NULL,
    customer_id     UUID         NOT NULL DEFAULT gen_random_uuid(),
    email           VARCHAR(255) NOT NULL,
    email_hash      CHAR(64),                  -- SHA-256, PII-free analytics join key
    country_code    CHAR(2)      REFERENCES countries(code),
    ccpa_opt_out    BOOLEAN      NOT NULL DEFAULT false,
    is_erased       BOOLEAN      NOT NULL DEFAULT false,   -- GDPR art.17 tombstone
    created_at      TIMESTAMPTZ  DEFAULT now(),
    PRIMARY KEY (merchant_id, customer_id),
    -- Email is unique PER MERCHANT, not globally: the same person may be a
    -- customer of two merchants. A global UNIQUE would also be impossible
    -- under Citus (it would not contain the distribution column).
    UNIQUE (merchant_id, email),
    FOREIGN KEY (merchant_id) REFERENCES merchants(merchant_id)
);

-- ── Product catalog (Reference Data in the brief; merchant-scoped in reality)
CREATE TABLE products (
    merchant_id     UUID          NOT NULL,
    product_id      UUID          NOT NULL DEFAULT gen_random_uuid(),
    name            VARCHAR(255)  NOT NULL,
    category_code   VARCHAR(30)   REFERENCES product_categories(category_code),
    unit_amount     NUMERIC(18,4) CHECK (unit_amount >= 0),
    currency        CHAR(3)       REFERENCES currencies(code),
    is_recurring    BOOLEAN       NOT NULL DEFAULT false,
    is_active       BOOLEAN       NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ   DEFAULT now(),
    updated_at      TIMESTAMPTZ   DEFAULT now(),
    PRIMARY KEY (merchant_id, product_id),
    FOREIGN KEY (merchant_id) REFERENCES merchants(merchant_id)
);

-- ============================================================
-- 3. SUBSCRIPTIONS  (explicitly required by the brief:
--    "payments, refunds, chargebacks, and subscription management")
-- ============================================================

CREATE TABLE subscriptions (
    merchant_id           UUID        NOT NULL,
    subscription_id       UUID        NOT NULL DEFAULT gen_random_uuid(),
    customer_id           UUID        NOT NULL,
    product_id            UUID        NOT NULL,
    status                VARCHAR(20) NOT NULL
                          CHECK (status IN ('trialing','active','past_due',
                                            'canceled','unpaid','paused')),
    billing_interval      VARCHAR(10) NOT NULL
                          CHECK (billing_interval IN ('day','week','month','year')),
    interval_count        SMALLINT    NOT NULL DEFAULT 1 CHECK (interval_count > 0),
    unit_amount           NUMERIC(18,4) NOT NULL CHECK (unit_amount >= 0),
    currency              CHAR(3)     REFERENCES currencies(code),
    quantity              INT         NOT NULL DEFAULT 1 CHECK (quantity > 0),
    current_period_start  TIMESTAMPTZ NOT NULL,
    current_period_end    TIMESTAMPTZ NOT NULL,
    trial_end             TIMESTAMPTZ,
    cancel_at_period_end  BOOLEAN     NOT NULL DEFAULT false,
    canceled_at           TIMESTAMPTZ,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (merchant_id, subscription_id),
    FOREIGN KEY (merchant_id, customer_id) REFERENCES customers(merchant_id, customer_id),
    FOREIGN KEY (merchant_id, product_id)  REFERENCES products(merchant_id, product_id),
    CHECK (current_period_end > current_period_start),
    -- A canceled subscription must carry its cancellation timestamp.
    CHECK (status <> 'canceled' OR canceled_at IS NOT NULL)
);

-- Append-only status history. This is what makes MRR / churn / cohort
-- analysis possible downstream — without it the OLAP layer can only see
-- the CURRENT status and churn becomes unmeasurable.
CREATE TABLE subscription_events (
    merchant_id     UUID        NOT NULL,
    event_id        BIGSERIAL   NOT NULL,
    subscription_id UUID        NOT NULL,
    event_type      VARCHAR(30) NOT NULL
                    CHECK (event_type IN ('created','trial_started','trial_ended',
                                          'activated','renewed','upgraded','downgraded',
                                          'payment_failed','paused','resumed','canceled')),
    from_status     VARCHAR(20),
    to_status       VARCHAR(20),
    mrr_delta_usd   NUMERIC(18,4),          -- signed: +upgrade / -downgrade / -churn
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (merchant_id, event_id),
    FOREIGN KEY (merchant_id, subscription_id)
        REFERENCES subscriptions(merchant_id, subscription_id)
);

CREATE INDEX idx_sub_events_sub_time
    ON subscription_events (merchant_id, subscription_id, occurred_at DESC);
CREATE INDEX idx_sub_status_period_end
    ON subscriptions (merchant_id, status, current_period_end)
    WHERE status IN ('active','trialing','past_due');

-- ============================================================
-- 4. TRANSACTIONS  (distributed by merchant_id, partitioned by created_at)
-- ============================================================

CREATE TABLE transactions (
    merchant_id            UUID          NOT NULL,
    transaction_id         UUID          NOT NULL DEFAULT gen_random_uuid(),
    created_at             TIMESTAMPTZ   NOT NULL DEFAULT now(),
    customer_id            UUID          NOT NULL,
    -- Refunds and chargebacks are transactions that POINT BACK to the
    -- original payment. Modelling them as a status alone loses the link
    -- (which payment was refunded? how much of it?).
    kind                   VARCHAR(20)   NOT NULL DEFAULT 'payment'
                           CHECK (kind IN ('payment','refund','chargeback',
                                           'chargeback_reversal')),
    parent_transaction_id  UUID,
    parent_created_at      TIMESTAMPTZ,
    subscription_id        UUID,                       -- NULL for one-off payments
    product_id             UUID,
    amount                 NUMERIC(18,4) NOT NULL CHECK (amount > 0),
    currency               CHAR(3)       REFERENCES currencies(code),
    amount_usd             NUMERIC(18,4),
    fx_rate_used           NUMERIC(18,6),              -- frozen at write time
    payment_method         VARCHAR(50)   NOT NULL,
    status                 VARCHAR(20)   NOT NULL
                           CHECK (status IN ('pending','success','failed',
                                             'refunded','partially_refunded',
                                             'chargeback')),
    device_type            VARCHAR(20),
    ip_country             CHAR(2),
    fraud_score            NUMERIC(5,4)  CHECK (fraud_score BETWEEN 0 AND 1),

    PRIMARY KEY (merchant_id, transaction_id, created_at),

    FOREIGN KEY (merchant_id, customer_id)
        REFERENCES customers(merchant_id, customer_id),
    FOREIGN KEY (merchant_id, subscription_id)
        REFERENCES subscriptions(merchant_id, subscription_id),
    FOREIGN KEY (merchant_id, product_id)
        REFERENCES products(merchant_id, product_id),

    -- A refund/chargeback MUST reference its parent payment; a payment must not.
    CHECK (
        (kind = 'payment'  AND parent_transaction_id IS NULL)
        OR
        (kind <> 'payment' AND parent_transaction_id IS NOT NULL
                           AND parent_created_at     IS NOT NULL)
    )
) PARTITION BY RANGE (created_at);

-- NOTE — why parent_transaction_id has no FOREIGN KEY:
--   A self-referencing FK onto a PARTITIONED table is rejected by PostgreSQL
--   ("cannot reference partitioned table"), and Citus does not support
--   self-FKs on distributed partitioned tables either. The link is therefore
--   enforced by:
--     (a) the CHECK constraint above (presence),
--     (b) fn_validate_parent_txn() below (existence + amount ceiling),
--     (c) a dbt test `refund_has_parent` in the OLAP layer (detective control).
--   This is a deliberate, documented trade-off, not an oversight.

-- ── Partitions ───────────────────────────────────────────────
CREATE TABLE transactions_2024_01 PARTITION OF transactions
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE transactions_2024_02 PARTITION OF transactions
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
-- DEFAULT partition = safety net. Without it, an INSERT outside every
-- declared range raises "no partition of relation found for row" and the
-- payment is REJECTED. Losing a payment is worse than storing it unpruned.
CREATE TABLE transactions_default PARTITION OF transactions DEFAULT;
-- Production: pg_partman creates partitions M+3 in advance and detaches M-24.
--   SELECT partman.create_parent('public.transactions', 'created_at',
--                                'native', 'monthly', p_premake := 3);

-- ── Citus distribution (run AFTER table creation, BEFORE loading) ──
-- SELECT create_distributed_table('merchants',           'merchant_id');
-- SELECT create_distributed_table('customers',           'merchant_id', colocate_with => 'merchants');
-- SELECT create_distributed_table('products',            'merchant_id', colocate_with => 'merchants');
-- SELECT create_distributed_table('subscriptions',       'merchant_id', colocate_with => 'merchants');
-- SELECT create_distributed_table('subscription_events', 'merchant_id', colocate_with => 'merchants');
-- SELECT create_distributed_table('transactions',        'merchant_id', colocate_with => 'merchants');
-- SELECT create_distributed_table('audit_log',           'merchant_id', colocate_with => 'merchants');

-- ============================================================
-- 5. STRATEGIC INDEXES
-- ============================================================
CREATE INDEX idx_txn_merchant_date   ON transactions (merchant_id, created_at DESC);
CREATE INDEX idx_txn_customer_date   ON transactions (merchant_id, customer_id, created_at DESC);
CREATE INDEX idx_txn_subscription    ON transactions (merchant_id, subscription_id, created_at DESC)
    WHERE subscription_id IS NOT NULL;
CREATE INDEX idx_txn_parent          ON transactions (merchant_id, parent_transaction_id)
    WHERE parent_transaction_id IS NOT NULL;
CREATE INDEX idx_txn_status_pending  ON transactions (merchant_id, status, created_at DESC)
    WHERE status IN ('pending','failed');
CREATE INDEX idx_txn_fraud_high      ON transactions (merchant_id, fraud_score DESC, created_at DESC)
    WHERE fraud_score > 0.7;

-- ============================================================
-- 6. FX CONVERSION — freeze the rate at write time
-- ============================================================
CREATE OR REPLACE FUNCTION fn_set_amount_usd()
RETURNS TRIGGER AS $$
DECLARE
    v_rate NUMERIC(18,6);
BEGIN
    IF NEW.amount_usd IS NULL THEN
        SELECT usd_rate INTO v_rate FROM currencies WHERE code = NEW.currency;
        IF v_rate IS NULL THEN
            RAISE EXCEPTION 'Unknown currency %', NEW.currency;
        END IF;
        NEW.fx_rate_used := v_rate;
        NEW.amount_usd   := ROUND(NEW.amount * v_rate, 4);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_transactions_fx
    BEFORE INSERT ON transactions
    FOR EACH ROW EXECUTE FUNCTION fn_set_amount_usd();

-- ============================================================
-- 7. REFUND / CHARGEBACK INTEGRITY (replaces the impossible self-FK)
-- ============================================================
CREATE OR REPLACE FUNCTION fn_validate_parent_txn()
RETURNS TRIGGER AS $$
DECLARE
    v_parent_amount   NUMERIC(18,4);
    v_already_refunded NUMERIC(18,4);
BEGIN
    IF NEW.kind = 'payment' THEN
        RETURN NEW;
    END IF;

    SELECT amount INTO v_parent_amount
    FROM transactions
    WHERE merchant_id    = NEW.merchant_id
      AND transaction_id = NEW.parent_transaction_id
      AND created_at     = NEW.parent_created_at
      AND kind           = 'payment';

    IF v_parent_amount IS NULL THEN
        RAISE EXCEPTION 'Parent payment %/% not found for % %',
            NEW.parent_transaction_id, NEW.parent_created_at, NEW.kind, NEW.transaction_id;
    END IF;

    -- A refund can never exceed what is left of the original payment.
    SELECT COALESCE(SUM(amount), 0) INTO v_already_refunded
    FROM transactions
    WHERE merchant_id           = NEW.merchant_id
      AND parent_transaction_id = NEW.parent_transaction_id
      AND parent_created_at     = NEW.parent_created_at
      AND kind                  IN ('refund','chargeback')
      AND status                <> 'failed';

    IF v_already_refunded + NEW.amount > v_parent_amount THEN
        RAISE EXCEPTION 'Refund overflow: % + % > % on parent %',
            v_already_refunded, NEW.amount, v_parent_amount, NEW.parent_transaction_id;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_transactions_parent_check
    BEFORE INSERT ON transactions
    FOR EACH ROW EXECUTE FUNCTION fn_validate_parent_txn();

-- ============================================================
-- 8. IMMUTABLE AUDIT LOG
-- ============================================================
CREATE TABLE audit_log (
    merchant_id     UUID         NOT NULL,       -- distribution column (colocated)
    log_id          BIGSERIAL    NOT NULL,
    table_name      VARCHAR(50)  NOT NULL,
    operation       CHAR(1)      NOT NULL CHECK (operation IN ('I','U','D')),
    record_id       UUID         NOT NULL,
    reason          VARCHAR(50),                 -- promoted out of new_values:
                                                 -- compliance jobs filter on it
    changed_by      VARCHAR(100) DEFAULT current_user,
    changed_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    old_values      JSONB,
    new_values      JSONB,
    PRIMARY KEY (merchant_id, log_id)
);

CREATE INDEX idx_audit_reason_time ON audit_log (reason, changed_at DESC)
    WHERE reason IS NOT NULL;
CREATE INDEX idx_audit_record      ON audit_log (merchant_id, table_name, record_id);

-- Append-only enforcement: an audit trail that can be UPDATEd is not an audit trail.
CREATE OR REPLACE FUNCTION fn_audit_immutable()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'audit_log is append-only (attempted %)', TG_OP;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_audit_no_update_delete
    BEFORE UPDATE OR DELETE ON audit_log
    FOR EACH ROW EXECUTE FUNCTION fn_audit_immutable();

-- ── The audit trigger itself ─────────────────────────────────
-- BUG FIXED (was blocking every INSERT on transactions):
--   the previous version called row_to_json(OLD) on INSERT and
--   NEW.transaction_id on DELETE. In PL/pgSQL, OLD is unassigned during
--   INSERT and NEW is unassigned during DELETE — touching either raises
--   `record "old" is not assigned yet`. Every write to the core payments
--   table therefore aborted. TG_OP branching is mandatory.
CREATE OR REPLACE FUNCTION fn_audit_trigger()
RETURNS TRIGGER AS $$
DECLARE
    v_pk_col      TEXT  := TG_ARGV[0];   -- PK column name, passed per table
    -- Logical table name, passed per trigger. On a PARTITIONED table,
    -- TG_TABLE_NAME resolves to the PHYSICAL partition that received the row
    -- (e.g. 'transactions_default'), NOT the parent 'transactions'. Compliance
    -- reports and runbooks filter audit_log on table_name = 'transactions', so
    -- logging the partition name would silently break every such query. We
    -- therefore pass the logical name explicitly instead of trusting
    -- TG_TABLE_NAME. (Caught by runtime test, invisible to a syntax parser.)
    v_table_name  TEXT  := COALESCE(TG_ARGV[1], TG_TABLE_NAME);
    v_row         JSONB;
    v_merchant_id UUID;
    v_record_id   UUID;
    v_old         JSONB := NULL;
    v_new         JSONB := NULL;
BEGIN
    IF TG_OP = 'INSERT' THEN
        v_new := to_jsonb(NEW);
        v_row := v_new;
    ELSIF TG_OP = 'UPDATE' THEN
        v_old := to_jsonb(OLD);
        v_new := to_jsonb(NEW);
        v_row := v_new;
    ELSE  -- DELETE
        v_old := to_jsonb(OLD);
        v_row := v_old;
    END IF;

    -- Generic extraction: the function must serve transactions
    -- (PK transaction_id) and subscriptions (PK subscription_id) alike.
    v_merchant_id := (v_row ->> 'merchant_id')::UUID;
    v_record_id   := (v_row ->> v_pk_col)::UUID;

    INSERT INTO audit_log (merchant_id, table_name, operation, record_id,
                           old_values, new_values)
    VALUES (v_merchant_id, v_table_name, LEFT(TG_OP, 1), v_record_id, v_old, v_new);

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_transactions_audit
    AFTER INSERT OR UPDATE OR DELETE ON transactions
    FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger('transaction_id', 'transactions');

CREATE TRIGGER trg_subscriptions_audit
    AFTER INSERT OR UPDATE OR DELETE ON subscriptions
    FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger('subscription_id', 'subscriptions');
