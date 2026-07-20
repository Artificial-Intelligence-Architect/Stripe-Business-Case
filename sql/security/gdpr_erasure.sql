-- ============================================================
-- SQL/Security: Right to Erasure — GDPR art.17 (PostgreSQL)
-- ============================================================
--
-- ⚠️  BUG FIXED IN THIS FILE (silent compliance failure)
--
--   v1 anonymised the customer with an UPDATE, then logged it as
--       INSERT INTO audit_log (..., operation, ...) VALUES (..., 'D', ...)
--   while the Airflow compliance report counted erasures with
--       WHERE operation = 'U' AND new_values->>'reason' = 'GDPR_erasure_request'
--
--   The two never matched: the daily GDPR report returned 0 anonymisations
--   forever, including on days when erasures actually ran. A compliance
--   control that always returns "nothing to see" is worse than no control —
--   it manufactures false assurance and would fail an audit.
--
--   Fix: operation reflects the PHYSICAL act ('U' = update/anonymise), and
--   `reason` is a first-class indexed column instead of a JSONB needle.
--
-- ⚠️  SCOPE: this file covers PostgreSQL only.
--   GDPR erasure is NOT complete until it propagates to MongoDB, Snowflake,
--   Kafka and backups. See docs/13_gdpr_cross_system_erasure.md — the DAG
--   task `propagate_erasure` emits the event that drives the other systems.
-- ============================================================

-- Erasure request register (the evidence an auditor asks for)
CREATE TABLE IF NOT EXISTS gdpr_erasure_requests (
    request_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    merchant_id     UUID        NOT NULL,
    customer_id     UUID        NOT NULL,
    requested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- GDPR art.12(3): one month to respond, extendable to three.
    -- NOTE: this is a DEFAULT, not a GENERATED column. PostgreSQL requires
    -- generated expressions to be IMMUTABLE, and (timestamptz + interval) is
    -- STABLE (its result depends on the session time zone), so it is rejected
    -- as a generation expression. A DEFAULT evaluated at INSERT time is the
    -- correct construct here and behaves identically for our purpose.
    sla_due_at      TIMESTAMPTZ NOT NULL DEFAULT (now() + INTERVAL '30 days'),
    status          VARCHAR(20) NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending','processing','completed','rejected')),
    completed_at    TIMESTAMPTZ,
    -- Erasure is not absolute: art.17(3)(b) + PCI-DSS/AML require retaining
    -- financial records. We anonymise; we never delete the transaction.
    legal_hold      BOOLEAN     NOT NULL DEFAULT false,
    rejection_reason TEXT,
    verified_by     VARCHAR(100),
    verification_method VARCHAR(50),
    -- Cross-system propagation receipts
    postgres_done   BOOLEAN NOT NULL DEFAULT false,
    mongodb_done    BOOLEAN NOT NULL DEFAULT false,
    snowflake_done  BOOLEAN NOT NULL DEFAULT false,
    FOREIGN KEY (merchant_id, customer_id)
        REFERENCES customers(merchant_id, customer_id)
);

CREATE INDEX IF NOT EXISTS idx_gdpr_sla
    ON gdpr_erasure_requests (status, sla_due_at)
    WHERE status IN ('pending','processing');

-- ============================================================
-- Erasure procedure
-- ============================================================
CREATE OR REPLACE PROCEDURE gdpr_erase_customer(
    p_merchant_id UUID,
    p_customer_id UUID,
    p_request_id  UUID DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_request_id UUID := p_request_id;
    v_hold       BOOLEAN;
BEGIN
    -- 1. Refuse to erase under legal hold (art.17(3)(b) / AML retention)
    SELECT legal_hold INTO v_hold
    FROM gdpr_erasure_requests WHERE request_id = v_request_id;

    IF COALESCE(v_hold, false) THEN
        UPDATE gdpr_erasure_requests
        SET status = 'rejected',
            rejection_reason = 'Legal hold: AML retention (art.17(3)(b))',
            completed_at = now()
        WHERE request_id = v_request_id;
        RETURN;
    END IF;

    -- 2. Anonymise the identifying data. The row survives: transactions
    --    must keep pointing at a valid customer_id for financial integrity.
    UPDATE customers
    SET email      = 'erased_' || encode(digest(customer_id::text, 'sha256'), 'hex')
                     || '@deleted.invalid',
        email_hash = NULL,
        is_erased  = true
    WHERE merchant_id = p_merchant_id
      AND customer_id = p_customer_id;

    -- 3. Strip re-identification vectors from the transaction history.
    --    Amounts and dates STAY (financial record, art.17(3)(b)).
    UPDATE transactions
    SET ip_country  = NULL,
        device_type = NULL
    WHERE merchant_id = p_merchant_id
      AND customer_id = p_customer_id;

    -- 4. Evidence. operation = 'U' because the physical act IS an update.
    --    reason is an indexed column, not a JSONB key: the compliance job
    --    filters on it, and a typo in a JSONB path fails silently.
    INSERT INTO audit_log (merchant_id, table_name, operation, record_id, reason, new_values)
    VALUES (p_merchant_id, 'customers', 'U', p_customer_id, 'GDPR_erasure_request',
            jsonb_build_object('request_id', v_request_id,
                               'erased_at',  now(),
                               'scope',      'postgres'));

    -- 5. Mark PostgreSQL done. The request stays OPEN until MongoDB and
    --    Snowflake confirm — "completed" must mean erased everywhere.
    UPDATE gdpr_erasure_requests
    SET status = 'processing', postgres_done = true
    WHERE request_id = v_request_id;
END;
$$;

-- ============================================================
-- Closure: only when every system has confirmed
-- ============================================================
CREATE OR REPLACE PROCEDURE gdpr_close_request(p_request_id UUID)
LANGUAGE plpgsql
AS $$
DECLARE r RECORD;
BEGIN
    SELECT * INTO r FROM gdpr_erasure_requests WHERE request_id = p_request_id;

    IF r.postgres_done AND r.mongodb_done AND r.snowflake_done THEN
        UPDATE gdpr_erasure_requests
        SET status = 'completed', completed_at = now()
        WHERE request_id = p_request_id;

        INSERT INTO audit_log (merchant_id, table_name, operation, record_id,
                               reason, new_values)
        VALUES (r.merchant_id, 'gdpr_erasure_requests', 'U', r.customer_id,
                'GDPR_erasure_completed',
                jsonb_build_object('request_id', p_request_id,
                                   'systems', ARRAY['postgres','mongodb','snowflake']));
    ELSE
        RAISE EXCEPTION 'Cannot close %: pg=% mongo=% snowflake=%',
            p_request_id, r.postgres_done, r.mongodb_done, r.snowflake_done;
    END IF;
END;
$$;

-- ============================================================
-- Data portability — GDPR art.20 (the 2% gap claimed in docs/06)
-- ============================================================
CREATE OR REPLACE FUNCTION gdpr_export_customer(
    p_merchant_id UUID,
    p_customer_id UUID
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE v_out JSONB;
BEGIN
    SELECT jsonb_build_object(
        'export_generated_at', now(),
        'format',              'JSON (RFC 8259) — machine-readable, art.20(1)',
        'customer', (
            SELECT jsonb_build_object('customer_id', customer_id,
                                      'email', email,
                                      'country', country_code,
                                      'created_at', created_at)
            FROM customers
            WHERE merchant_id = p_merchant_id AND customer_id = p_customer_id
        ),
        'transactions', COALESCE((
            SELECT jsonb_agg(jsonb_build_object(
                'transaction_id', transaction_id, 'date', created_at,
                'amount', amount, 'currency', currency, 'kind', kind,
                'status', status, 'payment_method', payment_method))
            FROM transactions
            WHERE merchant_id = p_merchant_id AND customer_id = p_customer_id
        ), '[]'::jsonb),
        'subscriptions', COALESCE((
            SELECT jsonb_agg(jsonb_build_object(
                'subscription_id', subscription_id, 'status', status,
                'billing_interval', billing_interval, 'created_at', created_at))
            FROM subscriptions
            WHERE merchant_id = p_merchant_id AND customer_id = p_customer_id
        ), '[]'::jsonb)
        -- fraud_score is deliberately EXCLUDED: art.20 covers data "provided
        -- by the data subject", not scores we inferred about them. Including
        -- it would also leak the model's decision boundary to fraudsters.
    ) INTO v_out;

    INSERT INTO audit_log (merchant_id, table_name, operation, record_id, reason, new_values)
    VALUES (p_merchant_id, 'customers', 'U', p_customer_id, 'GDPR_portability_export',
            jsonb_build_object('exported_at', now()));

    RETURN v_out;
END;
$$;
