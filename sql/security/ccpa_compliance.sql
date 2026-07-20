-- ============================================================
-- SQL/Security: CCPA Compliance Procedures
-- ============================================================

-- The CCPA (California Consumer Privacy Act) grants California residents
-- the right to access their data, request its deletion, and opt out of its sale.
-- These procedures complement the existing GDPR compliance framework.

-- 1. CCPA Requests Logging Table
CREATE TABLE IF NOT EXISTS ccpa_requests (
    request_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Composite FK: customers is now keyed (merchant_id, customer_id) to carry
    -- the Citus distribution column. A bare customer_id FK no longer resolves.
    merchant_id     UUID NOT NULL,
    customer_id     UUID NOT NULL,
    request_type    VARCHAR(20) NOT NULL CHECK (request_type IN ('access', 'deletion', 'opt_out')),
    request_date    TIMESTAMPTZ DEFAULT now(),
    status          VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'completed', 'rejected')),
    completed_date  TIMESTAMPTZ,
    notes           TEXT,
    -- Verification data (required by CCPA)
    verified_by         VARCHAR(100),
    verification_method VARCHAR(50),
    FOREIGN KEY (merchant_id, customer_id)
        REFERENCES customers(merchant_id, customer_id)
);

-- 2. Data Access Request Procedure (Right to Know)
CREATE OR REPLACE PROCEDURE ccpa_access_request(
    p_merchant_id UUID,
    p_customer_id UUID,
    p_verified_by VARCHAR(100),
    p_verification_method VARCHAR(50)
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_request_id UUID;
BEGIN
    -- Log the CCPA request
    INSERT INTO ccpa_requests (merchant_id, customer_id, request_type, status, verified_by, verification_method)
    VALUES (p_merchant_id, p_customer_id, 'access', 'processing', p_verified_by, p_verification_method)
    RETURNING request_id INTO v_request_id;

    -- Note: The collected data is returned to the user via a secure export process.
    -- This process is logged here; the actual export is handled by the application layer.

    -- Mark the request as completed
    UPDATE ccpa_requests
    SET status = 'completed', completed_date = now()
    WHERE request_id = v_request_id;

    -- Record the request in the audit log (proof of compliance)
    INSERT INTO audit_log (merchant_id, table_name, operation, record_id, reason, new_values)
    VALUES (p_merchant_id, 'ccpa_requests', 'I', v_request_id, 'CCPA_request',
            jsonb_build_object(
                'reason', 'CCPA_access_request',
                'customer_id', p_customer_id,
                'request_type', 'access'
            ));
END;
$$;

-- 3. Data Deletion Request Procedure (Right to Delete)
-- Complements the GDPR procedure with specific CCPA logging
CREATE OR REPLACE PROCEDURE ccpa_deletion_request(
    p_merchant_id UUID,
    p_customer_id UUID,
    p_verified_by VARCHAR(100),
    p_verification_method VARCHAR(50)
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_request_id UUID;
BEGIN
    -- Log the CCPA request
    INSERT INTO ccpa_requests (merchant_id, customer_id, request_type, status, verified_by, verification_method)
    VALUES (p_merchant_id, p_customer_id, 'deletion', 'processing', p_verified_by, p_verification_method)
    RETURNING request_id INTO v_request_id;

    -- Call the existing GDPR procedure for anonymisation.
    -- New signature is (merchant_id, customer_id, request_id). We pass NULL for
    -- the request_id: this CCPA deletion is tracked in ccpa_requests, not in
    -- gdpr_erasure_requests, so there is no gdpr request row to update here.
    CALL gdpr_erase_customer(p_merchant_id, p_customer_id, NULL);

    -- Mark the request as completed
    UPDATE ccpa_requests
    SET status = 'completed', completed_date = now()
    WHERE request_id = v_request_id;

    -- Record the deletion in the audit log
    INSERT INTO audit_log (merchant_id, table_name, operation, record_id, reason, new_values)
    VALUES (p_merchant_id, 'ccpa_requests', 'I', v_request_id, 'CCPA_request',
            jsonb_build_object(
                'reason', 'CCPA_deletion_request',
                'customer_id', p_customer_id,
                'request_type', 'deletion'
            ));
END;
$$;

-- 4. Opt-Out of Data Sale Procedure (Right to Opt-Out)
CREATE OR REPLACE PROCEDURE ccpa_opt_out_request(
    p_merchant_id UUID,
    p_customer_id UUID,
    p_verified_by VARCHAR(100),
    p_verification_method VARCHAR(50)
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_request_id UUID;
BEGIN
    -- Log the CCPA request
    INSERT INTO ccpa_requests (merchant_id, customer_id, request_type, status, verified_by, verification_method)
    VALUES (p_merchant_id, p_customer_id, 'opt_out', 'processing', p_verified_by, p_verification_method)
    RETURNING request_id INTO v_request_id;

    -- Mark the customer as opt-out. The ccpa_opt_out column now exists in the
    -- OLTP schema (sql/oltp/schema.sql), so this is a real UPDATE, not a comment.
    UPDATE customers SET ccpa_opt_out = true
    WHERE merchant_id = p_merchant_id AND customer_id = p_customer_id;

    -- Mark the request as completed
    UPDATE ccpa_requests
    SET status = 'completed', completed_date = now()
    WHERE request_id = v_request_id;

    -- Record the opt-out in the audit log
    INSERT INTO audit_log (merchant_id, table_name, operation, record_id, reason, new_values)
    VALUES (p_merchant_id, 'ccpa_requests', 'I', v_request_id, 'CCPA_request',
            jsonb_build_object(
                'reason', 'CCPA_opt_out_request',
                'customer_id', p_customer_id,
                'request_type', 'opt_out'
            ));
END;
$$;

-- 5. CCPA Compliance Report View
CREATE OR REPLACE VIEW ccpa_compliance_report AS
SELECT
    DATE_TRUNC('month', request_date) AS month,
    request_type,
    COUNT(*) FILTER (WHERE status = 'completed') AS completed_requests,
    COUNT(*) FILTER (WHERE status = 'rejected') AS rejected_requests,
    AVG(EXTRACT(EPOCH FROM (completed_date - request_date)) / 3600) AS avg_processing_hours
FROM ccpa_requests
GROUP BY 1, 2
ORDER BY 1 DESC, 2;
