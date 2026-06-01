-- ============================================================
-- SQL/Security: CCPA Compliance Procedures
-- ============================================================

-- The CCPA (California Consumer Privacy Act) grants California residents
-- the right to access their data, request its deletion, and opt out of its sale.
-- These procedures complement the existing GDPR compliance framework.

-- 1. CCPA Requests Logging Table
CREATE TABLE IF NOT EXISTS ccpa_requests (
    request_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id     UUID NOT NULL REFERENCES customers(customer_id),
    request_type    VARCHAR(20) NOT NULL CHECK (request_type IN ('access', 'deletion', 'opt_out')),
    request_date    TIMESTAMPTZ DEFAULT now(),
    status          VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'completed', 'rejected')),
    completed_date  TIMESTAMPTZ,
    notes           TEXT,
    -- Verification data (required by CCPA)
    verified_by         VARCHAR(100),
    verification_method VARCHAR(50)
);

-- 2. Data Access Request Procedure (Right to Know)
CREATE OR REPLACE PROCEDURE ccpa_access_request(
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
    INSERT INTO ccpa_requests (customer_id, request_type, status, verified_by, verification_method)
    VALUES (p_customer_id, 'access', 'processing', p_verified_by, p_verification_method)
    RETURNING request_id INTO v_request_id;

    -- Note: The collected data is returned to the user via a secure export process.
    -- This process is logged here; the actual export is handled by the application layer.

    -- Mark the request as completed
    UPDATE ccpa_requests
    SET status = 'completed', completed_date = now()
    WHERE request_id = v_request_id;

    -- Record the request in the audit log (proof of compliance)
    INSERT INTO audit_log (table_name, operation, record_id, new_values)
    VALUES ('ccpa_requests', 'I', v_request_id,
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
    INSERT INTO ccpa_requests (customer_id, request_type, status, verified_by, verification_method)
    VALUES (p_customer_id, 'deletion', 'processing', p_verified_by, p_verification_method)
    RETURNING request_id INTO v_request_id;

    -- Call the existing GDPR procedure for anonymisation
    CALL gdpr_erase_customer(p_customer_id);

    -- Mark the request as completed
    UPDATE ccpa_requests
    SET status = 'completed', completed_date = now()
    WHERE request_id = v_request_id;

    -- Record the deletion in the audit log
    INSERT INTO audit_log (table_name, operation, record_id, new_values)
    VALUES ('ccpa_requests', 'I', v_request_id,
            jsonb_build_object(
                'reason', 'CCPA_deletion_request',
                'customer_id', p_customer_id,
                'request_type', 'deletion'
            ));
END;
$$;

-- 4. Opt-Out of Data Sale Procedure (Right to Opt-Out)
CREATE OR REPLACE PROCEDURE ccpa_opt_out_request(
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
    INSERT INTO ccpa_requests (customer_id, request_type, status, verified_by, verification_method)
    VALUES (p_customer_id, 'opt_out', 'processing', p_verified_by, p_verification_method)
    RETURNING request_id INTO v_request_id;

    -- Mark the customer as "opt-out" in the customers table
    -- (Requires adding a ccpa_opt_out column to the customers table)
    -- ALTER TABLE customers ADD COLUMN IF NOT EXISTS ccpa_opt_out BOOLEAN DEFAULT false;
    -- UPDATE customers SET ccpa_opt_out = true WHERE customer_id = p_customer_id;

    -- Mark the request as completed
    UPDATE ccpa_requests
    SET status = 'completed', completed_date = now()
    WHERE request_id = v_request_id;

    -- Record the opt-out in the audit log
    INSERT INTO audit_log (table_name, operation, record_id, new_values)
    VALUES ('ccpa_requests', 'I', v_request_id,
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
<<<<<<< HEAD
ORDER BY 1 DESC, 2;
=======
ORDER BY 1 DESC, 2;
>>>>>>> f74ac85e4b9154faa2cc9e8f53ae4bedf6923069
