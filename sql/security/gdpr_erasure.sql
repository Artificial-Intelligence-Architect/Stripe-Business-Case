-- ============================================================
-- SQL/Security: Right to Erasure Procedure (GDPR)
-- ============================================================

-- This procedure anonymises a customer's personal data
-- instead of deleting it, in order to preserve the referential integrity
-- of the transactions and the audit trail.

CREATE OR REPLACE PROCEDURE gdpr_erase_customer(p_customer_id UUID)
LANGUAGE plpgsql
AS $$
BEGIN
    -- 1. Anonymise the customer record
    UPDATE customers
    SET
        email      = 'erased_' || MD5(customer_id::text) || '@deleted.stripe.com',
        email_hash = NULL
    WHERE customer_id = p_customer_id;

    -- 2. Anonymise location/device data in their transactions
    UPDATE transactions
    SET
        ip_country  = NULL,
        device_type = NULL
    WHERE customer_id = p_customer_id;

    -- 3. Log the operation (proof of compliance)
    INSERT INTO audit_log (table_name, operation, record_id, new_values)
    VALUES ('customers', 'D', p_customer_id,
            '{"reason": "GDPR_erasure_request"}'::jsonb);

    -- Note: No physical deletion is performed.
    -- Transactions remain associated with the customer_id,
    -- but the identifying data has been neutralised.
    COMMIT;
END;
$$;