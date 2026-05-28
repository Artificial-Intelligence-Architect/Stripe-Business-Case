-- ============================================================
-- SQL/Sécurité: Procédure de droit à l'oubli (RGPD)
-- ============================================================

-- Cette procédure anonymise les données personnelles d'un client
-- au lieu de les supprimer, pour préserver l'intégrité référentielle
-- des transactions et de l'audit.

CREATE OR REPLACE PROCEDURE gdpr_erase_customer(p_customer_id UUID)
LANGUAGE plpgsql
AS $$
BEGIN
    -- 1. Anonymisation de l'enregistrement client
    UPDATE customers
    SET
        email      = 'erased_' || MD5(customer_id::text) || '@deleted.stripe.com',
        email_hash = NULL
    WHERE customer_id = p_customer_id;

    -- 2. Anonymisation des données de localisation/device dans ses transactions
    UPDATE transactions
    SET
        ip_country  = NULL,
        device_type = NULL
    WHERE customer_id = p_customer_id;

    -- 3. Journalisation de l'opération (preuve de conformité)
    INSERT INTO audit_log (table_name, operation, record_id, new_values)
    VALUES ('customers', 'D', p_customer_id,
            '{"reason": "GDPR_erasure_request"}'::jsonb);

    -- Note : Aucune suppression physique n'est effectuée.
    -- Les transactions restent associées au customer_id,
    -- mais les données identifiantes ont été neutralisées.
    COMMIT;
END;
$$;