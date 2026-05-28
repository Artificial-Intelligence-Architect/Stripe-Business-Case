-- ============================================================
-- SQL/Sécurité: Configuration RBAC (PostgreSQL)
-- ============================================================

-- Création des rôles de groupe (sans login)
-- Ils servent de modèles de permissions pour les utilisateurs finaux.
CREATE ROLE analyst_read;
CREATE ROLE engineer_write;
CREATE ROLE compliance_officer;
CREATE ROLE ml_service;

-- (Optionnel) Exemple de création d'utilisateurs héritant des rôles
-- CREATE USER john_analyst WITH PASSWORD 'change_me' IN ROLE analyst_read;
-- CREATE USER jane_engineer WITH PASSWORD 'change_me' IN ROLE engineer_write;

-- ============================================================
-- 1. Rôle analyst_read : lectures métier (hors PII)
-- ============================================================
GRANT SELECT ON countries, currencies TO analyst_read;

-- Merchants : consultation autorisée
GRANT SELECT ON merchants TO analyst_read;

-- Transactions : accès global, mais on retire la colonne sensible customer_id
GRANT SELECT ON transactions TO analyst_read;
REVOKE SELECT (customer_id) ON transactions FROM analyst_read;

-- Customers : pas d'accès direct (contient emails, hash)
-- (aucune permission accordée sur cette table)

-- ============================================================
-- 2. Rôle engineer_write : opérations d'écriture sur le cœur métier
-- ============================================================
GRANT SELECT, INSERT, UPDATE ON transactions, merchants, customers TO engineer_write;
GRANT SELECT ON countries, currencies TO engineer_write;

-- ============================================================
-- 3. Rôle compliance_officer : accès complet en lecture
--    (y compris audit logs)
-- ============================================================
GRANT SELECT ON ALL TABLES IN SCHEMA public TO compliance_officer;
-- S'assurer que le rôle puisse lire les futures tables (via paramétrage)
-- ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO compliance_officer;

-- ============================================================
-- 4. Rôle ml_service : accès strictement nécessaire pour le ML
-- ============================================================
-- Pour la table transactions, seules les colonnes transaction_id et fraud_score sont exposées.
GRANT SELECT (transaction_id, fraud_score) ON transactions TO ml_service;
-- Aucun autre accès (pas de merchants, customers, ni autres colonnes).

-- ============================================================
-- Vérifications (optionnel) : pour tester les permissions
-- ============================================================
-- SET ROLE analyst_read;
-- SELECT * FROM transactions LIMIT 1; -- doit échouer sur customer_id
-- RESET ROLE;