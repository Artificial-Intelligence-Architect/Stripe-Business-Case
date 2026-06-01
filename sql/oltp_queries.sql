-- ============================================================
-- SQL/OLTP: Requêtes opérationnelles (PostgreSQL)
-- ============================================================

-- 1. Détection en temps réel : transactions à haut risque (dernières 5 min)
SELECT
    t.transaction_id,
    t.merchant_id,
    m.name AS merchant_name,
    t.amount,
    t.currency,
    t.status,
    t.fraud_score,
    t.created_at
FROM transactions t
JOIN merchants m ON t.merchant_id = m.merchant_id
WHERE t.fraud_score > 0.9
  AND t.created_at > NOW() - INTERVAL '5 minutes'
ORDER BY t.fraud_score DESC;

-- 2. Activité récente d'un marchand (dernière heure)
SELECT
    status,
    COUNT(*) AS nb_transactions,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount,
    AVG(fraud_score) AS avg_fraud_score
FROM transactions
WHERE merchant_id = '7c9e6679-7425-40de-944b-e07fc1f90ae7'  -- exemple d'UUID marchand
  AND created_at > NOW() - INTERVAL '1 hour'
GROUP BY status;

-- 3. Transactions en échec pour un client (24h)
SELECT
    transaction_id,
    amount,
    currency,
    status,
    created_at
FROM transactions
WHERE customer_id = '3d0b4a7e-1234-5678-9abc-def012345678'
  AND status IN ('failed', 'chargeback')
  AND created_at > NOW() - INTERVAL '24 hours'
ORDER BY created_at DESC;

-- 4. Vélocité anormale : clients avec > 5 transactions en 1 minute
SELECT
    customer_id,
    COUNT(*) AS txn_count,
    MIN(created_at) AS first_txn,
    MAX(created_at) AS last_txn
FROM transactions
WHERE created_at > NOW() - INTERVAL '24 hours'
GROUP BY customer_id
HAVING COUNT(*) >= 5
   AND MAX(created_at) - MIN(created_at) <= INTERVAL '1 minute'
ORDER BY txn_count DESC;

-- 5. Récapitulatif quotidien (aujourd'hui)
SELECT
    COUNT(*) AS total_transactions,
    COUNT(*) FILTER (WHERE status = 'success') AS successful,
    COUNT(*) FILTER (WHERE status = 'failed') AS failed,
    COUNT(*) FILTER (WHERE status = 'refunded') AS refunded,
    COUNT(*) FILTER (WHERE status = 'chargeback') AS chargebacks,
    SUM(amount) FILTER (WHERE status = 'success') AS total_success_amount,
    AVG(fraud_score) FILTER (WHERE status = 'success') AS avg_fraud_score_success,
    COUNT(*) FILTER (WHERE fraud_score > 0.7) AS high_risk_count
FROM transactions
WHERE created_at >= CURRENT_DATE;

-- 6. Top 10 marchands par volume sur les 7 derniers jours
SELECT
    m.merchant_id,
    m.name,
    COUNT(*) AS txn_count,
    SUM(t.amount) AS total_amount,
    AVG(t.fraud_score) AS avg_fraud_score
FROM transactions t
JOIN merchants m ON t.merchant_id = m.merchant_id
WHERE t.created_at >= NOW() - INTERVAL '7 days'
  AND t.status = 'success'
GROUP BY m.merchant_id, m.name
ORDER BY total_amount DESC
LIMIT 10;