-- ============================================================
-- SQL/OLTP: Operational Queries (PostgreSQL)
-- Stripe Business Case — AIA Certification
-- ============================================================
-- Scope: real-time and near-real-time queries executed directly
-- against the transactional database (PostgreSQL + Citus).
-- Complementary to sql/olap/queries_analytics.sql (Snowflake),
-- which handles historical/aggregated analytics.
-- ============================================================

-- ------------------------------------------------------------
-- 1. REAL-TIME FRAUD DETECTION
--    Customers with abnormal velocity in the last hour:
--    more than 10 transactions OR more than $5,000 cumulated.
--    Used by the fraud engine to trigger a manual review.
-- ------------------------------------------------------------
WITH tx_velocity_1h AS (
    SELECT
        merchant_id,
        customer_id,
        COUNT(*)                                    AS tx_count_1h,
        SUM(amount_usd)                             AS amount_usd_1h,
        MAX(fraud_score)                            AS max_fraud_score,
        COUNT(*) FILTER (WHERE status = 'failed')   AS failed_count,
        COUNT(DISTINCT ip_country)                  AS distinct_countries
    FROM transactions
    WHERE created_at >= NOW() - INTERVAL '1 hour'
    GROUP BY merchant_id, customer_id
)
SELECT
    v.customer_id,
    c.email_hash,
    v.tx_count_1h,
    ROUND(v.amount_usd_1h, 2)   AS amount_usd_1h,
    v.max_fraud_score,
    v.failed_count,
    v.distinct_countries,
    CASE
        WHEN v.max_fraud_score  > 0.9 THEN 'CRITICAL'
        WHEN v.tx_count_1h      > 20  THEN 'HIGH'
        WHEN v.amount_usd_1h    > 10000 THEN 'HIGH'
        ELSE 'MEDIUM'
    END                          AS risk_level
FROM tx_velocity_1h v
-- Composite join key: customers is keyed (merchant_id, customer_id) under Citus,
-- and this makes the join single-shard (colocated) instead of cross-shard.
JOIN customers c ON c.merchant_id = v.merchant_id AND c.customer_id = v.customer_id
WHERE v.tx_count_1h > 10
   OR v.amount_usd_1h > 5000
ORDER BY v.max_fraud_score DESC, v.amount_usd_1h DESC;


-- ------------------------------------------------------------
-- 2. PENDING TRANSACTIONS MONITORING
--    Transactions stuck in "pending" for more than 5 minutes.
--    Operational alert: possible issuer timeout or processor hang.
-- ------------------------------------------------------------
SELECT
    t.transaction_id,
    t.merchant_id,
    m.name                                   AS merchant_name,
    t.amount_usd,
    t.payment_method,
    t.device_type,
    t.created_at,
    EXTRACT(EPOCH FROM (NOW() - t.created_at)) / 60  AS pending_minutes
FROM transactions t
JOIN merchants m ON t.merchant_id = m.merchant_id
WHERE t.status = 'pending'
  AND t.created_at < NOW() - INTERVAL '5 minutes'
ORDER BY t.created_at ASC;   -- oldest first → highest priority


-- ------------------------------------------------------------
-- 3. MERCHANT DAILY REVENUE SUMMARY
--    Real-time snapshot of today's revenue per merchant.
--    Used for operational dashboards (not historical reports).
--    For historical trends, use Snowflake mv_daily_revenue.
-- ------------------------------------------------------------
SELECT
    m.merchant_id,
    m.name                                          AS merchant_name,
    m.tier,
    COUNT(*)                                        AS total_tx,
    COUNT(*) FILTER (WHERE t.status = 'success')    AS successful_tx,
    COUNT(*) FILTER (WHERE t.status = 'failed')     AS failed_tx,
    COUNT(*) FILTER (WHERE t.status = 'refunded')   AS refunded_tx,
    ROUND(SUM(t.amount_usd)
          FILTER (WHERE t.status = 'success'), 2)   AS revenue_usd,
    ROUND(AVG(t.fraud_score)
          FILTER (WHERE t.fraud_score IS NOT NULL), 4) AS avg_fraud_score,
    ROUND(
        COUNT(*) FILTER (WHERE t.status = 'failed') * 100.0
        / NULLIF(COUNT(*), 0),
        2
    )                                               AS failure_rate_pct
FROM transactions t
JOIN merchants m ON t.merchant_id = m.merchant_id
WHERE t.created_at >= CURRENT_DATE        -- today only (partition pruning)
GROUP BY m.merchant_id, m.name, m.tier
ORDER BY revenue_usd DESC NULLS LAST;


-- ------------------------------------------------------------
-- 4. CHARGEBACK EXPOSURE PER MERCHANT (ROLLING 30 DAYS)
--    Identifies merchants approaching the chargeback threshold
--    (1% is the Visa/Mastercard limit before fine/suspension).
--    PCI-DSS compliance monitoring.
-- ------------------------------------------------------------
SELECT
    m.merchant_id,
    m.name                                              AS merchant_name,
    m.tier,
    COUNT(*)                                            AS total_tx_30d,
    COUNT(*) FILTER (WHERE t.status = 'chargeback')     AS chargeback_count,
    ROUND(
        COUNT(*) FILTER (WHERE t.status = 'chargeback') * 100.0
        / NULLIF(COUNT(*), 0),
        3
    )                                                   AS chargeback_rate_pct,
    ROUND(SUM(t.amount_usd)
          FILTER (WHERE t.status = 'chargeback'), 2)    AS chargeback_exposure_usd,
    CASE
        WHEN COUNT(*) FILTER (WHERE t.status = 'chargeback') * 100.0
             / NULLIF(COUNT(*), 0) > 1.0 THEN 'CRITICAL — Above threshold'
        WHEN COUNT(*) FILTER (WHERE t.status = 'chargeback') * 100.0
             / NULLIF(COUNT(*), 0) > 0.75 THEN 'WARNING — Approaching threshold'
        ELSE 'OK'
    END                                                 AS risk_status
FROM transactions t
JOIN merchants m ON t.merchant_id = m.merchant_id
WHERE t.created_at >= NOW() - INTERVAL '30 days'
GROUP BY m.merchant_id, m.name, m.tier
HAVING COUNT(*) >= 50    -- statistical minimum for significance
ORDER BY chargeback_rate_pct DESC;


-- ------------------------------------------------------------
-- 5. PAYMENT METHOD PERFORMANCE (LAST 7 DAYS)
--    Failure rates and fraud exposure per payment method.
--    Used by the product team to tune payment routing.
-- ------------------------------------------------------------
SELECT
    payment_method,
    COUNT(*)                                            AS total_tx,
    COUNT(*) FILTER (WHERE status = 'success')          AS success_count,
    COUNT(*) FILTER (WHERE status = 'failed')           AS failed_count,
    ROUND(
        COUNT(*) FILTER (WHERE status = 'failed') * 100.0
        / NULLIF(COUNT(*), 0),
        2
    )                                                   AS failure_rate_pct,
    ROUND(AVG(amount_usd), 2)                           AS avg_amount_usd,
    ROUND(SUM(amount_usd)
          FILTER (WHERE status = 'success'), 2)         AS total_revenue_usd,
    ROUND(AVG(fraud_score)
          FILTER (WHERE fraud_score IS NOT NULL), 4)    AS avg_fraud_score,
    COUNT(*) FILTER (WHERE fraud_score > 0.7)           AS high_risk_tx_count
FROM transactions
WHERE created_at >= NOW() - INTERVAL '7 days'
GROUP BY payment_method
ORDER BY total_revenue_usd DESC NULLS LAST;


-- ------------------------------------------------------------
-- 6. GEOGRAPHIC ANOMALY DETECTION
--    Customers who transacted from more than 2 different
--    countries within 24 hours — strong mule/account-takeover
--    signal. Feeds the fraud_events collection in MongoDB.
-- ------------------------------------------------------------
WITH customer_countries AS (
    SELECT
        merchant_id,
        customer_id,
        array_agg(DISTINCT ip_country ORDER BY ip_country) AS countries_used,
        COUNT(DISTINCT ip_country)                          AS country_count,
        MIN(created_at)                                     AS first_tx,
        MAX(created_at)                                     AS last_tx,
        COUNT(*)                                            AS tx_count,
        SUM(amount_usd)                                     AS total_amount_usd
    FROM transactions
    WHERE created_at >= NOW() - INTERVAL '24 hours'
      AND ip_country IS NOT NULL
    GROUP BY merchant_id, customer_id
)
SELECT
    cc.customer_id,
    c.email_hash,
    cc.country_count,
    cc.countries_used,
    cc.tx_count,
    ROUND(cc.total_amount_usd, 2)   AS total_amount_usd,
    EXTRACT(EPOCH FROM (cc.last_tx - cc.first_tx)) / 60 AS span_minutes
FROM customer_countries cc
JOIN customers c ON c.merchant_id = cc.merchant_id AND c.customer_id = cc.customer_id
WHERE cc.country_count > 2
ORDER BY cc.country_count DESC, cc.total_amount_usd DESC;


-- ------------------------------------------------------------
-- 7. AUDIT LOG — RECENT SENSITIVE CHANGES
--    Last 100 operations on the customers table.
--    Used by the compliance team for incident investigation
--    and GDPR/PCI-DSS audit trails.
-- ------------------------------------------------------------
SELECT
    log_id,
    table_name,
    operation,
    record_id,
    changed_by,
    changed_at,
    old_values  -> 'email'     AS old_email,
    new_values  -> 'email'     AS new_email,
    new_values  -> 'reason'    AS change_reason
FROM audit_log
WHERE table_name = 'customers'
  AND changed_at >= NOW() - INTERVAL '7 days'
ORDER BY changed_at DESC
LIMIT 100;
