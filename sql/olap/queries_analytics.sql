-- ============================================================
-- SQL/OLAP: Analytical Queries (Snowflake)
-- ============================================================
-- Every query declares its SCD2 join semantics explicitly.
--   [POINT-IN-TIME] join by surrogate key, no is_current filter
--   [CURRENT-STATE] join by natural key + is_current = true
-- See the header of sql/olap/schema.sql for why this matters.
-- ============================================================

-- ============================================================
-- 1. Monthly net revenue per merchant with MoM growth
--    [POINT-IN-TIME] — historical revenue must never move.
-- ============================================================
WITH monthly_revenue AS (
    SELECT
        d.year,
        d.month,
        m.merchant_id,
        m.name   AS merchant_name,
        m.region,
        SUM(IFF(f.kind = 'payment', f.amount_usd, -f.amount_usd)) AS net_revenue_usd,
        COUNT(*)                                                  AS nb_transactions,
        SUM(IFF(f.is_fraud, 1, 0))                                AS fraud_count
    FROM fact_transactions f
    JOIN dim_date     d ON f.date_sk     = d.date_sk
    JOIN dim_merchant m ON f.merchant_sk = m.merchant_sk
    WHERE f.status = 'success'
    -- no is_current: merchant_sk already pins the correct historical version
    GROUP BY 1, 2, 3, 4, 5
)
SELECT
    *,
    LAG(net_revenue_usd) OVER (
        PARTITION BY merchant_id ORDER BY year, month
    ) AS prev_month_revenue,
    ROUND(
        (net_revenue_usd - LAG(net_revenue_usd) OVER (
            PARTITION BY merchant_id ORDER BY year, month)
        ) * 100.0 / NULLIF(LAG(net_revenue_usd) OVER (
            PARTITION BY merchant_id ORDER BY year, month), 0),
        2
    ) AS mom_growth_pct
FROM monthly_revenue
ORDER BY year DESC, month DESC, net_revenue_usd DESC;

-- ============================================================
-- 2. Merchants with fraud rate > 2% (current year)
--    [CURRENT-STATE] — a risk officer acts on TODAY's tier/region,
--    so this one joins by natural key and DOES filter is_current.
--    This is the legitimate use of the filter.
-- ============================================================
SELECT
    m.name                                      AS merchant_name,
    m.tier,
    m.region,
    COUNT(*)                                    AS total_transactions,
    SUM(IFF(f.is_fraud, 1, 0))                  AS fraud_transactions,
    ROUND(SUM(IFF(f.is_fraud, 1, 0)) * 100.0 / COUNT(*), 3) AS fraud_rate_pct,
    SUM(IFF(f.is_fraud, f.amount_usd, 0))       AS fraud_exposure_usd
FROM fact_transactions f
JOIN dim_date d ON f.date_sk = d.date_sk
-- natural-key join to the CURRENT merchant version, on purpose:
JOIN dim_merchant m_hist ON f.merchant_sk = m_hist.merchant_sk
JOIN dim_merchant m      ON m_hist.merchant_id = m.merchant_id
                        AND m.is_current = true
WHERE d.year = YEAR(CURRENT_DATE)
  AND f.kind = 'payment'
GROUP BY 1, 2, 3
HAVING COUNT(*) >= 100
   AND SUM(IFF(f.is_fraud, 1, 0)) * 100.0 / COUNT(*) > 2.0
ORDER BY fraud_rate_pct DESC;

-- ============================================================
-- 3. RFM customer segmentation
--    [POINT-IN-TIME] on facts, [CURRENT-STATE] on the customer label.
--    GDPR: erased customers are excluded — they must not be marketed to.
-- ============================================================
WITH rfm_raw AS (
    SELECT
        c.customer_id,
        MAX(c.merchant_id)                               AS merchant_id,
        DATEDIFF('day', MAX(d.full_date), CURRENT_DATE)  AS recency_days,
        COUNT(DISTINCT f.transaction_sk)                 AS frequency,
        SUM(f.amount_usd)                                AS monetary_usd
    FROM fact_transactions f
    JOIN dim_customer c ON f.customer_sk = c.customer_sk
    JOIN dim_date     d ON f.date_sk     = d.date_sk
    WHERE f.status    = 'success'
      AND f.kind      = 'payment'
      AND c.is_erased = false
    -- Aggregate on the NATURAL key: a customer whose segment changed has two
    -- SKs; grouping by customer_sk would split one human into two "customers"
    -- and halve their frequency. v1 grouped by customer_sk. This is the same
    -- class of bug as the is_current filter.
    GROUP BY 1
),
rfm_scored AS (
    SELECT *,
        NTILE(5) OVER (ORDER BY recency_days ASC) AS r_score,
        NTILE(5) OVER (ORDER BY frequency DESC)   AS f_score,
        NTILE(5) OVER (ORDER BY monetary_usd DESC) AS m_score
    FROM rfm_raw
)
SELECT
    customer_id,
    merchant_id,
    r_score, f_score, m_score,
    (r_score + f_score + m_score) AS rfm_total,
    CASE
        WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
        WHEN r_score >= 3 AND f_score >= 3                  THEN 'Loyal Customers'
        WHEN r_score >= 4 AND f_score <= 2                  THEN 'New Customers'
        WHEN r_score <= 2 AND f_score >= 3                  THEN 'At Risk'
        WHEN r_score <= 2 AND f_score <= 2                  THEN 'Lost'
        ELSE 'Potential'
    END AS customer_segment
FROM rfm_scored
ORDER BY rfm_total DESC;

-- ============================================================
-- 4. Product performance  — brief: "Product Performance Metrics"
--    [POINT-IN-TIME]
-- ============================================================
SELECT
    p.category_label,
    p.name                                      AS product_name,
    p.is_recurring,
    d.year,
    d.quarter,
    COUNT(DISTINCT f.transaction_sk)            AS units_sold,
    SUM(IFF(f.kind = 'payment', f.amount_usd, -f.amount_usd)) AS net_revenue_usd,
    SUM(IFF(f.kind = 'refund', f.amount_usd, 0))              AS refunded_usd,
    ROUND(SUM(IFF(f.kind = 'refund', f.amount_usd, 0)) * 100.0
          / NULLIF(SUM(IFF(f.kind = 'payment', f.amount_usd, 0)), 0), 2) AS refund_rate_pct,
    ROUND(AVG(f.fraud_score), 4)                AS avg_fraud_score
FROM fact_transactions f
JOIN dim_product p ON f.product_sk = p.product_sk
JOIN dim_date    d ON f.date_sk    = d.date_sk
WHERE f.status = 'success'
GROUP BY 1, 2, 3, 4, 5
ORDER BY net_revenue_usd DESC;

-- ============================================================
-- 5. Subscription MRR waterfall + net revenue retention
--    Brief: "subscription management". Answers: is growth coming from
--    new logos or from expansion of the existing base?
-- ============================================================
WITH mrr_movement AS (
    SELECT
        d.year,
        d.month,
        SUM(IFF(e.event_type = 'created',    e.mrr_delta_usd, 0)) AS new_mrr,
        SUM(IFF(e.event_type = 'upgraded',   e.mrr_delta_usd, 0)) AS expansion_mrr,
        SUM(IFF(e.event_type = 'downgraded', e.mrr_delta_usd, 0)) AS contraction_mrr,
        SUM(IFF(e.event_type = 'canceled',   e.mrr_delta_usd, 0)) AS churned_mrr
    FROM fact_subscription_events e
    JOIN dim_date d ON e.date_sk = d.date_sk
    GROUP BY 1, 2
),
with_base AS (
    SELECT *,
        SUM(new_mrr + expansion_mrr + contraction_mrr + churned_mrr)
            OVER (ORDER BY year, month
                  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS opening_mrr
    FROM mrr_movement
)
SELECT
    year, month,
    ROUND(opening_mrr, 2)                                    AS opening_mrr_usd,
    ROUND(new_mrr, 2)                                        AS new_mrr_usd,
    ROUND(expansion_mrr, 2)                                  AS expansion_mrr_usd,
    ROUND(contraction_mrr, 2)                                AS contraction_mrr_usd,
    ROUND(churned_mrr, 2)                                    AS churned_mrr_usd,
    ROUND(opening_mrr + new_mrr + expansion_mrr
          + contraction_mrr + churned_mrr, 2)                AS closing_mrr_usd,
    -- NRR excludes new logos: it measures the existing base only.
    ROUND((opening_mrr + expansion_mrr + contraction_mrr + churned_mrr)
          * 100.0 / NULLIF(opening_mrr, 0), 1)               AS net_revenue_retention_pct,
    ROUND(-churned_mrr * 100.0 / NULLIF(opening_mrr, 0), 2)  AS gross_churn_pct
FROM with_base
ORDER BY year DESC, month DESC;

-- ============================================================
-- 6. Refund / chargeback lineage — brief: "payments, refunds, chargebacks"
--    Impossible in v1: refunds were a status with no link to the payment.
-- ============================================================
SELECT
    m.name                             AS merchant_name,
    pay.transaction_id                 AS original_payment_id,
    pay.amount_usd                     AS original_amount_usd,
    dp.full_date                       AS payment_date,
    COUNT(ref.transaction_sk)          AS nb_refunds,
    SUM(ref.amount_usd)                AS total_refunded_usd,
    ROUND(SUM(ref.amount_usd) * 100.0 / NULLIF(pay.amount_usd, 0), 1) AS refunded_pct,
    DATEDIFF('day', dp.full_date, MAX(dr.full_date))                  AS days_to_last_refund,
    MAX(IFF(ref.kind = 'chargeback', 1, 0))                           AS had_chargeback
FROM fact_transactions pay
JOIN dim_merchant m  ON pay.merchant_sk = m.merchant_sk
JOIN dim_date     dp ON pay.date_sk     = dp.date_sk
JOIN fact_transactions ref ON ref.parent_transaction_id = pay.transaction_id
JOIN dim_date     dr ON ref.date_sk     = dr.date_sk
WHERE pay.kind = 'payment'
  AND ref.kind IN ('refund','chargeback')
GROUP BY 1, 2, 3, 4
HAVING SUM(ref.amount_usd) > 0
ORDER BY refunded_pct DESC, total_refunded_usd DESC;

-- ============================================================
-- 7. Time-series: 7-day rolling fraud rate + WoW anomaly detection
--    Brief: "support time-series analysis" + "near real-time insights".
-- ============================================================
WITH daily AS (
    SELECT
        d.full_date,
        m.merchant_id,
        COUNT(*)                   AS txn_count,
        SUM(IFF(f.is_fraud, 1, 0)) AS fraud_count
    FROM fact_transactions f
    JOIN dim_date     d ON f.date_sk     = d.date_sk
    JOIN dim_merchant m ON f.merchant_sk = m.merchant_sk
    WHERE f.kind = 'payment'
      AND d.full_date >= DATEADD('day', -90, CURRENT_DATE)
    GROUP BY 1, 2
),
rolling AS (
    SELECT *,
        SUM(fraud_count) OVER (PARTITION BY merchant_id ORDER BY full_date
                               ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS fraud_7d,
        SUM(txn_count)   OVER (PARTITION BY merchant_id ORDER BY full_date
                               ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS txn_7d,
        AVG(fraud_count * 1.0 / NULLIF(txn_count, 0))
            OVER (PARTITION BY merchant_id ORDER BY full_date
                  ROWS BETWEEN 30 PRECEDING AND 7 PRECEDING) AS baseline_rate,
        STDDEV(fraud_count * 1.0 / NULLIF(txn_count, 0))
            OVER (PARTITION BY merchant_id ORDER BY full_date
                  ROWS BETWEEN 30 PRECEDING AND 7 PRECEDING) AS baseline_stddev
    FROM daily
)
SELECT
    full_date,
    merchant_id,
    txn_7d,
    ROUND(fraud_7d * 100.0 / NULLIF(txn_7d, 0), 3) AS fraud_rate_7d_pct,
    ROUND(baseline_rate * 100, 3)                  AS baseline_rate_pct,
    -- z-score > 3 = the merchant's fraud rate broke out of its own history
    ROUND((fraud_7d * 1.0 / NULLIF(txn_7d, 0) - baseline_rate)
          / NULLIF(baseline_stddev, 0), 2)         AS z_score,
    IFF((fraud_7d * 1.0 / NULLIF(txn_7d, 0) - baseline_rate)
        / NULLIF(baseline_stddev, 0) > 3, 'ALERT', 'OK') AS anomaly_flag
FROM rolling
WHERE txn_7d >= 50
ORDER BY z_score DESC NULLS LAST;

-- ============================================================
-- 8. Compliance: GDPR erasure SLA + audit coverage
--    Brief: "automated compliance reporting".
-- ============================================================
SELECT
    d.year,
    d.month,
    COUNT(*)                                              AS erasure_requests,
    SUM(IFF(a.completed_within_30d, 1, 0))                AS completed_in_sla,
    ROUND(SUM(IFF(a.completed_within_30d, 1, 0)) * 100.0 / COUNT(*), 1) AS sla_compliance_pct,
    MAX(a.days_to_complete)                               AS worst_case_days,
    -- GDPR art.12(3): one month to respond.
    IFF(MAX(a.days_to_complete) > 30, 'BREACH', 'COMPLIANT') AS gdpr_status
FROM audit_erasure_requests a
JOIN dim_date d ON a.request_date_sk = d.date_sk
GROUP BY 1, 2
ORDER BY 1 DESC, 2 DESC;
