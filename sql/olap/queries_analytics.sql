-- ============================================================
-- SQL/OLAP: Analytical Queries (Snowflake)
-- ============================================================

-- 1. Monthly revenue per merchant with MoM evolution
WITH monthly_revenue AS (
    SELECT
        d.year,
        d.month,
        m.name          AS merchant_name,
        m.region,
        SUM(f.amount_usd) AS revenue_usd,
        COUNT(*)          AS nb_transactions,
        COUNT(*) FILTER (WHERE f.is_fraud) AS fraud_count
    FROM fact_transactions f
    JOIN dim_date d     ON f.date_sk = d.date_sk
    JOIN dim_merchant m ON f.merchant_sk = m.merchant_sk
    WHERE f.status = 'success'
      AND m.is_current = true
    GROUP BY 1, 2, 3, 4
)
SELECT
    *,
    LAG(revenue_usd) OVER (PARTITION BY merchant_name ORDER BY year, month) AS prev_month_revenue,
    ROUND(
        (revenue_usd - LAG(revenue_usd) OVER (PARTITION BY merchant_name ORDER BY year, month))
        * 100.0 / NULLIF(LAG(revenue_usd) OVER (PARTITION BY merchant_name ORDER BY year, month), 0),
        2
    ) AS mom_growth_pct
FROM monthly_revenue
ORDER BY year DESC, month DESC, revenue_usd DESC;

-- 2. Merchants with an abnormal fraud rate (> 2% over the current year)
SELECT
    m.name                          AS merchant_name,
    m.tier,
    m.region,
    COUNT(*)                        AS total_transactions,
    COUNT(*) FILTER (WHERE f.is_fraud)   AS fraud_transactions,
    ROUND(
        COUNT(*) FILTER (WHERE f.is_fraud) * 100.0 / COUNT(*),
        3
    )                               AS fraud_rate_pct,
    SUM(f.amount_usd) FILTER (WHERE f.is_fraud) AS fraud_exposure_usd
FROM fact_transactions f
JOIN dim_merchant m ON f.merchant_sk = m.merchant_sk
JOIN dim_date d     ON f.date_sk = d.date_sk
WHERE d.year = YEAR(CURRENT_DATE)
  AND m.is_current = true
GROUP BY 1, 2, 3
HAVING COUNT(*) >= 100              -- statistical minimum
   AND COUNT(*) FILTER (WHERE f.is_fraud) * 100.0 / COUNT(*) > 2.0
ORDER BY fraud_rate_pct DESC;

-- 3. RFM customer segmentation (Recency, Frequency, Monetary)
WITH rfm_raw AS (
    SELECT
        c.customer_sk,
        DATEDIFF('day', MAX(d.full_date), CURRENT_DATE) AS recency_days,
        COUNT(DISTINCT f.transaction_sk)                 AS frequency,
        SUM(f.amount_usd)                                AS monetary_usd
    FROM fact_transactions f
    JOIN dim_customer c ON f.customer_sk = c.customer_sk
    JOIN dim_date d     ON f.date_sk = d.date_sk
    WHERE f.status = 'success'
      AND c.is_current = true
    GROUP BY 1
),
rfm_scored AS (
    SELECT *,
        NTILE(5) OVER (ORDER BY recency_days ASC)   AS r_score,  -- 5 = most recent
        NTILE(5) OVER (ORDER BY frequency DESC)      AS f_score,
        NTILE(5) OVER (ORDER BY monetary_usd DESC)   AS m_score
    FROM rfm_raw
)
SELECT
    customer_sk,
    r_score, f_score, m_score,
    (r_score + f_score + m_score) AS rfm_total,
    CASE
        WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
        WHEN r_score >= 3 AND f_score >= 3 THEN 'Loyal Customers'
        WHEN r_score >= 4 AND f_score <= 2 THEN 'New Customers'
        WHEN r_score <= 2 AND f_score >= 3 THEN 'At Risk'
        WHEN r_score <= 2 AND f_score <= 2 THEN 'Lost'
        ELSE 'Potential'
    END AS customer_segment
FROM rfm_scored
ORDER BY rfm_total DESC;