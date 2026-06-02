-- ============================================================
-- SQL/OLAP: Stripe Analytical Model (Snowflake)
-- ============================================================

-- ── DIMENSIONS ───────────────────────────────────────────────

CREATE TABLE dim_date (
    date_sk         INT          PRIMARY KEY,   -- YYYYMMDD format
    full_date       DATE         NOT NULL,
    year            SMALLINT,
    quarter         SMALLINT,
    month           SMALLINT,
    month_name      VARCHAR(10),
    week_of_year    SMALLINT,
    day_of_week     SMALLINT,
    day_name        VARCHAR(10),
    is_weekend      BOOLEAN,
    is_holiday      BOOLEAN DEFAULT false
);

CREATE TABLE dim_merchant (
    merchant_sk     INT          PRIMARY KEY AUTOINCREMENT,
    merchant_id     VARCHAR(36)  NOT NULL,       -- UUID from OLTP source
    name            VARCHAR(255),
    country         CHAR(2),
    region          VARCHAR(50),
    tier            VARCHAR(20),
    effective_from  DATE,                        -- SCD Type 2
    effective_to    DATE,
    is_current      BOOLEAN DEFAULT true
);

CREATE TABLE dim_customer (
    customer_sk     INT          PRIMARY KEY AUTOINCREMENT,
    customer_id     VARCHAR(36)  NOT NULL,
    country         CHAR(2),
    segment         VARCHAR(30),                 -- calculated by ML (high_value, at_risk, etc.)
    acquisition_channel VARCHAR(50),
    effective_from  DATE,
    effective_to    DATE,
    is_current      BOOLEAN DEFAULT true
);

CREATE TABLE dim_payment_method (
    payment_sk      INT          PRIMARY KEY AUTOINCREMENT,
    method          VARCHAR(50),                 -- card, bank_transfer, wallet
    provider        VARCHAR(50),                 -- Visa, Mastercard, PayPal...
    card_type       VARCHAR(20)                  -- debit, credit, prepaid
);

-- ── FACT TABLE ───────────────────────────────────────────────

CREATE TABLE fact_transactions (
    transaction_sk    BIGINT        PRIMARY KEY AUTOINCREMENT,
    transaction_id    VARCHAR(36)   NOT NULL,        -- business key
    date_sk           INT           REFERENCES dim_date(date_sk),
    merchant_sk       INT           REFERENCES dim_merchant(merchant_sk),
    customer_sk       INT           REFERENCES dim_customer(customer_sk),
    payment_sk        INT           REFERENCES dim_payment_method(payment_sk),
    amount_usd        NUMERIC(18,4) NOT NULL,
    original_amount   NUMERIC(18,4),
    original_currency CHAR(3),
    is_fraud          BOOLEAN       DEFAULT false,
    fraud_score       NUMERIC(5,4),
    status            VARCHAR(20),
    device_type       VARCHAR(20),
    ip_country        CHAR(2)
)
CLUSTER BY (date_sk, merchant_sk);

-- ── PRE-AGGREGATIONS (Snowflake Dynamic Tables) ──────────────
--
-- Why Dynamic Tables instead of CREATE MATERIALIZED VIEW?
-- Snowflake deprecated MATERIALIZED VIEW for complex aggregations
-- (joins, GROUP BY) in favour of DYNAMIC TABLE (GA since 2024),
-- which supports:
--   • Full SQL (joins, aggregations, window functions)
--   • Configurable TARGET_LAG (refresh SLA)
--   • Automatic incremental refresh where possible
--   • ALTER DYNAMIC TABLE … REFRESH for manual/forced refresh
--
-- MATERIALIZED VIEW in Snowflake is still supported but limited
-- to single-table, non-aggregated projections — unsuitable here.
-- ─────────────────────────────────────────────────────────────

-- 1. Daily revenue per merchant
--    TARGET_LAG = '1 hour': data is at most 1 hour stale.
--    Used by: operational dashboards, Airflow compliance report.
CREATE OR REPLACE DYNAMIC TABLE mv_daily_revenue
    TARGET_LAG = '1 hour'
    WAREHOUSE  = 'ANALYTICS_WH'
AS
SELECT
    d.full_date,
    m.name                                              AS merchant_name,
    m.region,
    m.tier,
    COUNT(*)                                            AS total_transactions,
    SUM(f.amount_usd)                                   AS total_revenue_usd,
    AVG(f.amount_usd)                                   AS avg_transaction_usd,
    COUNT(*) FILTER (WHERE f.is_fraud)                  AS fraud_count,
    SUM(f.amount_usd) FILTER (WHERE f.is_fraud)         AS fraud_amount_usd,
    COUNT(*) FILTER (WHERE f.status = 'refunded')       AS refund_count
FROM fact_transactions f
JOIN dim_date     d ON f.date_sk     = d.date_sk
JOIN dim_merchant m ON f.merchant_sk = m.merchant_sk
WHERE m.is_current = true
GROUP BY 1, 2, 3, 4;

-- 2. Monthly customer segmentation
--    TARGET_LAG = '1 day': refreshed once per day (batch workload).
--    Used by: marketing segmentation, churn prediction pipeline.
CREATE OR REPLACE DYNAMIC TABLE mv_customer_monthly
    TARGET_LAG = '1 day'
    WAREHOUSE  = 'ANALYTICS_WH'
AS
SELECT
    d.year,
    d.month,
    c.segment,
    c.country,
    COUNT(DISTINCT f.customer_sk)   AS active_customers,
    SUM(f.amount_usd)               AS total_spend_usd,
    AVG(f.amount_usd)               AS avg_order_value
FROM fact_transactions f
JOIN dim_date     d ON f.date_sk     = d.date_sk
JOIN dim_customer c ON f.customer_sk = c.customer_sk
WHERE f.status    = 'success'
  AND c.is_current = true
GROUP BY 1, 2, 3, 4;
