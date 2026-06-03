-- ============================================================
-- SQL/OLAP: Stripe Analytical Model (Snowflake)
-- ============================================================

CREATE TABLE dim_date (
    date_sk         INT          PRIMARY KEY,
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
    merchant_id     VARCHAR(36)  NOT NULL,
    name            VARCHAR(255),
    country         CHAR(2),
    region          VARCHAR(50),
    tier            VARCHAR(20),
    effective_from  DATE,
    effective_to    DATE,
    is_current      BOOLEAN DEFAULT true
);

CREATE TABLE dim_customer (
    customer_sk         INT          PRIMARY KEY AUTOINCREMENT,
    customer_id         VARCHAR(36)  NOT NULL,
    country             CHAR(2),
    segment             VARCHAR(30),
    acquisition_channel VARCHAR(50),
    effective_from      DATE,
    effective_to        DATE,
    is_current          BOOLEAN DEFAULT true
);

CREATE TABLE dim_payment_method (
    payment_sk  INT         PRIMARY KEY AUTOINCREMENT,
    method      VARCHAR(50),
    provider    VARCHAR(50),
    card_type   VARCHAR(20)
);

CREATE TABLE fact_transactions (
    transaction_sk    BIGINT        PRIMARY KEY AUTOINCREMENT,
    transaction_id    VARCHAR(36)   NOT NULL,
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

-- ── Pre-aggregations (Snowflake Dynamic Tables) ──────────────
-- Why DYNAMIC TABLE instead of CREATE MATERIALIZED VIEW?
-- Snowflake MATERIALIZED VIEW is limited to single-table,
-- non-aggregated projections — incompatible with joins + GROUP BY.
-- DYNAMIC TABLE (GA since 2024): full SQL, TARGET_LAG, incremental refresh.

CREATE OR REPLACE DYNAMIC TABLE mv_daily_revenue
    TARGET_LAG = '1 hour'
    WAREHOUSE  = 'ANALYTICS_WH'
AS
SELECT
    d.full_date,
    m.name                                        AS merchant_name,
    m.region,
    m.tier,
    COUNT(*)                                      AS total_transactions,
    SUM(f.amount_usd)                             AS total_revenue_usd,
    AVG(f.amount_usd)                             AS avg_transaction_usd,
    SUM(IFF(f.is_fraud, 1, 0))                    AS fraud_count,
    SUM(IFF(f.is_fraud, f.amount_usd, 0))         AS fraud_amount_usd,
    SUM(IFF(f.status = 'refunded', 1, 0))         AS refund_count
FROM fact_transactions f
JOIN dim_date     d ON f.date_sk     = d.date_sk
JOIN dim_merchant m ON f.merchant_sk = m.merchant_sk
WHERE m.is_current = true
GROUP BY 1, 2, 3, 4;

CREATE OR REPLACE DYNAMIC TABLE mv_customer_monthly
    TARGET_LAG = '1 day'
    WAREHOUSE  = 'ANALYTICS_WH'
AS
SELECT
    d.year,
    d.month,
    c.segment,
    c.country,
    COUNT(DISTINCT f.customer_sk) AS active_customers,
    SUM(f.amount_usd)             AS total_spend_usd,
    AVG(f.amount_usd)             AS avg_order_value
FROM fact_transactions f
JOIN dim_date     d ON f.date_sk     = d.date_sk
JOIN dim_customer c ON f.customer_sk = c.customer_sk
WHERE f.status    = 'success'
  AND c.is_current = true
GROUP BY 1, 2, 3, 4;
