-- ============================================================
-- SQL/OLAP: Stripe Analytical Model (Snowflake)
-- ============================================================
--
-- ⚠️  THE SCD2 RULE THAT DRIVES THIS FILE
--
--   fact_transactions stores merchant_sk / customer_sk — the surrogate key
--   of the dimension version that was CURRENT AT LOAD TIME. The SK already
--   carries the history. Therefore:
--
--       JOIN dim_merchant m ON f.merchant_sk = m.merchant_sk
--       WHERE m.is_current = true          -- ❌ BUG
--
--   The moment a merchant is upgraded startup → growth, its old version
--   flips is_current = false, and EVERY historical fact row pointing at that
--   old SK is silently dropped from the aggregate. Last year's revenue
--   changes retroactively. This defeats the entire purpose of SCD Type 2.
--
--   Correct:
--     - join by SURROGATE key  → no is_current filter (point-in-time truth)
--     - join by NATURAL   key  → is_current = true (current-state reporting)
--
--   Both are legitimate; they answer different questions. Every query below
--   states which one it uses. This was the #1 correctness bug in v1.
-- ============================================================

-- ── DIMENSIONS ───────────────────────────────────────────────

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
    merchant_id         VARCHAR(36)  NOT NULL,   -- customers are merchant-scoped
    country             CHAR(2),
    segment             VARCHAR(30),
    acquisition_channel VARCHAR(50),
    is_erased           BOOLEAN DEFAULT false,   -- GDPR tombstone propagated from OLTP
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

-- ADDED: docs/03 listed dim_currency but the DDL never created it.
-- Doc and code now agree. Rates are SCD2 — a report re-run in 2027 must
-- reuse the 2024 rate, not today's.
CREATE TABLE dim_currency (
    currency_sk     INT           PRIMARY KEY AUTOINCREMENT,
    currency_code   CHAR(3)       NOT NULL,
    currency_name   VARCHAR(100),
    usd_rate        NUMERIC(18,6) NOT NULL,
    effective_from  DATE,
    effective_to    DATE,
    is_current      BOOLEAN DEFAULT true
);

-- ADDED: the brief requires "Product Performance Metrics" (OLAP data source)
-- and "Product Catalogs" (reference data). Neither was modelled.
CREATE TABLE dim_product (
    product_sk      INT          PRIMARY KEY AUTOINCREMENT,
    product_id      VARCHAR(36)  NOT NULL,
    merchant_id     VARCHAR(36)  NOT NULL,
    name            VARCHAR(255),
    category_code   VARCHAR(30),
    category_label  VARCHAR(100),
    mcc             CHAR(4),
    is_recurring    BOOLEAN,
    effective_from  DATE,
    effective_to    DATE,
    is_current      BOOLEAN DEFAULT true
);

-- ADDED: subscription management (brief, Business Scenario §1).
CREATE TABLE dim_subscription (
    subscription_sk      INT          PRIMARY KEY AUTOINCREMENT,
    subscription_id      VARCHAR(36)  NOT NULL,
    merchant_id          VARCHAR(36)  NOT NULL,
    customer_sk          INT,
    product_sk           INT,
    status               VARCHAR(20),
    billing_interval     VARCHAR(10),
    interval_count       SMALLINT,
    started_at           DATE,
    canceled_at          DATE,
    effective_from       DATE,
    effective_to         DATE,
    is_current           BOOLEAN DEFAULT true
);

-- ── FACTS ────────────────────────────────────────────────────

CREATE TABLE fact_transactions (
    transaction_sk    BIGINT        PRIMARY KEY AUTOINCREMENT,
    transaction_id    VARCHAR(36)   NOT NULL,
    date_sk           INT           REFERENCES dim_date(date_sk),
    merchant_sk       INT           REFERENCES dim_merchant(merchant_sk),
    customer_sk       INT           REFERENCES dim_customer(customer_sk),
    payment_sk        INT           REFERENCES dim_payment_method(payment_sk),
    currency_sk       INT           REFERENCES dim_currency(currency_sk),
    product_sk        INT           REFERENCES dim_product(product_sk),
    subscription_sk   INT           REFERENCES dim_subscription(subscription_sk),
    -- Degenerate dimensions: refund lineage carried on the fact itself.
    kind              VARCHAR(20)   NOT NULL DEFAULT 'payment',
    parent_transaction_id VARCHAR(36),
    amount_usd        NUMERIC(18,4) NOT NULL,
    original_amount   NUMERIC(18,4),
    original_currency CHAR(3),
    is_fraud          BOOLEAN       DEFAULT false,
    fraud_score       NUMERIC(5,4),
    status            VARCHAR(20),
    device_type       VARCHAR(20),
    ip_country        CHAR(2),
    created_at        TIMESTAMP_NTZ NOT NULL
)
-- Snowflake has no user-defined partitions: it has immutable micro-partitions
-- plus a clustering key. docs/05 previously claimed "partitioned by
-- transaction_date (daily)" — that column does not exist and the concept does
-- not apply. Clustering key aligned with the real filter pattern
-- (date range first, then merchant).
CLUSTER BY (date_sk, merchant_sk);

-- ADDED: MRR / churn are unanswerable from a status column alone.
CREATE TABLE fact_subscription_events (
    event_sk         BIGINT        PRIMARY KEY AUTOINCREMENT,
    event_id         BIGINT        NOT NULL,
    date_sk          INT           REFERENCES dim_date(date_sk),
    merchant_sk      INT           REFERENCES dim_merchant(merchant_sk),
    subscription_sk  INT           REFERENCES dim_subscription(subscription_sk),
    customer_sk      INT           REFERENCES dim_customer(customer_sk),
    product_sk       INT           REFERENCES dim_product(product_sk),
    event_type       VARCHAR(30)   NOT NULL,
    from_status      VARCHAR(20),
    to_status        VARCHAR(20),
    mrr_delta_usd    NUMERIC(18,4),
    occurred_at      TIMESTAMP_NTZ NOT NULL
)
CLUSTER BY (date_sk, merchant_sk);

-- ============================================================
-- PRE-AGGREGATIONS — Snowflake Dynamic Tables
-- ============================================================
-- Why DYNAMIC TABLE and not MATERIALIZED VIEW?
--   Snowflake MATERIALIZED VIEW is restricted to single-table, non-aggregated
--   projections: no joins, no GROUP BY. Every aggregate below needs both.
--   DYNAMIC TABLE (GA 2024) supports full SQL with declarative TARGET_LAG
--   and incremental refresh.
--
-- TARGET_LAG is the single source of truth for freshness. The Airflow DAG
-- no longer issues ALTER ... REFRESH: declaring a lag AND forcing a manual
-- refresh are two contradictory contracts, and the manual one silently
-- doubles the warehouse bill. Airflow now only ASSERTS the lag is met.
--
-- Freshness contract (aligned across README, docs/05 and this file):
--   mv_daily_revenue     : 1 hour  — operational merchant dashboards
--   mv_customer_monthly  : 1 day   — finance/segmentation reporting
--   mv_subscription_mrr  : 1 hour  — revenue-critical
-- ============================================================

CREATE OR REPLACE DYNAMIC TABLE mv_daily_revenue
    TARGET_LAG = '1 hour'
    WAREHOUSE  = 'ANALYTICS_WH'
AS
SELECT
    d.full_date,
    f.merchant_sk,
    m.merchant_id,
    m.name                                          AS merchant_name,
    m.region,
    m.tier,
    COUNT(*)                                        AS total_transactions,
    -- Refunds and chargebacks are NEGATIVE revenue. v1 summed every row
    -- regardless of kind, inflating gross revenue by the refund amount.
    SUM(IFF(f.kind = 'payment', f.amount_usd, 0))   AS gross_revenue_usd,
    SUM(IFF(f.kind IN ('refund','chargeback'), f.amount_usd, 0)) AS refunded_usd,
    SUM(IFF(f.kind = 'payment', f.amount_usd, -f.amount_usd))    AS net_revenue_usd,
    AVG(IFF(f.kind = 'payment', f.amount_usd, NULL)) AS avg_transaction_usd,
    SUM(IFF(f.is_fraud, 1, 0))                      AS fraud_count,
    SUM(IFF(f.is_fraud, f.amount_usd, 0))           AS fraud_amount_usd,
    SUM(IFF(f.kind = 'refund', 1, 0))               AS refund_count,
    SUM(IFF(f.kind = 'chargeback', 1, 0))           AS chargeback_count
FROM fact_transactions f
JOIN dim_date     d ON f.date_sk     = d.date_sk
JOIN dim_merchant m ON f.merchant_sk = m.merchant_sk
-- NO is_current filter: joining by merchant_sk already resolves the
-- point-in-time merchant version. Filtering here would erase the revenue
-- of every merchant that has ever changed tier.
WHERE f.status = 'success'
GROUP BY 1, 2, 3, 4, 5, 6;

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
WHERE f.status = 'success'
  AND f.kind   = 'payment'
  AND c.is_erased = false        -- GDPR: erased customers leave the segmentation
GROUP BY 1, 2, 3, 4;

-- ADDED: monthly recurring revenue, expansion, contraction, churn.
CREATE OR REPLACE DYNAMIC TABLE mv_subscription_mrr
    TARGET_LAG = '1 hour'
    WAREHOUSE  = 'ANALYTICS_WH'
AS
SELECT
    d.year,
    d.month,
    m.merchant_id,
    m.name AS merchant_name,
    p.category_label,
    SUM(IFF(e.event_type = 'created',    e.mrr_delta_usd, 0)) AS new_mrr_usd,
    SUM(IFF(e.event_type = 'upgraded',   e.mrr_delta_usd, 0)) AS expansion_mrr_usd,
    SUM(IFF(e.event_type = 'downgraded', e.mrr_delta_usd, 0)) AS contraction_mrr_usd,
    SUM(IFF(e.event_type = 'canceled',   e.mrr_delta_usd, 0)) AS churned_mrr_usd,
    SUM(e.mrr_delta_usd)                                      AS net_mrr_movement_usd,
    COUNT(DISTINCT IFF(e.event_type = 'canceled', e.subscription_sk, NULL)) AS churned_subs
FROM fact_subscription_events e
JOIN dim_date         d ON e.date_sk     = d.date_sk
JOIN dim_merchant     m ON e.merchant_sk = m.merchant_sk
LEFT JOIN dim_product p ON e.product_sk  = p.product_sk
GROUP BY 1, 2, 3, 4, 5;
