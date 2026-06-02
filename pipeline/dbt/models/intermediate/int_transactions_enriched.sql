{{
  config(
    materialized = 'ephemeral'
  )
}}

/*
  Intermediate model — enriches staged transactions with
  dimension surrogate keys and derived business attributes.
  Ephemeral: compiled inline, no physical table created.
*/

with stg as (
    select * from {{ ref('stg_transactions') }}
),

dim_merchant as (
    select merchant_sk, merchant_id
    from {{ ref('dim_merchant') }}
    where is_current = true
),

dim_customer as (
    select customer_sk, customer_id
    from {{ ref('dim_customer') }}
    where is_current = true
),

dim_payment as (
    select payment_sk, method
    from {{ ref('dim_payment_method') }}
),

enriched as (
    select
        -- Business key (preserved for traceability)
        s.transaction_id,

        -- Surrogate keys for star schema joins
        s.date_sk,
        m.merchant_sk,
        c.customer_sk,
        p.payment_sk,

        -- Measures
        s.amount_usd,
        s.amount                as original_amount,
        s.currency              as original_currency,

        -- Fraud attributes
        s.fraud_score,
        s.is_high_risk,
        case when s.status = 'chargeback' or s.is_high_risk
             then true else false end                    as is_fraud,

        -- Operational attributes
        s.status,
        s.device_type,
        s.ip_country,

        -- Audit
        s.created_at

    from stg s
    left join dim_merchant m on s.merchant_id = m.merchant_id
    left join dim_customer c on s.customer_id = c.customer_id
    left join dim_payment  p on s.payment_method = p.method
)

select * from enriched
