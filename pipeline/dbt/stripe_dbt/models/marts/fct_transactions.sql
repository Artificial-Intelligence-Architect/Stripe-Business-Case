{{
  config(
    materialized = 'incremental',
    unique_key   = 'transaction_id',
    cluster_by   = ['date_sk', 'merchant_sk'],
    tags         = ['daily']
  )
}}

select
    transaction_id,
    date_sk,
    merchant_sk,
    customer_sk,
    payment_sk,
    amount_usd,
    original_amount,
    original_currency,
    fraud_score,
    is_high_risk,
    is_fraud,
    status,
    device_type,
    ip_country,
    created_at
from {{ ref('int_transactions_enriched') }}

{% if is_incremental() %}
    where created_at > (select max(created_at) from {{ this }})
{% endif %}
