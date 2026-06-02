-- Fact table for the star schema
select
    transaction_id,
    date_sk, -- can be generated from created_at via a macro
    merchant_id,
    customer_id,
    payment_method,
    amount_usd,
    original_amount,
    original_currency,
    is_fraud,
    fraud_score,
    status,
    device_type,
    ip_country
from {{ ref('int_transactions_enriched') }}
-- join with dim_date to retrieve date_sk (to be adapted)