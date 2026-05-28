-- Joint les référentiels pour enrichir avant agrégation
select
    t.transaction_id,
    t.merchant_id,
    m.name as merchant_name,
    m.country as merchant_country,
    m.tier as merchant_tier,
    t.customer_id,
    c.country as customer_country,
    c.segment as customer_segment,
    t.amount_usd,
    t.status,
    t.fraud_score,
    t.created_at
from {{ ref('stg_transactions') }} t
left join {{ source('raw', 'raw_merchants') }} m on t.merchant_id = m.merchant_id
left join {{ source('raw', 'raw_customers') }} c on t.customer_id = c.customer_id