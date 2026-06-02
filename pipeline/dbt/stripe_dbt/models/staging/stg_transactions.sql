select
    transaction_id,
    merchant_id,
    customer_id,
    amount,
    currency,
    case
        when currency = 'USD' then amount
        else amount * 1.0
    end as amount_usd,
    payment_method,
    status,
    device_type,
    ip_country,
    fraud_score,
    created_at
from {{ source('raw', 'raw_transactions') }}
where status is not null
