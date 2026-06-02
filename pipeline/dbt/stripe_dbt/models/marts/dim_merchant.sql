select
    merchant_id,
    name,
    country,
    tier,
    current_date() as effective_from,
    null as effective_to,
    true as is_current
from {{ source('raw', 'raw_merchants') }}