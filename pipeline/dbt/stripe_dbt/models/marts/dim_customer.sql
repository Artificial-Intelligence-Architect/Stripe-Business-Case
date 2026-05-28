select
    customer_id,
    country,
    segment,
    current_date() as effective_from,
    null as effective_to,
    true as is_current
from {{ source('raw', 'raw_customers') }}