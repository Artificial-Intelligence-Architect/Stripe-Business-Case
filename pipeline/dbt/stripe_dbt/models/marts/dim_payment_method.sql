{{
  config(materialized = 'table', tags = ['daily'])
}}

select
    {{ dbt_utils.generate_surrogate_key(['method', 'provider']) }} as payment_sk,
    method,
    provider,
    card_type
from {{ ref('payment_methods') }}
