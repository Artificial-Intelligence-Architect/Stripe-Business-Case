{{
  config(
    materialized = 'table',
    tags         = ['daily']
  )
}}

/*
  Static dimension — payment methods don't change frequently.
  Seeded from seeds/payment_methods.csv.
*/

select
    {{ dbt_utils.generate_surrogate_key(['method', 'provider']) }}
                    as payment_sk,
    method,
    provider,
    card_type
from {{ ref('payment_methods') }}
