{% snapshot customer_snapshot %}

{{
  config(
    target_schema  = 'snapshots',
    unique_key     = 'customer_id',
    strategy       = 'check',
    check_cols     = ['country', 'segment', 'acquisition_channel'],
    invalidate_hard_deletes = true
  )
}}

select
    customer_id,
    country,
    segment,
    acquisition_channel,
    updated_at
from {{ source('raw', 'raw_customers') }}

{% endsnapshot %}
