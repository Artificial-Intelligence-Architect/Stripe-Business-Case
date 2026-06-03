{% snapshot merchant_snapshot %}
{{
  config(
    target_schema           = 'snapshots',
    unique_key              = 'merchant_id',
    strategy                = 'check',
    check_cols              = ['name', 'country', 'region', 'tier'],
    invalidate_hard_deletes = true
  )
}}
select merchant_id, name, country, region, tier, updated_at
from {{ source('raw', 'raw_merchants') }}
{% endsnapshot %}
