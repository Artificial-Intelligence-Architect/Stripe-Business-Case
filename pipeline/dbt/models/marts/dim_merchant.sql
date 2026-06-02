{{
  config(
    materialized = 'table',
    tags         = ['daily']
  )
}}

/*
  SCD Type 2 — each change to merchant attributes creates a new row.
  is_current = true identifies the active record.
  Snapshot managed by dbt snapshot (snapshots/merchant_snapshot.sql).
  This model reads from the snapshot output.
*/

with snapshot as (
    select * from {{ ref('merchant_snapshot') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['merchant_id', 'dbt_updated_at']) }}
                                        as merchant_sk,
        merchant_id,
        name,
        country,
        region,
        tier,
        dbt_valid_from::date            as effective_from,
        dbt_valid_to::date              as effective_to,
        case when dbt_valid_to is null
             then true else false end   as is_current
    from snapshot
)

select * from final
