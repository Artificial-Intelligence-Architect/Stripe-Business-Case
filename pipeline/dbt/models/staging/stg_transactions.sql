{{
  config(
    materialized = 'view',
    tags         = ['daily']
  )
}}

with source as (
    select * from {{ source('raw', 'raw_transactions') }}
),

currencies as (
    select code, usd_rate
    from {{ source('raw', 'raw_currencies') }}
),

cleaned as (
    select
        -- Keys
        t.transaction_id::varchar(36)                       as transaction_id,
        t.merchant_id::varchar(36)                          as merchant_id,
        t.customer_id::varchar(36)                          as customer_id,

        -- Amounts
        t.amount::numeric(18,4)                             as amount,
        upper(trim(t.currency))                             as currency,
        case
            when upper(trim(t.currency)) = 'USD'
                then t.amount
            else round(t.amount * coalesce(c.usd_rate, 1), 4)
        end                                                 as amount_usd,

        -- Payment metadata
        lower(trim(t.payment_method))                       as payment_method,
        lower(trim(t.status))                               as status,
        lower(trim(t.device_type))                          as device_type,
        upper(trim(t.ip_country))                           as ip_country,

        -- Fraud signal
        t.fraud_score::numeric(5,4)                         as fraud_score,
        case when t.fraud_score >= 0.7 then true
             else false end                                 as is_high_risk,

        -- Timestamps
        t.created_at::timestamptz                           as created_at,
        to_date(to_char(t.created_at, 'YYYYMMDD'), 'YYYYMMDD')
                                                            as transaction_date,
        to_number(to_char(t.created_at, 'YYYYMMDD'), '99999999')::int
                                                            as date_sk

    from source t
    left join currencies c on upper(trim(t.currency)) = c.code

    -- Remove rows with critical nulls (defensive dedup)
    where t.transaction_id is not null
      and t.status         is not null
      and t.amount         > 0

    -- Deduplicate: keep the latest CDC event per transaction_id
    qualify row_number() over (
        partition by t.transaction_id
        order by t.created_at desc
    ) = 1
)

select * from cleaned
