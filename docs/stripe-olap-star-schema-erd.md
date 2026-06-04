erDiagram
    dim_date {
        INT date_sk PK
        DATE full_date
        INT year
        INT month
        INT quarter
        BOOLEAN is_weekend
    }

    dim_customer {
        INT customer_sk PK
        UUID customer_id
        VARCHAR segment
        CHAR2 country
        VARCHAR tier
        DATE valid_from
        DATE valid_to
        BOOLEAN is_current
    }

    dim_merchant {
        INT merchant_sk PK
        UUID merchant_id
        VARCHAR name
        CHAR2 country_code
        VARCHAR region
        VARCHAR category
        VARCHAR risk_level
        DATE valid_from
        DATE valid_to
        BOOLEAN is_current
    }

    dim_currency {
        INT currency_sk PK
        CHAR3 code
        NUMERIC rate_usd
        DATE valid_from
    }

    dim_payment_method {
        INT method_sk PK
        VARCHAR method_name
        VARCHAR category
    }

    fact_transactions {
        INT tx_sk PK
        INT date_sk FK
        INT merchant_sk FK
        INT customer_sk FK
        INT currency_sk FK
        INT method_sk FK
        NUMERIC amount_usd
        VARCHAR status
        BOOLEAN is_fraud
        NUMERIC fraud_score
        VARCHAR device_type
        CHAR2 ip_country
    }

    dim_date ||--o{ fact_transactions : "date_sk"
    dim_customer ||--o{ fact_transactions : "customer_sk"
    dim_merchant ||--o{ fact_transactions : "merchant_sk"
    dim_currency ||--o{ fact_transactions : "currency_sk"
    dim_payment_method ||--o{ fact_transactions : "method_sk"