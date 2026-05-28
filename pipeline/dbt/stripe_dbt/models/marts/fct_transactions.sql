-- Table de faits pour le schéma en étoile
select
    transaction_id,
    date_sk, -- peut être généré depuis created_at via une macro
    merchant_id,
    customer_id,
    payment_method,
    amount_usd,
    original_amount,
    original_currency,
    is_fraud,
    fraud_score,
    status,
    device_type,
    ip_country
from {{ ref('int_transactions_enriched') }}
-- jointure avec dim_date pour récupérer date_sk (à adapter)