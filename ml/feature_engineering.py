"""
Feature Engineering - Détection de fraude (PySpark)
Calcule les features dynamiques sur des fenêtres temporelles glissantes.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def compute_fraud_features(df_transactions):
    """
    Calcule les features pour un modèle de détection de fraude.
    Chaque feature est calculée AVANT la décision de paiement.
    """
    # Fenêtres temporelles par client
    w_customer_24h = Window.partitionBy("customer_id") \
                           .orderBy("created_at") \
                           .rangeBetween(-86400, 0)  # 24 heures en secondes

    w_customer_30d = Window.partitionBy("customer_id") \
                           .orderBy("created_at") \
                           .rangeBetween(-2592000, 0)  # 30 jours

    df_features = df_transactions.select(
        "transaction_id",
        "customer_id",
        "merchant_id",
        "amount_usd",

        # Vélocité : nombre de transactions dans les dernières 24h
        F.count("transaction_id").over(w_customer_24h)
         .alias("txn_count_24h"),

        # Montant moyen des 30 derniers jours
        F.avg("amount_usd").over(w_customer_30d)
         .alias("avg_amount_30d"),

        # Ratio montant actuel / moyenne 30j (anomalie de montant)
        (F.col("amount_usd") /
         F.avg("amount_usd").over(w_customer_30d))
         .alias("amount_ratio_30d"),

        # Nombre de pays distincts utilisés (mule financière)
        F.countDistinct("ip_country").over(w_customer_30d)
         .alias("distinct_countries_30d"),

        # Flag mismatch géographique (IP vs pays de la carte)
        (F.col("ip_country") != F.col("card_country"))
         .cast("integer")
         .alias("geo_mismatch"),
    )

    return df_features