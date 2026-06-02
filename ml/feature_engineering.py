"""
Feature Engineering - Fraud Detection (PySpark)
Computes dynamic features over sliding time windows.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def compute_fraud_features(df_transactions):
    """
    Computes features for a fraud detection model.
    Each feature is calculated BEFORE the payment decision.
    """
    # Time windows per customer
    w_customer_24h = Window.partitionBy("customer_id") \
                           .orderBy("created_at") \
                           .rangeBetween(-86400, 0)  # 24 hours in seconds

    w_customer_30d = Window.partitionBy("customer_id") \
                           .orderBy("created_at") \
                           .rangeBetween(-2592000, 0)  # 30 days

    df_features = df_transactions.select(
        "transaction_id",
        "customer_id",
        "merchant_id",
        "amount_usd",

        # Velocity: number of transactions in the last 24 hours
        F.count("transaction_id").over(w_customer_24h)
         .alias("txn_count_24h"),

        # Average amount over the last 30 days
        F.avg("amount_usd").over(w_customer_30d)
         .alias("avg_amount_30d"),

        # Ratio of current amount to 30-day average (amount anomaly)
        (F.col("amount_usd") /
         F.avg("amount_usd").over(w_customer_30d))
         .alias("amount_ratio_30d"),

        # Number of distinct countries used (financial mule)
        F.countDistinct("ip_country").over(w_customer_30d)
         .alias("distinct_countries_30d"),

        # Geographical mismatch flag (IP vs card country)
        (F.col("ip_country") != F.col("card_country"))
         .cast("integer")
         .alias("geo_mismatch"),
    )

    return df_features