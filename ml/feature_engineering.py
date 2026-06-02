"""
Feature Engineering — Fraud Detection (PySpark)
================================================
Computes dynamic features over sliding time windows.
All 7 features documented in README are implemented here.

Fix vs previous version:
  - Added device_fingerprint_match (feature #6)
  - Added merchant_fraud_rate_7d   (feature #7)
  - Added null-safe division to avoid ZeroDivisionError
  - Renamed avg_amount_30d → avg_txn_amount_30d (README alignment)
"""

from pyspark.sql import functions as F, Window


def compute_fraud_features(df_transactions, df_merchant_stats=None):
    """
    Parameters
    ----------
    df_transactions : DataFrame
        Raw transactions with columns:
        transaction_id, customer_id, merchant_id, amount_usd,
        created_at, ip_country, card_country, device_fingerprint,
        known_device_fingerprint (last known fingerprint per customer)

    df_merchant_stats : DataFrame, optional
        Pre-computed merchant fraud rates (merchant_id, fraud_rate_7d).
        If None, merchant_fraud_rate_7d is set to null.

    Returns
    -------
    DataFrame with one row per transaction + 7 fraud features.
    """

    # ── Time windows (seconds-based rangeBetween) ────────────
    w_cust_24h = (Window
                  .partitionBy("customer_id")
                  .orderBy(F.col("created_at").cast("long"))
                  .rangeBetween(-86_400, 0))

    w_cust_30d = (Window
                  .partitionBy("customer_id")
                  .orderBy(F.col("created_at").cast("long"))
                  .rangeBetween(-2_592_000, 0))

    w_cust_7d  = (Window
                  .partitionBy("customer_id")
                  .orderBy(F.col("created_at").cast("long"))
                  .rangeBetween(-604_800, 0))

    # ── Feature 1: transaction velocity (24 h) ───────────────
    df = df_transactions.withColumn(
        "txn_count_24h",
        F.count("transaction_id").over(w_cust_24h)
    )

    # ── Feature 2: average transaction amount (30 d) ─────────
    df = df.withColumn(
        "avg_txn_amount_30d",
        F.avg("amount_usd").over(w_cust_30d)
    )

    # ── Feature 3: amount anomaly ratio ──────────────────────
    # Null-safe: if avg is 0 or null, ratio = null (not inf)
    df = df.withColumn(
        "amount_ratio_30d",
        F.when(
            F.col("avg_txn_amount_30d") > 0,
            F.col("amount_usd") / F.col("avg_txn_amount_30d")
        ).otherwise(F.lit(None))
    )

    # ── Feature 4: distinct countries (30 d) ─────────────────
    df = df.withColumn(
        "distinct_countries_30d",
        F.countDistinct("ip_country").over(w_cust_30d)
    )

    # ── Feature 5: geo mismatch (IP country ≠ card country) ──
    df = df.withColumn(
        "geo_mismatch",
        (F.col("ip_country") != F.col("card_country")).cast("integer")
    )

    # ── Feature 6: device fingerprint match ──────────────────
    # 1 = known device, 0 = new/unknown device
    # known_device_fingerprint = last fingerprint seen for customer
    # (pre-joined upstream from the user_sessions MongoDB collection)
    df = df.withColumn(
        "device_fingerprint_match",
        (F.col("device_fingerprint") == F.col("known_device_fingerprint"))
        .cast("integer")
    )

    # ── Feature 7: merchant fraud rate (7 d) ─────────────────
    # Ratio of fraud_count / total_count for the merchant
    # over a rolling 7-day window — computed as a separate
    # batch job and joined here to avoid O(n²) window on merchant.
    if df_merchant_stats is not None:
        df = df.join(
            df_merchant_stats.select("merchant_id", "fraud_rate_7d"),
            on="merchant_id",
            how="left"
        ).withColumnRenamed("fraud_rate_7d", "merchant_fraud_rate_7d")
    else:
        df = df.withColumn("merchant_fraud_rate_7d", F.lit(None).cast("double"))

    # ── Final feature vector ──────────────────────────────────
    feature_cols = [
        "transaction_id",
        "customer_id",
        "merchant_id",
        "amount_usd",
        "txn_count_24h",            # velocity
        "avg_txn_amount_30d",       # baseline amount
        "amount_ratio_30d",         # amount anomaly
        "distinct_countries_30d",   # geographic spread
        "geo_mismatch",             # IP vs card country
        "device_fingerprint_match", # known device  ← was missing
        "merchant_fraud_rate_7d",   # merchant risk ← was missing
    ]

    return df.select(feature_cols)


def compute_merchant_fraud_rates(df_transactions):
    """
    Computes merchant_fraud_rate_7d as a standalone batch job.
    Output is joined into compute_fraud_features() above.

    Separated from the main window to avoid an expensive
    merchant-level window scan on every transaction row.
    """
    w_merch_7d = (Window
                  .partitionBy("merchant_id")
                  .orderBy(F.col("created_at").cast("long"))
                  .rangeBetween(-604_800, 0))

    return (df_transactions
            .withColumn(
                "is_fraud_int",
                F.col("is_fraud").cast("integer")
            )
            .withColumn(
                "fraud_rate_7d",
                F.when(
                    F.count("transaction_id").over(w_merch_7d) > 0,
                    F.sum("is_fraud_int").over(w_merch_7d)
                    / F.count("transaction_id").over(w_merch_7d)
                ).otherwise(F.lit(0.0))
            )
            .select("merchant_id", "fraud_rate_7d")
            .dropDuplicates(["merchant_id"]))
