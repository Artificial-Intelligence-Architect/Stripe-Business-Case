"""
Unit tests for fraud feature engineering (PySpark).
Uses a simplified merchant fraud rate function for test determinism.
The production version uses a sliding window (see ml/feature_engineering.py).
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, BooleanType
from pyspark.sql import functions as F
from datetime import datetime
from ml.feature_engineering import compute_fraud_features   # production function

@pytest.fixture(scope="session")
def spark():
    """Creates a local Spark session for testing."""
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()

# ----- Simplified merchant fraud rate (for tests only) -----
def _simple_merchant_fraud_rates(df_transactions):
    """Simplified version (groupBy + avg) used only for unit tests.
    The production implementation uses a rolling 7‑day window."""
    return (df_transactions
            .groupBy("merchant_id")
            .agg(F.avg(F.col("is_fraud").cast("double")).alias("fraud_rate_7d")))

def test_compute_fraud_features_basic(spark):
    """Test the main fraud feature engineering function with a small dataset."""
    schema = StructType([
        StructField("transaction_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("merchant_id", StringType()),
        StructField("amount_usd", DoubleType()),
        StructField("created_at", TimestampType()),
        StructField("ip_country", StringType()),
        StructField("card_country", StringType()),
        StructField("device_fingerprint", StringType()),
        StructField("known_device_fingerprint", StringType()),
        StructField("is_fraud", BooleanType())
    ])

    data = [
        ("tx1", "cust1", "merch1", 100.0, datetime(2024, 1, 1, 10, 0, 0), "FR", "FR", "devA", "devA", False),
        ("tx2", "cust1", "merch1", 200.0, datetime(2024, 1, 1, 10, 5, 0), "FR", "FR", "devA", "devA", False),
        ("tx3", "cust1", "merch1", 50.0,  datetime(2024, 1, 1, 10, 12, 0), "US", "FR", "devB", "devA", True),
    ]
    df_transactions = spark.createDataFrame(data, schema)

    # Use simplified merchant stats for test predictability
    df_merchant_stats = _simple_merchant_fraud_rates(df_transactions)

    result_df = compute_fraud_features(df_transactions, df_merchant_stats)
    results = result_df.orderBy("created_at").collect()

    # First transaction (tx1)
    assert results[0]["txn_count_24h"] == 1
    assert results[0]["avg_txn_amount_30d"] == 100.0
    assert results[0]["amount_ratio_30d"] == 1.0
    assert results[0]["distinct_countries_30d"] == 1
    assert results[0]["geo_mismatch"] == 0
    assert results[0]["device_fingerprint_match"] == 1
    assert round(results[0]["merchant_fraud_rate_7d"], 2) == 0.33

    # Second transaction (tx2)
    assert results[1]["txn_count_24h"] == 2
    assert round(results[1]["avg_txn_amount_30d"], 1) == 150.0
    assert round(results[1]["amount_ratio_30d"], 2) == round(200.0 / 150.0, 2)
    assert results[1]["distinct_countries_30d"] == 1
    assert results[1]["geo_mismatch"] == 0
    assert results[1]["device_fingerprint_match"] == 1
    assert round(results[1]["merchant_fraud_rate_7d"], 2) == 0.33

    # Third transaction (tx3)
    assert results[2]["txn_count_24h"] == 3
    assert round(results[2]["avg_txn_amount_30d"], 1) == round((100 + 200 + 50) / 3, 1)
    assert round(results[2]["amount_ratio_30d"], 2) == round(50.0 / 116.666, 2)
    assert results[2]["distinct_countries_30d"] == 2
    assert results[2]["geo_mismatch"] == 1
    assert results[2]["device_fingerprint_match"] == 0
    assert round(results[2]["merchant_fraud_rate_7d"], 2) == 0.33

def test_simple_merchant_fraud_rates(spark):
    """Test the simplified (groupBy) merchant fraud rate function used in tests."""
    schema = StructType([
        StructField("merchant_id", StringType()),
        StructField("created_at", TimestampType()),
        StructField("is_fraud", BooleanType()),
        StructField("transaction_id", StringType())
    ])
    data = [
        ("merch1", datetime(2024, 1, 1, 10, 0, 0), False, "t1"),
        ("merch1", datetime(2024, 1, 3, 10, 5, 0), False, "t2"),
        ("merch1", datetime(2024, 1, 10, 10, 10, 0), True, "t3"),
        ("merch2", datetime(2024, 1, 1, 10, 0, 0), False, "t4"),
    ]
    df = spark.createDataFrame(data, schema)
    result = _simple_merchant_fraud_rates(df)
    result_dict = {row["merchant_id"]: round(row["fraud_rate_7d"], 2) for row in result.collect()}
    assert result_dict["merch1"] == 0.33   # 1 fraud out of 3 transactions
    assert result_dict["merch2"] == 0.0