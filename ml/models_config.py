"""
ML Models Configuration for Stripe Fraud Detection Platform
"""

MODELS = {
    "fraud_detection": {
        "algorithm": "XGBoost",
        "metric": "AUC 0.93",
        "latency": "< 100ms",
        "business_impact": "-40% fraud ($15M/year)",
        "features": [
            "velocity_1h",
            "amount_zscore",
            "ip_country_match",
            "device_fingerprint_match"
        ],
        "retraining_frequency": "weekly",
        "monitoring": "Evidently AI (drift detection)"
    },
    "churn_prediction": {
        "algorithm": "LightGBM",
        "metric": "AUC 0.87",
        "latency": "< 200ms",
        "business_impact": "+12% retention ($25M/year)",
        "features": [
            "customer_avg_amount_30d",
            "merchant_fraud_rate_7d",
            "txn_count_7d",
            "days_since_last_txn"
        ],
        "retraining_frequency": "monthly",
        "monitoring": "Evidently AI"
    },
    "customer_ltv": {
        "algorithm": "XGBoost Regressor",
        "metric": "R² 0.82",
        "latency": "< 500ms",
        "business_impact": "Marketing optimisation",
        "features": [
            "customer_avg_amount_30d",
            "txn_count_30d",
            "customer_age_days",
            "merchant_count"
        ],
        "retraining_frequency": "monthly",
        "monitoring": "MLflow"
    },
    "customer_segmentation": {
        "algorithm": "K-Means",
        "metric": "8 clusters",
        "latency": "< 1s",
        "business_impact": "+15% conversion",
        "features": [
            "recency_days",
            "frequency",
            "monetary_usd",
            "avg_basket_size"
        ],
        "retraining_frequency": "quarterly",
        "monitoring": "Silhouette score"
    }
}