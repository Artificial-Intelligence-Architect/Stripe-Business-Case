# 07 — Machine Learning Integration Strategy

## Overview

This document describes the MLOps lifecycle for integrating real‑time fraud detection, personalisation and predictive analytics into Stripe’s data architecture. The strategy relies on a feature store (Feast), model serving (KServe/SageMaker), and continuous monitoring (Evidently AI).

## 1. Model Lifecycle

### 1.1 Training (offline)
- **Data sources**: features prepared in Feast from Snowflake (aggregated transaction features) and MongoDB (user session features).
- **Tools**: Jupyter notebooks for exploration → production script with `scikit-learn` / `XGBoost` / `PyTorch`.
- **Tracking**: MLflow records hyperparameters, metrics (AUC, precision, recall, F1) and the model binary.

### 1.2 Validation and staging
- **Validation**: time‑series cross‑validation to prevent data leakage.
- **Promotion threshold**: AUC > 0.85 on the last 7 days of holdout data.
- **Staging environment**: model deployed to a FastAPI endpoint (same code as production), fed by a Kafka topic `fraud-events-staging`.

### 1.3 Production deployment
- **Current model**: served via KServe (or SageMaker) with auto‑scaling.
- **Shadow mode**: for 24 hours, every request is duplicated to the new model without affecting the final decision. Scores are compared with the incumbent model.
- **Traffic switch**: if score correlation > 0.99, progressively shift traffic using Istio weighted routing.

### 1.4 Continuous monitoring
- **Input drift**: compare live feature distributions with training distributions using Evidently AI. Alert if Kolmogorov–Smirnov statistic > 0.05.
- **Output drift**: track the average fraud score per hour – must stay within a confidence interval.
- **Performance**: daily precision/recall computation on confirmed chargeback data (7‑day latency).
- **Alerting**: PagerDuty notification if precision < 0.75 or drift threshold exceeded.

### 1.5 Retraining and versioning
- **Automatic retraining**: Airflow DAG triggered weekly or when drift exceeds a threshold.
- **Versioning**: all models are tagged in the MLflow registry (`stripe/fraud_model:v2.3.0`).
- **Rollback**: Ansible script to revert to previous version in under 2 minutes.

## 2. Real‑time Fraud Detection Example

The following Faust worker consumes transaction events and calls the production model:

```python
# real_time_fraud_detection.py
import mlflow.pyfunc
from faust import App

app = App('fraud-detector', broker='kafka://localhost:9092')
model = mlflow.pyfunc.load_model("models:/stripe_fraud_model/production")

@app.agent(app.topic('raw_transactions'))
async def score_transactions(stream):
    async for event in stream:
        features = await extract_features(event)   # from Feast
        score = model.predict([features])[0]
        if score > 0.85:
            await send_to_kafka('fraud_alerts', event)
```

## 3. Business Performance Metrics
Metric	Target	Measurement
False positive rate	< 2% of transactions	Daily SQL on Snowflake
Detection rate (recall)	≥ 90% of true frauds	Weekly chargeback reconciliation
p99 latency	< 100 ms	From transaction timestamp to decision

## 4. Model Monitoring Dashboard

Suggested Grafana panels:

    Drift scores per feature (KS statistic)

    Average fraud score over time

    Model prediction count & latency percentiles

    Alert history (PagerDuty)

This strategy ensures that machine learning models remain accurate, safe and auditable in Stripe’s high‑throughput financial environment.