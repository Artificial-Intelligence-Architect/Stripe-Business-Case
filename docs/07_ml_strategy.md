# 07 — Machine Learning Integration Strategy

## Objective

The ML layer supports real-time fraud detection, customer behaviour analysis and predictive analytics.

The objective is not to build a production ML platform, but to define how machine learning integrates with the proposed data architecture.

## Main Use Case: Fraud Detection

Fraud detection is the primary ML use case.

The system scores transactions using:

- transaction amount
- transaction velocity
- merchant risk level
- customer behaviour
- device consistency
- IP geolocation risk
- historical fraud patterns

## Feature Sources

| Feature | Source | Window |
|---|---|---|
| `velocity_1h` | Kafka / streaming layer | rolling 1 hour |
| `amount_zscore` | PostgreSQL / analytical history | 30 days |
| `merchant_fraud_rate_7d` | MongoDB / OLAP | 7 days |
| `customer_avg_amount_30d` | OLAP | 30 days |
| `device_fingerprint_match` | MongoDB sessions | current session |
| `ip_country_match` | OLTP transaction metadata | current transaction |

## ML Workflow

1. Transaction event is received.
2. Features are extracted from streaming and historical sources.
3. A fraud score is generated.
4. The score is written back to the transactional flow.
5. High-risk transactions are blocked or sent for review.
6. Fraud events are stored in MongoDB.
7. Aggregated results are made available in the OLAP layer.

## Model Lifecycle

| Stage                 | Tooling                           |
|-----------------------|-----------------------------------|
| Feature engineering   | Python                            |
| Training              | scikit-learn / XGBoost            |
| Experiment tracking   | MLflow target architecture        |
| Serving               | Python API target architecture    |
| Monitoring            | Evidently AI target architecture  |
| Reporting             | Airflow and analytical marts      |

## Monitoring Strategy

The model is monitored for:

- inference latency
- prediction distribution drift
- feature drift
- fraud score threshold performance
- false positives
- false negatives
- business impact

## Retraining Strategy

Retraining may be triggered by:

- data drift
- degraded model performance
- new fraud patterns
- scheduled review
- compliance or business rule changes

## Implementation Scope

The project includes Python-based feature engineering and monitoring artefacts.

Enterprise tools such as Feast, MLflow and Evidently are documented as target-state components and are not mandatory for the local demonstration.

## Trade-offs

| Choice                    | Benefit                           | Trade-off                                 |
|---------------------------|-----------------------------------|-------------------------------------------|
| Python workflow           | understandable and maintainable   | not a full production serving platform    |
| MongoDB feature storage   | flexible fraud signals            | requires governance                       |
| Real-time scoring         | fast fraud response               | operational complexity                    |

## Alignment with Project Requirements

This strategy satisfies the ML requirements by covering:

- feature extraction
- fraud detection
- predictive analytics
- model deployment strategy
- model monitoring
- retraining approach