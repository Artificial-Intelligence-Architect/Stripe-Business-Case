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

## Second Use Case: Customer Personalisation & Recommendations

The brief names three ML use cases — fraud detection, **customer
personalisation**, and predictive analytics. Fraud is covered above; this
section covers personalisation, which shares the *same* data architecture. That
reuse is the point: one feature pipeline, one serving layer, two models.

### Why this belongs in the NoSQL layer

Personalisation reads exactly the data MongoDB already holds — clickstream in
`user_sessions`, sentiment in `customer_feedback` — joined to the purchase
history in OLAP. It needs a flexible, evolving feature shape (new signals get
added constantly), which is the document model's strength and the star schema's
weakness. So the recommender lives beside the fraud engine, not in a separate
stack.

### What it produces

For a merchant's checkout or dashboard: **product recommendations** ("customers
like this bought…"), **next-best-action** (upgrade prompt, retention offer), and
a **churn-risk / propensity score** that feeds the subscription retention flow.

### Feature sources (reusing the existing layers)

| Feature | Source | Why |
|---|---|---|
| `session_click_sequence` | MongoDB `user_sessions` | in-session intent signal |
| `category_affinity_90d` | OLAP `fact_transactions` × `dim_product` | what they actually buy |
| `avg_order_value` / `purchase_frequency` | OLAP (RFM inputs) | value + cadence |
| `sentiment_trend` | MongoDB `customer_feedback` | satisfaction trajectory |
| `subscription_status` / `mrr_delta` | OLAP `fact_subscription_events` | expansion vs churn signal |
| `days_since_last_purchase` | OLTP / OLAP | recency for propensity |

Note the split by lifecycle: **real-time** intent from MongoDB (this session),
**historical** taste from OLAP (90-day windows). This is the same OLTP+OLAP+NoSQL
integration the fraud model uses, which is precisely the "supports integration
with OLTP and OLAP systems" requirement of the NoSQL brief.

### Approach

A two-stage recommender: **collaborative filtering** (matrix factorisation /
ALS) for the "customers like you" candidate set, re-ranked by a **content-based**
layer using `category_affinity` and `sentiment_trend`. Cold-start (new customer,
no history) falls back to merchant-level popularity from
`mv_daily_revenue` — a recommender that returns nothing for a first-time visitor
is the classic failure, so the fallback is explicit.

### Serving

Precomputed recommendations are cached per customer in a MongoDB
`recommendations` collection (candidate list + scores + `model_version` +
`generated_at`), refreshed by a batch job and read at < 10 ms at checkout. Truly
real-time intent (current session clicks) re-ranks the cached list at request
time. This mirrors the fraud engine's "precompute heavy, decide light" pattern.

## Third Use Case: Predictive Analytics

Beyond scoring individual events, the platform supports forward-looking
questions on the OLAP layer:

- **Churn prediction** — the propensity model above, scored monthly against
  `fact_subscription_events`, surfaces at-risk subscriptions before they cancel.
- **Revenue / MRR forecasting** — time-series on `mv_subscription_mrr` (the MRR
  waterfall already computes new/expansion/contraction/churn deltas).
- **Fraud-rate trend** — the z-score anomaly query in `queries_analytics.sql` #7
  is the descriptive half; the predictive half extrapolates the 7-day rolling
  rate to flag merchants trending toward a threshold breach.

These are analytical models: they read from Snowflake, write predictions back to
a mart, and are consumed by dashboards — no real-time serving path required.

## Model Lifecycle

Applies to all three models (fraud, recommender, predictive):

| Stage                 | Tooling                           |
|-----------------------|-----------------------------------|
| Feature engineering   | Python (shared feature definitions) |
| Training              | scikit-learn / XGBoost (fraud, propensity) · implicit-ALS (recommender) |
| Experiment tracking   | MLflow target architecture        |
| Serving               | Python API (fraud, real-time re-rank) · batch cache (recommendations) |
| Monitoring            | Evidently AI target architecture  |
| Reporting             | Airflow and analytical marts      |

## Monitoring Strategy

All three models are monitored; the metrics that matter differ per model.

**Fraud** — inference latency, prediction/feature drift, threshold performance,
false positives/negatives, business impact (fraud caught vs good customers blocked).

**Recommender** — click-through and conversion on served recommendations,
coverage (share of catalog ever recommended — guards against collapse onto a few
popular items), cold-start fallback rate, staleness of the cached list.

**Predictive (churn/forecast)** — prediction error vs realised outcome
(e.g. predicted churn vs actual cancellation), calibration drift over time.

A model that is never measured on the *right* metric silently degrades; latency
alone tells you nothing about whether recommendations are still relevant.

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

The brief's ML section asks for three use cases and a full lifecycle. Mapping:

| Brief requirement | Covered by |
|---|---|
| Real-time **fraud detection** | Main use case — streaming features, sub-50ms scoring |
| Customer **personalisation** | Recommender — collaborative + content, MongoDB-served |
| **Predictive analytics** | Churn/propensity + MRR forecast on OLAP |
| Integrate ML **within the NoSQL system** | Features in `user_sessions`/`customer_feedback`; `recommendations` cached in MongoDB |
| **Feature extraction** | Shared Python feature definitions across OLTP/OLAP/NoSQL |
| **Model deployment** | Real-time API (fraud) + batch cache (recommendations) |
| **Monitoring & updating** | Per-model metrics + drift-triggered retraining |

The unifying design decision: **one feature and serving architecture, three
models.** Personalisation and predictive analytics are not bolted on — they reuse
the exact OLTP→OLAP→MongoDB integration built for fraud, which is what makes the
architecture "comprehensive" rather than fraud-only.