# Stripe Data Architecture — Full Technical Documentation

> **Audience**: Data Engineers, Data Scientists, AIA certification reviewers.
> **Reading time**: 2–3 hours (complete documentation).
> **Prerequisites**: SQL proficiency, familiarity with distributed systems, Python/PySpark basics.

---

## Table of Contents

1. [Global Architecture](#1-global-architecture)
2. [OLTP System — PostgreSQL](#2-oltp-system--postgresql)
3. [OLAP System — Snowflake](#3-olap-system--snowflake)
4. [NoSQL System — MongoDB](#4-nosql-system--mongodb)
5. [Data Pipeline](#5-data-pipeline)
6. [Security & Compliance](#6-security--compliance)
7. [ML Strategy](#7-ml-strategy)
8. [SQL & NoSQL Queries](#8-sql--nosql-queries)
9. [Observability](#9-observability)
10. [Operational Runbooks](#10-operational-runbooks)
11. [Architecture Decision Records (ADR)](#11-architecture-decision-records-adr)
12. [Business Impact](#12-business-impact)

---

## 1. Global Architecture

**Reference file**: [`docs/01_architecture.md`](../01_architecture.md)
**Diagrams**: [`docs/architecture_diagram.png`](../architecture_diagram.png) · [`docs/data_pipeline.png`](../data_pipeline.png)

### Hybrid Lambda Paradigm

The architecture combines two complementary processing modes:

- **Speed layer** (Kafka + Faust): real-time stream processing, latency < 100 ms, used for fraud detection and score updates.
- **Batch layer** (Airflow + dbt): full reprocessing of D−1 data, scheduled at 02:00 UTC, SLA H+1, used for OLAP aggregations and ML model training.
- **Serving layer**: three specialised storage systems (PostgreSQL, Snowflake, MongoDB) exposing data to the appropriate consumers.

### End-to-End Data Flow

```
Stripe Application
    │
    ▼
PostgreSQL (OLTP) ──── CDC Debezium (WAL pgoutput) ────► Kafka (Schema Registry Avro)
    │                                                          │
    │                                              ┌───────────┼───────────┐
    │                                              ▼           ▼           ▼
    │                                           Faust      Snowpipe      S3 Archive
    │                                          (fraud)    (Snowflake)  (compliance)
    │                                              │
    │                                    ┌─────────┴─────────┐
    │                                    ▼                   ▼
    │                               MongoDB             PostgreSQL
    │                            (fraud_events)      (fraud_score update)
    │
    └── Airflow DAG (02:00 UTC)
            │
            ▼
          dbt ──► Snowflake (star schema, SCD Type 2, Dynamic Tables)
```

---

## 2. OLTP System — PostgreSQL

**Reference file**: [`docs/02_oltp_model.md`](../02_oltp_model.md)
**SQL schema**: [`sql/oltp/schema.sql`](../../sql/oltp/schema.sql)
**Queries**: [`sql/oltp/queries.sql`](../../sql/oltp/queries.sql)
**ERD**: [`docs/erd_oltp.png`](../erd_oltp.png)

### Normalised Schema (3NF)

| Table             | Role               | Primary Key  | Partitioning                  |
|-------------------|--------------------|--------------|-------------------------------|
| `transactions`    | Central fact       | UUID         | Monthly range on `created_at` |
| `merchants`       | Merchant dimension | UUID         | —                             |
| `customers`       | Customer dimension | UUID         | —                             |
| `currencies`      | Currency reference | ISO 4217     | —                             |
| `payment_methods` | Payment types      | UUID         | —                             |

### Technical Choices

- **Range partitioning** on `created_at` (monthly): automatic partition pruning on temporal queries, per-partition VACUUM/ANALYSE maintenance.
- **Citus sharding** on `merchant_id`: horizontal distribution without application code changes, transaction colocation per merchant for local analytical queries.
- **Synchronous replication**: 1 primary + 2 replicas in quorum write → RPO = 0, RTO < 30 s via automatic failover.
- **Partial indices**: `WHERE status = 'pending'` on active transactions only, reducing index size by 60–70%.

### Integrity Constraints

```sql
-- Example: CHECK constraint on amount
ALTER TABLE transactions ADD CONSTRAINT chk_amount_positive CHECK (amount_usd > 0);

-- FK with ON DELETE RESTRICT to preserve the audit trail
ALTER TABLE transactions ADD CONSTRAINT fk_merchant
    FOREIGN KEY (merchant_id) REFERENCES merchants(id) ON DELETE RESTRICT;
```

---

## 3. OLAP System — Snowflake

**Reference file**: [`docs/03_olap_model.md`](../03_olap_model.md)
**SQL schema**: [`sql/olap/schema.sql`](../../sql/olap/schema.sql)
**Analytical queries**: [`sql/olap/queries_analytics.sql`](../../sql/olap/queries_analytics.sql)
**Star schema ERD**: [`docs/stripe-olap-star-schema-erd.md`](../stripe-olap-star-schema-erd.md)
**dbt models**: [`pipeline/dbt/stripe_dbt/models/`](../../pipeline/dbt/stripe_dbt/models/)

### Star Schema

```
                    ┌─────────────┐
                    │  dim_date   │
                    │  date_sk PK │
                    └──────┬──────┘
                           │
┌──────────────┐    ┌──────▼────────────┐    ┌─────────────────┐
│ dim_customer │    │ fact_transactions │    │  dim_merchant   │
│customer_sk PK├────┤ transaction_sk    ├────┤  merchant_sk PK │
│ SCD Type 2   │    │ amount_usd        │    │  SCD Type 2     │
└──────────────┘    │ fraud_score       │    └─────────────────┘
                    │ date_sk FK        │
┌──────────────┐    │ customer_sk FK    │    ┌─────────────────┐
│ dim_currency │    │ merchant_sk FK    │    │dim_payment_meth │
│currency_sk PK├────┤ currency_sk FK    ├────┤payment_meth_sk  │
└──────────────┘    │ payment_meth_sk   │    └─────────────────┘
                    └───────────────────┘
```

### SCD Type 2 — History Management

The `dim_customer` and `dim_merchant` dimensions implement SCD Type 2 via dbt snapshots:

```sql
-- customer_snapshot.sql
{% snapshot customer_snapshot %}
    {{
        config(
          target_schema='snapshots',
          unique_key='customer_id',
          strategy='timestamp',
          updated_at='updated_at',
        )
    }}
    SELECT * FROM {{ source('stripe', 'customers') }}
{% endsnapshot %}
```

Columns added automatically by dbt: `dbt_scd_id`, `dbt_updated_at`, `dbt_valid_from`, `dbt_valid_to`.

### Query Optimisation

- **Automatic clustering** on `(date_sk, merchant_sk)` in `fact_transactions`: Snowflake reorganises micro-partitions in the background, reducing table scans by 80–90% on temporal queries.
- **Dynamic Tables** for hourly pre-aggregations: revenue per merchant, fraud rate per region — automatic refresh, configurable lag.
- **90-day Time Travel**: pipeline error reprocessing, PCI-DSS auditing, point-in-time comparisons.

---

## 4. NoSQL System — MongoDB

**Reference file**: [`docs/04_nosql_model.md`](../04_nosql_model.md)
**Schema**: [`nosql/mongodb/mongodb_schema.py`](../../nosql/mongodb/mongodb_schema.py)
**Indices**: [`nosql/mongodb/mongodb_indexes.py`](../../nosql/mongodb/mongodb_indexes.py)
**Queries**: [`nosql/mongodb/mongodb_aggregation_queries.py`](../../nosql/mongodb/mongodb_aggregation_queries.py)
**Sample data**: [`demo/sample_data/fraud_events.json`](../../demo/sample_data/fraud_events.json)

### Collections

#### `fraud_events`

Reference document (see `demo/sample_data/fraud_events.json`):

```json
{
  "transaction_id": "550e8400-...",
  "merchant_id": "7c9e6679-...",
  "fraud_signals": {
    "score": 0.87,
    "velocity_flag": true,
    "geo_anomaly": true
  },
  "ml_features": {
    "avg_txn_amount_30d": 52.30,
    "txn_count_24h": 12,
    "country_mismatch": true
  },
  "model_version": "xgb_fraud_v2.3",
  "decision": "flagged"
}
```

**Modelling decision**: `ml_features` are embedded within the document (intentional denormalisation) to avoid joins at ML inference time — a single `findOne` is sufficient to retrieve all features required for scoring.

#### Index Strategy

```python
# Compound index for real-time detection queries
db.fraud_events.create_index([("merchant_id", 1), ("timestamp", -1)])

# Partial index on flagged events only
db.fraud_events.create_index(
    [("fraud_signals.score", -1)],
    partialFilterExpression={"decision": {"$in": ["flagged", "blocked"]}}
)

# TTL index on application logs (90-day retention)
db.app_logs.create_index("created_at", expireAfterSeconds=7_776_000)
```

---

## 5. Data Pipeline

**Reference file**: [`docs/05_pipeline_architecture.md`](../05_pipeline_architecture.md)
**Airflow DAG**: [`pipeline/airflow/stripe_daily_etl.py`](../../pipeline/airflow/stripe_daily_etl.py)
**CDC Debezium**: [`pipeline/debezium/`](../../pipeline/debezium/)
**Stream processing**: [`pipeline/flink/fraud_detection_job.py`](../../pipeline/flink/fraud_detection_job.py)
**dbt**: [`pipeline/dbt/stripe_dbt/`](../../pipeline/dbt/stripe_dbt/)

### CDC — Debezium + Kafka

```json
// debezium-postgres-connector.json (excerpt)
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "plugin.name": "pgoutput",
  "slot.name": "stripe_debezium_slot",
  "transforms": "maskPAN",
  "transforms.maskPAN.type": "org.apache.kafka.connect.transforms.MaskField$Value",
  "transforms.maskPAN.fields": "card_number"
}
```

PAN masking via SMT (Single Message Transform) ensures that no card number passes through Kafka in plain text — PCI-DSS compliance at the transport layer.

### Stream Processing — Faust

Primary agent `detect_fraud`: consumes `pg.transactions`, computes velocity (5-minute tumbling window) and geographical anomaly, produces `stripe.fraud_events`. Latency < 100 ms p99.

See full implementation: [`pipeline/flink/fraud_detection_job.py`](../../pipeline/flink/fraud_detection_job.py)

### Batch ETL — Airflow + dbt

DAG `stripe_daily_etl`: schedule `0 2 * * *`, SLA H+1, exponential retry (5/15/45 min), Slack alerts on failure.

Execution order:
1. `extract_postgres` → S3 dump
2. `dbt run --select staging` → normalisation
3. `dbt run --select marts` → star schema
4. `dbt test` → data tests
5. `dbt snapshot` → SCD Type 2
6. `load_mongodb_features` → feature store

---

## 6. Security & Compliance

**Reference file**: [`docs/06_security_compliance.md`](../06_security_compliance.md)
**Scripts**: [`sql/security/`](../../sql/security/)

### Security Matrix

| Layer     | Measure           | Implementation            | Standard      |
|-----------|-------------------|---------------------------|---------------|
| Transport | TLS 1.3           | Nginx, Kafka SSL          | PCI-DSS 4.2   |
| Storage   | AES-256           | AWS KMS, monthly rotation | PCI-DSS 3.5   |
| Access    | RBAC + MFA        | Okta SSO                  | SOC 2         |
| Audit     | Immutable logs    | CloudTrail + pg_audit     | PCI-DSS 10    |
| PAN       | Tokenisation      | Stripe Vault              | PCI-DSS 3.4   |
| GDPR      | Erasure           | `gdpr_erasure.sql`        | GDPR Art. 17  |
| CCPA      | Opt-out           | `ccpa_compliance.sql`     | CCPA §1798    |

### GDPR — Right to Erasure

```sql
-- gdpr_erasure.sql: irreversible anonymisation
UPDATE customers SET
    email = 'deleted_' || id || '@gdpr.invalid',
    full_name = 'DELETED',
    phone = NULL
WHERE id = :customer_id;
```

---

## 7. ML Strategy

**Reference file**: [`docs/07_ml_strategy.md`](../07_ml_strategy.md)
**Feature engineering**: [`ml/feature_engineering.py`](../../ml/feature_engineering.py)
**Monitoring**: [`ml/drift_monitoring.py`](../../ml/drift_monitoring.py) · [`ml/model_monitoring.py`](../../ml/model_monitoring.py)
**Evidently report**: [`demo/screenshots/evidently_report.html`](../../demo/screenshots/evidently_report.html)

### Full MLOps Pipeline

```
MongoDB (fraud_events)
    │
    ▼
PySpark feature engineering (7 features)
    │
    ▼
Feast Feature Store (online + offline)
    │
    ├──► MLflow Training (XGBoost fraud, LightGBM churn)
    │         │
    │         ▼
    │    MLflow Model Registry
    │         │
    │         ▼
    └──► FastAPI Serving (< 50 ms p99)
              │
              ▼
         Evidently AI (drift, performance, alerts)
```

### Engineered Features (PySpark)

| Feature                    | Description              | Window        |
|----------------------------|--------------------------|---------------|
| `txn_count_24h`            | Transaction velocity     | 24h           |
| `avg_txn_amount_30d`       | Baseline average amount  | 30d           |
| `amount_ratio_30d`         | Amount anomaly ratio     | 30d           |
| `distinct_countries_30d`   | Geographical spread      | 30d           |
| `geo_mismatch`             | IP ≠ card country        | —             |
| `device_fingerprint_match` | Known device             | —             |
| `merchant_fraud_rate_7d`   | Merchant risk score      | 7d (rolling)  |

### Models in Production

| Model            | Algorithm  | AUC  | Deployment       |
|------------------|------------|--- --|------------------|
| Fraud detection  | XGBoost    | 0.93 | FastAPI + MLflow |
| Churn prediction | LightGBM   | 0.87 | FastAPI + MLflow |

---

## 8. SQL & NoSQL Queries

**Reference file**: [`docs/08_queries.md`](../08_queries.md)

### OLTP Queries

See [`sql/oltp/queries.sql`](../../sql/oltp/queries.sql) — covers:
- Transactional insertion with `SELECT FOR UPDATE SKIP LOCKED`
- Duplicate detection via temporal window
- Atomic fraud score update

### OLAP Queries

See [`sql/olap/queries_analytics.sql`](../../sql/olap/queries_analytics.sql) — covers:
- Revenue analysis by region and period
- Customer RFM segmentation (Recency/Frequency/Monetary) via `NTILE(5)`
- Fraud rate by merchant and payment method
- Amount anomaly detection (z-score on sliding window)

### NoSQL Queries (MongoDB)

See [`nosql/mongodb/mongodb_aggregation_queries.py`](../../nosql/mongodb/mongodb_aggregation_queries.py) — covers:
- Fraud signal aggregation by merchant
- Top customers by transaction volume
- Abnormal velocity pattern detection
- ML model performance statistics

---

## 9. Observability

**Reference file**: [`docs/09_observability.md`](../09_observability.md)

### Monitoring Stack

| Component      | Tool                 | Key Metrics                         |
|----------------|----------------------|-------------------------------------|
| Infrastructure | Prometheus + Grafana | CPU, memory, network latency        |
| Kafka pipeline | Kafka JMX exporter   | Consumer lag, throughput            |
| Airflow        | Airflow metrics      | DAG duration, task failure rate     |
| ML             | Evidently AI         | Data drift, model performance       |
| Database       | pg_stat_statements   | Query duration p99, cache hit ratio |

### Critical Alerts

- Kafka lag > 10,000 messages → PagerDuty
- Airflow DAG failed after 3 retries → Slack #data-alerts
- Fraud score drift > 0.05 (PSI) → email data science team
- Serving API latency p99 > 100 ms → PagerDuty

---

## 10. Operational Runbooks

**Reference file**: [`docs/10_runbooks.md`](../10_runbooks.md)

Documented procedures:
- PostgreSQL primary failover
- Kafka replay from specific offset
- Partial dbt reprocessing (`dbt run --select +model_name`)
- ML model rollback (MLflow model registry → `Production` → `Archived`)
- GDPR customer erasure procedure (72h SLA)

---

## 11. Architecture Decision Records (ADR)

**Reference file**: [`docs/11_architecture_decisions.md`](../11_architecture_decisions.md)

| ADR      | Decision                          | Rejected Alternatives                |
|----------|-----------------------------------|--------------------------------------|
| ADR-001  | PostgreSQL + Citus vs CockroachDB | CockroachDB (+20 ms latency)         |
| ADR-002  | Snowflake vs BigQuery             | BigQuery (limited Time Travel)       |
| ADR-003  | MongoDB vs Cassandra              | Cassandra (no native $lookup)        |
| ADR-004  | Faust vs Apache Flink             | Flink (JVM complexity, ops overhead) |
| ADR-005  | dbt vs custom ETL                 | Custom ETL (no lineage, no tests)    |

See [`docs/technological-alternatives-evaluated.md`](../technological-alternatives-evaluated.md) for the full analysis.

---

## 12. Business Impact

**Reference file**: [`docs/12_business_impact.md`](../12_business_impact.md)

| Initiative                            | Metric                        | Value                       |
|---------------------------------------|-------------------------------|-----------------------------|
| Fraud detection (XGBoost AUC 0.93)    | Fraud reduction −40%          | +$15M/yr                    |
| Churn prediction (LightGBM AUC 0.87)  | Retention +12%                | +$25M/yr                    |
| 99.99% SLA                            | Contractual availability met  | Reputational risk avoided   |
| Automated GDPR/PCI-DSS compliance     | Fines avoided                 | Up to 4% of annual turnover |
| **Estimated total ROI**               |                               | **~$40M/yr**                |

---

## Quick Start

```bash
# Clone and install
git clone https://github.com/Artificial-Intelligence-Architect/Stripe-Business-Case
cd Stripe-Business-Case
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Run the tests
make test

# Generate PNG diagrams
make diagrams

# Start the local environment (Docker)
cd demo/local_setup
docker-compose up -d
```

See [`demo/local_setup/README.md`](../../demo/local_setup/README.md) for the full setup guide.
