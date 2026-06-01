# Stripe Business Case — Comprehensive Data Architecture
### AIA Certification Project | Data Engineering
> **Lead Data Engineer Senior** – Architecture Proposal

---

## Table of Contents

1. [Repository Structure](#1-repository-structure)
2. [Executive Summary](#2-executive-summary)
3. [Architecture Overview](#3-architecture-overview)
4. [OLTP Model — PostgreSQL](#4-oltp-model--postgresql)
5. [OLAP Model — Star Schema](#5-olap-model--star-schema)
6. [NoSQL Model — MongoDB](#6-nosql-model--mongodb)
7. [Data Pipeline](#7-data-pipeline)
8. [Security & Compliance](#8-security--compliance)
9. [Machine Learning Integration](#9-machine-learning-integration)
10. [SQL & NoSQL Queries](#10-sql--nosql-queries)
11. [Technology Choices & Justifications](#11-technology-choices--justifications)
12. [Proof of Functionality](#12-proof-of-functionality)
13. [Glossary](#13-glossary)

---

## 1. Repository Structure

Stripe-Business-Case/
├── demo/
│ ├── local_setup/
│ │ ├── docker-compose.yml
│ │ └── sample_data/
│ │ └── transactions.csv
│ ├── sample_data/
│ │ ├── fraud_events.json
│ │ └── transactions.csv
│ └── screenshots/
│ ├── airflow_dag_grid_run.png
│ ├── airflow_graph_run.png
│ ├── evidently_report.html
│ └── sql_query_result.png
├── docs/
│ ├── architecture_diagram.png
│ ├── architecture_diagram.svg
│ ├── data_ingestion.png
│ └── data_ingestion.svg
├── ml/
│ ├── feature_engineering.py
│ ├── generate_evidently_report.py
│ ├── model_monitoring.py
│ └── requirements.txt
├── nosql/
│ └── mongodb/
│ ├── aggregation_queries.js
│ ├── index.js
│ └── sample_documents.json
├── pipeline/
│ ├── airflow/
│ │ └── stripe_daily_etl.py
│ ├── dbt/
│ │ └── stripe_dbt/
│ │ ├── dbt_project.yml
│ │ ├── macros/
│ │ │ └── currency_conversion.sql
│ │ ├── models/
│ │ │ ├── intermediate/
│ │ │ │ └── int_transactions_enriched.sql
│ │ │ ├── marts/
│ │ │ │ ├── dim_customer.sql
│ │ │ │ ├── dim_merchant.sql
│ │ │ │ └── fct_transactions.sql
│ │ │ ├── sources.yml
│ │ │ └── staging/
│ │ │ └── stg_transactions.sql
│ │ └── README.md
│ └── flink/
│ └── FraudDetectionJob.java
├── sql/
│ ├── olap/
│ │ ├── queries_analytics.sql
│ │ └── schema.sql
│ ├── oltp/
│ │ └── schema.sql
│ ├── oltp_queries.sql
│ └── security/
│ ├── ccpa_compliance.sql
│ ├── gdpr_erasure.sql
│ └── rbac_setup.sql
├── Enonce-stripe.md
├── LICENSE
└── README.md
text


---

## 2. Executive Summary

Stripe, a FinTech leader, must unify its transactional (OLTP), analytical (OLAP), and unstructured (NoSQL) systems to meet consistency, scalability, and compliance requirements. The proposed architecture is built upon **PostgreSQL (OLTP)**, **Snowflake (OLAP)**, and **MongoDB Atlas (NoSQL)**, orchestrated by an event-driven pipeline (Kafka, Flink, Airflow, dbt). Security is ensured through encryption, RBAC, and automated compliance procedures (GDPR, PCI-DSS, CCPA). A complete Machine Learning lifecycle (feature store, training, serving, monitoring) enables real-time fraud detection.

---

## 3. Architecture Overview

### Global Diagram

```mermaid
graph TD
    subgraph Sources
        A1[Stripe API]
        A2[Mobile/Web SDK]
        A3[Webhooks]
    end

    subgraph Ingestion
        B[Apache Kafka]
    end

    subgraph Storage
        C1[(PostgreSQL + Citus)]
        C2[(MongoDB Atlas)]
        C3[(Snowflake)]
    end

    subgraph Processing
        D1[Apache Flink]
        D2[Airflow + dbt]
        D3[FastAPI]
    end

    subgraph Machine Learning
        E1[Feast Feature Store]
        E2[MLflow Model Registry]
    end

    subgraph Security
        F1[Okta / RBAC]
        F2[AWS KMS]
    end

    A1 & A2 & A3 --> B
    B --> C1
    B --> C2
    C1 -->|Debezium CDC| B
    B --> D1
    D1 --> C2
    D1 --> C3
    B --> D2
    D2 --> C3
    D2 --> C2
    C2 --> E1
    E1 --> E2
    E2 --> D3
    D3 -->|Real-time Score| C1
    D3 -->|Events| B
    C1 -.- F1
    C3 -.- F1
    C2 -.- F1
    C1 -.- F2
    C3 -.- F2
    C2 -.- F2

An exported PNG version of this diagram is available in docs/architecture_diagram.png.
Architecture Paradigm: Hybrid Lambda
Layer	Role	Target Latency
Speed layer	Kafka + Flink (real-time streaming)	< 100ms
Batch layer	Airflow + dbt (H+1 / D+1 reprocessing)	Minutes to hours
Serving layer	Snowflake (OLAP) + MongoDB (NoSQL) + PostgreSQL (OLTP)	< 1s
4. OLTP Model — PostgreSQL
Technology Choice: PostgreSQL + Citus Extension

Justification: Native ACID, declarative partitioning, logical replication for CDC (Debezium), Citus extension for horizontal sharding without altering application code.
SQL Schema (Extract)
sql

CREATE TABLE transactions (
    transaction_id  UUID    NOT NULL DEFAULT gen_random_uuid(),
    merchant_id     UUID    NOT NULL REFERENCES merchants(merchant_id),
    customer_id     UUID    NOT NULL REFERENCES customers(customer_id),
    amount          NUMERIC(18,4) NOT NULL CHECK (amount > 0),
    currency        CHAR(3)      REFERENCES currencies(code),
    amount_usd      NUMERIC(18,4),
    payment_method  VARCHAR(50)  NOT NULL,
    status          VARCHAR(20)  NOT NULL CHECK (status IN ('pending','success','failed','refunded','chargeback')),
    device_type     VARCHAR(20),
    ip_country      CHAR(2),
    fraud_score     NUMERIC(5,4) CHECK (fraud_score BETWEEN 0 AND 1),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (transaction_id, created_at)
) PARTITION BY RANGE (created_at);

The full DDL, indexes, and audit triggers are available in sql/oltp/schema.sql.
5. OLAP Model — Star Schema
Technology Choice: Snowflake

Separation of compute/storage, TIME TRAVEL, automatic clustering, native dbt connectors.
Materialised View (Example)
sql

CREATE OR REPLACE VIEW mv_daily_revenue AS
SELECT
    d.full_date,
    m.name AS merchant_name,
    m.region,
    SUM(f.amount_usd) AS total_revenue_usd,
    COUNT(*) FILTER (WHERE f.is_fraud) AS fraud_count
FROM fact_transactions f
JOIN dim_date d ON f.date_sk = d.date_sk
JOIN dim_merchant m ON f.merchant_sk = m.merchant_sk
WHERE m.is_current = true
GROUP BY 1,2,3;

The complete schema and DDLs are in sql/olap/schema.sql.
6. NoSQL Model — MongoDB
Technology Choice: MongoDB Atlas

Aggregation pipeline, automatic sharding, Atlas Search, change streams for CDC.
Main Collections

    fraud_events: fraud signals, ML features, decision

    user_sessions: clickstream, conversion funnel

    app_logs: application logs with TTL

The JSON schemas and indexes are in nosql/mongodb/.
7. Data Pipeline
text

PostgreSQL WAL → Debezium → Kafka
SDK/API → Kafka
Kafka → Flink (real-time fraud, enrichment)
Kafka Connect → MongoDB (fraud_events, sessions)
Kafka → S3 → Snowpipe → Snowflake (staging)
Airflow orchestrates dbt (transformations) → materialised views

The Airflow DAG is in pipeline/airflow/stripe_daily_etl.py.
8. Security & Compliance
Layer	Measure
Encryption	TLS 1.3, AES-256 at rest
Access	RBAC + MFA (Okta)
Audit	CloudTrail + audit_log table
PCI-DSS	PAN tokenisation (Stripe Vault), no CVV
GDPR	Automated anonymisation and right to erasure
CCPA	Access, deletion, and opt-out procedures

The RBAC, GDPR, and CCPA scripts are in sql/security/.
9. Machine Learning Integration

    Feature Store (Feast): fed from MongoDB and Kafka

    Training: MLflow, XGBoost, historical data

    Serving: FastAPI (inference < 50ms)

    Monitoring: Evidently AI detects data drift and triggers retraining

The code is in ml/feature_engineering.py and ml/model_monitoring.py.
10. SQL & NoSQL Queries
OLAP (Snowflake) – RFM Customer Segmentation
sql

WITH rfm_raw AS (
    SELECT customer_sk,
        DATEDIFF('day', MAX(d.full_date), CURRENT_DATE) AS recency,
        COUNT(*) AS frequency,
        SUM(amount_usd) AS monetary
    FROM fact_transactions f JOIN dim_date d ON f.date_sk = d.date_sk
    WHERE f.status = 'success'
    GROUP BY customer_sk
)
SELECT customer_sk,
    NTILE(5) OVER (ORDER BY recency) AS r_score,
    NTILE(5) OVER (ORDER BY frequency DESC) AS f_score,
    NTILE(5) OVER (ORDER BY monetary DESC) AS m_score
FROM rfm_raw;

NoSQL (MongoDB) – Top At-Risk Merchants
javascript

db.fraud_events.aggregate([
  { $match: { timestamp: { $gte: new Date(Date.now() - 3600*1000) }, "fraud_signals.score": { $gte: 0.7 } } },
  { $group: { _id: "$merchant_id", avg_score: { $avg: "$fraud_signals.score" }, count: { $sum: 1 } } },
  { $sort: { avg_score: -1 } },
  { $limit: 10 }
])

All queries are in sql/olap/queries_analytics.sql and nosql/mongodb/aggregation_queries.js.
11. Technology Choices & Justifications
Component	Choice	Alternative Discarded	Reason
OLTP	PostgreSQL + Citus	CockroachDB	ACID maturity, non-intrusive Citus
OLAP	Snowflake	Redshift	Compute/storage separation, Time Travel
NoSQL	MongoDB Atlas	Cassandra	Aggregation pipeline, flexible sharding
CDC	Debezium	AWS DMS	Open source, low latency
Streaming	Kafka + Flink	Kinesis	Flink < 100ms, stateful
Orchestration	Airflow	Prefect	Mature ecosystem
Feature Store	Feast	Tecton	Open source, multi-cloud
12. Proof of Functionality
Screenshot	Description
Airflow Grid	DAG executed successfully (4/4 tasks green)
Airflow Graph	Pipeline graph
SQL Query	Fraud detection query on PostgreSQL
Evidently Report	ML model monitoring report
13. Glossary
Term	Definition
OLTP	Online Transaction Processing
OLAP	Online Analytical Processing
CDC	Change Data Capture
SCD Type 2	Slowly Changing Dimension (historisation)
WAL	Write-Ahead Log
RFM	Recency, Frequency, Monetary
TDE	Transparent Data Encryption
PAN	Primary Account Number
TTL	Time To Live

Final document – ready for the AIA Data Engineering certification.
Architecture designed to support 10x Stripe's current volume without major refactoring.
