# Stripe Business Case — Comprehensive Data Architecture
### AIA Certification Project | Data Engineering
> **Lead Data Engineer Senior** – Architecture Proposal

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Architecture Overview](#1-architecture-overview)
3. [OLTP Model — PostgreSQL](#2-oltp-model--postgresql)
4. [OLAP Model — Star Schema](#3-olap-model--star-schema)
5. [NoSQL Model — MongoDB](#4-nosql-model--mongodb)
6. [Data Pipeline](#5-data-pipeline)
7. [Security & Compliance](#6-security--compliance)
8. [Machine Learning Integration](#7-machine-learning-integration)
9. [SQL & NoSQL Queries](#8-sql--nosql-queries)
10. [Technology Choices & Justifications](#9-technology-choices--justifications)
11. [Proof of Functionality](#10-proof-of-functionality)
12. [Oral Defence Preparation](#11-oral-defence-preparation)
13. [Glossary](#glossary)

---

## Executive Summary

Stripe, a FinTech leader, must unify its transactional (OLTP), analytical (OLAP), and unstructured (NoSQL) systems to meet consistency, scalability, and compliance requirements. The proposed architecture is built upon **PostgreSQL (OLTP)**, **Snowflake (OLAP)**, and **MongoDB Atlas (NoSQL)**, orchestrated by an event-driven pipeline (Kafka, Flink, Airflow, dbt). Security is ensured through encryption, RBAC, and automated compliance procedures (GDPR, PCI-DSS). A complete Machine Learning lifecycle (feature store, training, serving, monitoring) enables real-time fraud detection.

---

## 1. Architecture Overview

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

Architecture Paradigm: Hybrid Lambda
Layer	Role	Target Latency
Speed layer	Kafka + Flink (real-time streaming)	< 100ms
Batch layer	Airflow + dbt (H+1 / D+1 reprocessing)	Minutes to hours
Serving layer	Snowflake (OLAP) + MongoDB (NoSQL) + PostgreSQL (OLTP)	< 1s
2. OLTP Model — PostgreSQL
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
3. OLAP Model — Star Schema
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
4. NoSQL Model — MongoDB
Technology Choice: MongoDB Atlas

Aggregation pipeline, automatic sharding, Atlas Search, change streams for CDC.
Main Collections

    fraud_events: fraud signals, ML features, decision

    user_sessions: clickstream, conversion funnel

    app_logs: application logs with TTL

The JSON schemas and indexes are in nosql/mongodb/.
5. Data Pipeline
text

PostgreSQL WAL → Debezium → Kafka
SDK/API → Kafka
Kafka → Flink (real-time fraud, enrichment)
Kafka Connect → MongoDB (fraud_events, sessions)
Kafka → S3 → Snowpipe → Snowflake (staging)
Airflow orchestrates dbt (transformations) → materialised views

The Airflow DAG is in pipeline/airflow/stripe_daily_etl.py.
6. Security & Compliance
Layer	Measure
Encryption	TLS 1.3, AES-256 at rest
Access	RBAC + MFA (Okta)
Audit	CloudTrail + audit_log table
PCI-DSS	PAN tokenisation (Stripe Vault), no CVV
GDPR	Automated anonymisation and right to erasure

The RBAC and GDPR scripts are in sql/security/.
7. Machine Learning Integration

    Feature Store (Feast): fed from MongoDB and Kafka

    Training: MLflow, XGBoost, historical data

    Serving: FastAPI (inference < 50ms)

    Monitoring: Evidently AI detects data drift and triggers retraining

The code is in ml/feature_engineering.py and ml/model_monitoring.py.
8. SQL & NoSQL Queries
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
9. Technology Choices & Justifications
Component	Choice	Alternative Discarded	Reason
OLTP	PostgreSQL + Citus	CockroachDB	ACID maturity, non-intrusive Citus
OLAP	Snowflake	Redshift	Compute/storage separation, Time Travel
NoSQL	MongoDB Atlas	Cassandra	Aggregation pipeline, flexible sharding
CDC	Debezium	AWS DMS	Open source, low latency
Streaming	Kafka + Flink	Kinesis	Flink < 100ms, stateful
Orchestration	Airflow	Prefect	Mature ecosystem
Feature Store	Feast	Tecton	Open source, multi-cloud
10. Proof of Functionality
Screenshot	Description
Airflow Grid	DAG executed successfully (4/4 tasks green)
Airflow Graph	Pipeline graph
SQL Query	Fraud detection query on PostgreSQL
Evidently Report	ML model monitoring report

Architecture designed to support 10x Stripe's current volume without major refactoring.
