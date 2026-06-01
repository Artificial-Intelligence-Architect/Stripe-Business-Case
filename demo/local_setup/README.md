# Local Demonstration Environment

## Purpose

This local environment provides a lightweight demonstration of the Stripe Business Case architecture.

The objective is not to reproduce a production-grade Stripe deployment, but to demonstrate the main architectural concepts described in the project:

* OLTP data processing with PostgreSQL
* NoSQL document storage with MongoDB
* Event streaming with Kafka
* Workflow orchestration with Airflow
* Analytical modelling with dbt
* Fraud detection architecture patterns

---

## Included Components

| Component  | Purpose                               |
| ---------- | ------------------------------------- |
| PostgreSQL | OLTP transactional database           |
| MongoDB    | Semi-structured data and fraud events |
| Kafka      | Event streaming backbone              |
| Airflow    | Batch orchestration                   |
| dbt        | Analytical transformations            |

---

## Architecture Scope

This environment demonstrates architectural integration patterns rather than production-scale throughput.

Several components described in the architecture are intentionally represented as design artefacts:

* Snowflake (documented target-state OLAP platform)
* Apache Flink (reference implementation)
* Feast Feature Store
* MLflow Model Registry
* Evidently AI Monitoring

These services are included in the architecture design but are not required for local execution.

---

## Prerequisites

* Docker Engine 24+
* Docker Compose 2+
* Python 3.10+
* Java 17+ (optional)

---

## Start Infrastructure

```bash
docker compose up -d
```

Verify services:

```bash
docker ps
```

Expected services:

* PostgreSQL
* MongoDB
* Kafka
* Zookeeper
* Airflow

---

## Load Sample Data

### PostgreSQL

```bash
psql -U postgres -d stripe_demo \
-c "\copy transactions FROM './sample_data/transactions.csv' CSV HEADER"
```

### MongoDB

```bash
mongosh < init_mongo.js
```

---

## Demonstration Scenarios

### OLTP

* High-volume transaction processing
* Fraud score filtering
* Chargeback analysis

### OLAP

* Revenue aggregation
* Merchant performance
* Customer segmentation

### NoSQL

* Fraud event storage
* Session analytics
* Application log exploration

### Data Engineering

* CDC architecture review
* Batch pipeline review
* Streaming architecture review

---

## Disclaimer

All datasets included in this repository are synthetic and generated solely for demonstration purposes.

No real customer, merchant, payment, or personal data is included.

This project is intended for educational and certification purposes only.
