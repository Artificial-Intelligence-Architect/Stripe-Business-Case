# Data Engineering Certification Project: Stripe Data Architecture

This repository documents the data architecture design developed to address the scalability, financial reliability, and real-time analytics requirements of Stripe.

## 📋 Table of Contents
1. [Overview](#overview)
2. [Technical Architecture](#technical-architecture)
3. [Data Modelling](#data-modelling)
4. [Pipeline and Integration](#pipeline-and-integration)
5. [Security and Compliance](#security-and-compliance)
6. [Machine Learning Strategy](#machine-learning-strategy)

---

## 1. Overview
Stripe processes billions of transactions annually, requiring absolute transactional integrity (ACID) coupled with high-scale analytical capabilities. This architecture decouples transactional systems (OLTP) from analytical systems (OLAP) using an event-driven streaming approach.

## 2. Technical Architecture
The system employs a **Lambda/Event-Driven** approach:
- **OLTP:** PostgreSQL (3NF) serves as the source of truth.
- **Ingestion:** Change Data Capture (CDC) via **Debezium** (reading WAL logs).
- **Message Bus:** **Apache Kafka** for decoupling and temporary event persistence.
- **Analytics:** **Snowflake** (columnar storage) for high-performance reporting.
- **NoSQL:** **MongoDB** for flexible metadata (API payloads).
- **Orchestration:** **Apache Airflow** (idempotent jobs).

[Image of modern data platform architecture showing OLTP, Kafka, NoSQL, and OLAP integration]

## 3. Data Modelling
- **OLTP:** Normalised relational schema (3NF). B-Tree indexing for performance.
- **OLAP:** Star Schema (`fact_payments`, `dim_merchants`, `dim_geography`). Financial amounts stored as *cents* (Integer) to ensure precision.
- **NoSQL:** JSON documents with *sharding* by `customer_id` to ensure horizontal scalability.

## 4. Pipeline and Integration
The pipeline ensures data consistency across systems:
1. **Speed Layer:** Kafka + Flink (Real-time ML inference).
2. **Batch Layer:** Kafka -> S3 -> Airflow -> dbt -> Snowflake.
*Note: All jobs are **idempotent** to prevent transaction duplication.*

## 5. Security and Compliance
The architecture is designed to meet **PCI-DSS** and **GDPR** standards:
- **Tokenisation:** No Primary Account Numbers (PAN) are stored in plain text within the Data Warehouse.
- **Dynamic Masking:** SQL policies in Snowflake mask Personal Identifiable Information (PII) for non-authorised roles.
- **RBAC:** Strict Role-Based Access Control implemented across all layers.

## 6. Machine Learning Strategy
Integration of fraud detection models via a microservice consuming real-time events from Kafka.
- **Feature Store:** MongoDB provides aggregated features (e.g., `nb_transactions_last_min`) pre-calculated by Flink.
- **Monitoring:** Model drift is tracked to trigger automatic re-training via Airflow workflows.

---
*Project completed for the AIA certification.*
