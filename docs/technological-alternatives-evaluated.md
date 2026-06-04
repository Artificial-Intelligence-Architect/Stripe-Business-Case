# Technological Alternatives Evaluated

This document details the **technological alternatives** considered for each component of the architecture, along with the **decision criteria** that led to the final selection.

---

## 📊 OLAP: Snowflake vs. Amazon Redshift vs. Google BigQuery

### Comparison Table

| Criterion               | **Snowflake**                          | **Amazon Redshift**                     | **Google BigQuery**                     | **Our Choice** |
|-------------------------|----------------------------------------|-----------------------------------------|-----------------------------------------|-----------------|
| **Pricing Model**       | Compute/Storage separated (pay-as-you-go) | Dedicated nodes (fixed cost + manual scaling) | Query-based (pay-as-you-go)        | ✅ Snowflake    |
| **Scalability**          | **Automatic** (independent compute)     | Manual (node addition)                  | Automatic (serverless)                | ✅ Snowflake    |
| **Compute/Storage Separation** | ✅ Yes (key for Stripe) | ❌ No (tight coupling)                  | ✅ Yes (serverless)                      | ✅ Snowflake    |
| **Time Travel**          | ✅ 90 days (included)                   | ❌ No (manual snapshots)                 | ✅ 7 days (extendable)                   | ✅ Snowflake    |
| **dbt Integration**     | ✅ Native (official connector)          | ✅ Good (less mature)                    | ✅ Native                                | ✅ Snowflake    |
| **VACUUM Management**   | ❌ Not required                         | ❌ **Manual** (costly in ops)             | ❌ Not required                        | ✅ Snowflake    |
| **Ad-Hoc Query Performance** | ✅ Excellent (auto-clustering) | ⚠️ Average (optimised for repetitive queries) | ✅ Excellent (serverless) | ✅ Snowflake |
| **Cost for Stripe**      | ⚠️ High (but predictable)               | ✅ Low (if stable workload)             | ⚠️ Variable (risk of unexpected costs)   | ⚠️ Trade-off   |
| **Ecosystem**           | ✅ Mature (FinTech, SaaS)               | ✅ AWS (if already on AWS)               | ✅ GCP (if already on GCP)               | ✅ Snowflake    |
| **Verdict**              | **Best choice for Stripe**             | Discarded (compute/storage coupling)   | Discarded (variable costs, less control) | **Snowflake**   |

---

### Justification for Snowflake
1. **Compute/Storage Separation**: Critical for Stripe, which has **unpredictable workloads** (transaction spikes, ad-hoc analytics).
2. **Time Travel**: Allows **data replay** in case of errors (e.g., corruption, ETL bugs) without manual backups.
3. **dbt Integration**: Stripe already uses dbt for transformations. Snowflake offers **native integration** with atomic operations (`MERGE`).
4. **Automatic Clustering**: Optimises queries on `date_sk` and `merchant_sk` without manual intervention.
5. **No VACUUM Required**: Reduces operational overhead (Redshift requires regular VACUUM operations to prevent performance degradation).

---

## 🗃️ NoSQL: MongoDB Atlas vs. Apache Cassandra vs. Amazon DynamoDB

### Comparison Table

| Criterion               | **MongoDB Atlas**                     | **Apache Cassandra**                   | **Amazon DynamoDB**                     | **Our Choice** |
|-------------------------|---------------------------------------|----------------------------------------|-----------------------------------------|-----------------|
| **Data Model**          | JSON documents (flexible schema)      | Wide-column (rigid schema)             | Key-value + documents (flexible schema) | ✅ MongoDB      |
| **Complex Queries**     | ✅ Rich aggregations (`$lookup`, `$group`) | ❌ Limited (no joins, basic aggregations) | ⚠️ Limited (PartiQL, no `$lookup`) | ✅ MongoDB      |
| **Indexing**             | ✅ Secondary, compound, TTL, text      | ✅ Primary/secondary (but complex)      | ✅ Primary/secondary (LSI/GSI)          | ✅ MongoDB      |
| **Scalability**          | ✅ Horizontal (auto-sharding)          | ✅ Horizontal (best for writes)        | ✅ Horizontal (auto-partitioning)      | ✅ MongoDB      |
| **ML Integration**       | ✅ Atlas Search (Lucene), aggregations | ❌ Complex (no native support)          | ⚠️ Possible (but limited)                | ✅ MongoDB      |
| **Change Streams**       | ✅ Native (easy Kafka integration)     | ❌ Not native (requires connectors)     | ✅ Streams (less mature)                 | ✅ MongoDB      |
| **Cost**                 | ⚠️ High (but managed by Atlas)         | ✅ Low (open-source)                    | ⚠️ Variable (pay-as-you-go)               | ⚠️ Trade-off   |
| **Ecosystem**            | ✅ Mature (FinTech, logs, features)    | ✅ Used by Uber, Netflix                 | ✅ AWS (if already on AWS)               | ✅ MongoDB      |
| **Verdict**              | **Best choice for Stripe**             | Discarded (limits for ML/aggregations)  | Discarded (less flexible for our use cases) | **MongoDB**    |

---

### Justification for MongoDB Atlas
1. **Rich Aggregations**: Stripe requires **complex queries** for fraud detection (e.g., `$lookup` between `user_sessions` and `fraud_events`). Cassandra and DynamoDB are **limited** in this regard.
2. **ML Integration**:
   - **Atlas Search** (Lucene-based) enables full-text search and advanced filtering **without external Elasticsearch**.
   - **Aggregations** (`$group`, `$match`) are essential for calculating features like `merchant_fraud_rate_7d`.
3. **Change Streams**: Enables **real-time synchronisation** with Kafka to feed the fraud pipeline.
4. **Flexible Schema**: Ideal for storing **semi-structured data** (e.g., `fraud_signals`, `ml_features` in `fraud_events`).
5. **Automatic Sharding**: MongoDB Atlas handles sharding by `merchant_id` **without manual configuration**, simplifying operations.

---

## 🔄 Other Components: Quick Justifications

### OLTP: PostgreSQL + Citus vs. CockroachDB
| Criterion          | PostgreSQL + Citus | CockroachDB       | Choice  |
|--------------------|--------------------|-------------------|---------|
| **ACID Compliance** | ✅ Full ACID        | ✅ Full ACID       | ✅      |
| **Sharding**        | ✅ By `merchant_id` | ✅ Automatic       | ✅      |
| **Latency**         | ✅ < 10 ms          | ⚠️ ~20-50 ms (network overhead) | ✅ PostgreSQL |
| **Maturity**        | ✅ 20+ years in production | ⚠️ Less mature | ✅ PostgreSQL |
| **CDC Integration** | ✅ Native Debezium | ✅ Native           | ✅      |
| **Verdict**         | **PostgreSQL + Citus** (better latency and maturity) |

---

### Streaming: Kafka vs. AWS Kinesis vs. Google Pub/Sub
| Criterion          | Kafka               | Kinesis          | Pub/Sub          | Choice  |
|--------------------|---------------------|------------------|------------------|---------|
| **Throughput**     | ✅ 1M+ msg/sec       | ✅ 1M+ msg/sec    | ✅ 1M+ msg/sec    | ✅ Kafka |
| **Latency**        | ✅ < 10 ms           | ✅ < 100 ms       | ✅ < 100 ms       | ✅ Kafka |
| **Durability**     | ✅ 7 days (configurable) | ✅ 7 days   | ✅ 7 days        | ✅      |
| **Ecosystem**      | ✅ Rich connectors (Debezium, Faust) | ⚠️ Limited | ⚠️ Limited | ✅ Kafka |
| **Cost**           | ⚠️ Cluster management | ✅ Pay-as-you-go  | ✅ Pay-as-you-go  | ⚠️ Trade-off |
| **Verdict**        | **Kafka** (best ecosystem for our stack) |

---

### Orchestration: Airflow vs. Dagster vs. Prefect
| Criterion          | Airflow            | Dagster          | Prefect          | Choice  |
|--------------------|--------------------|------------------|------------------|---------|
| **Maturity**       | ✅ Very mature      | ✅ Mature        | ✅ Mature         | ✅ Airflow |
| **dbt Integration** | ✅ Native          | ✅ Native         | ✅ Native         | ✅      |
| **UI**             | ✅ Comprehensive    | ✅ Modern         | ✅ Modern         | ✅      |
| **Scalability**    | ⚠️ Limited (Celery) | ✅ Better         | ✅ Better         | ⚠️ Trade-off |
| **Verdict**        | **Airflow** (already used at Stripe, perfect dbt integration) |

---

## 📌 Conclusion
Technological choices were driven by:
1. **Stripe’s Business Needs**:
   - **High availability** (99.99% for OLTP).
   - **Horizontal scalability** (to handle billions of transactions).
   - **Flexibility** (evolving schemas for NoSQL, ad-hoc queries for OLAP).
2. **Integration with Existing Systems**:
   - Stripe already uses **PostgreSQL**, **Kafka**, and **dbt**.
3. **Maturity and Ecosystem**:
   - Preference for **production-proven tools** (e.g., PostgreSQL > CockroachDB).
4. **Total Cost of Ownership (TCO)**:
   - Balance between **cost**, **performance**, and **maintainability**.

---

## 🔗 Useful Links
- [Snowflake vs. Redshift (Gartner)](https://www.gartner.com/reviews/market/data-warehouse-solutions)
- [MongoDB vs. Cassandra (MongoDB)](https://www.mongodb.com/compare/mongodb-cassandra)
- [Kafka vs. Kinesis (Confluent)](https://www.confluent.io/blog/kafka-vs-kinesis/)