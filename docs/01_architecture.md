# 01 — Comprehensive Data Architecture

> **This file previously contained a stale copy of the Business Impact / ROI
> document.** A reader opening `docs/01_architecture.md` looking for the
> architecture found a table of ROI figures — and outdated ones, contradicting
> `docs/12`. The ROI content now lives only in `docs/12_business_impact.md`.

## Objective

Unify Stripe's transactional (OLTP), analytical (OLAP) and non-relational
(NoSQL) systems behind a single, consistent data flow that serves operational
payments, near-real-time analytics and machine learning — under GDPR, PCI-DSS
and CCPA constraints.

## The three pillars

| Layer     | Technology         | Why this one                                     | What it must never do        |
|-----------|--------------------|--------------------------------------------------|------------------------------|
| **OLTP**  | PostgreSQL + Citus | ACID, mature relational modelling, logical replication for CDC, horizontal sharding by `merchant_id` | Serve analytical scans |
| **OLAP**  | Snowflake          | Storage/compute separation, Time Travel, dbt-native, elastic warehouses | Be on the payment critical path |
| **NoSQL** | MongoDB Atlas      | Flexible schema, nested ML features, TTL retention, GridFS for binary/XML | Be the system of record for money |

The boundary matters more than the choice: **money lives in PostgreSQL**.
MongoDB stores what we *observe* and *infer* (signals, sessions, feedback,
evidence); Snowflake stores what we *aggregate*. Neither can contradict the ledger.

## End-to-end flow

```
                       ┌──────────────────────────────────────┐
                       │        Payment API (Stripe)          │
                       └───────────────┬──────────────────────┘
                                       │ write
                                       ▼
   ┌───────────────────────────────────────────────────────────────┐
   │  OLTP — PostgreSQL 15 + Citus                                  │
   │  distributed by merchant_id · partitioned by created_at        │
   │  merchants · customers · products · subscriptions              │
   │  subscription_events · transactions · audit_log                │
   └───────────────┬───────────────────────────────────────────────┘
                   │ logical replication (wal2json)
                   ▼
            ┌──────────────┐
            │  Debezium    │  CDC — no dual-write, the WAL is the truth
            └──────┬───────┘
                   ▼
   ┌───────────────────────────────────────────────────────────────┐
   │  Kafka (Confluent) · 3 brokers · RF=3 · 7-day retention        │
   │  topics: stripe.public.transactions / .subscriptions / ...     │
   │  Schema Registry (Avro) · DLQ per topic                        │
   └───────┬───────────────────────────────────┬───────────────────┘
           │ streaming                         │ batch (S3 sink)
           ▼                                   ▼
   ┌────────────────────┐            ┌──────────────────────────┐
   │ Faust / Flink      │            │ Snowpipe → staging.raw_* │
   │ fraud scoring      │            └────────────┬─────────────┘
   │ p99 < 50 ms        │                         │
   └─────┬──────────┬───┘                         ▼
         │          │                  ┌──────────────────────────┐
         │          │ decision         │ dbt: staging →           │
         ▼          ▼                  │ intermediate → marts     │
   ┌──────────┐  ┌──────────────┐      │ snapshots = SCD2         │
   │ MongoDB  │  │ back to OLTP │      └────────────┬─────────────┘
   │ Atlas    │  │ fraud_score  │                   ▼
   │          │  └──────────────┘      ┌──────────────────────────┐
   │ fraud_   │                        │ OLAP — Snowflake         │
   │  events  │───── batch load ──────▶│ fact_transactions        │
   │ sessions │                        │ fact_subscription_events │
   │ app_logs │                        │ dim_* (SCD2)             │
   │ feedback │                        │ Dynamic Tables (lag 1h)  │
   │ disputes │                        └──────────────────────────┘
   │ (GridFS) │
   └──────────┘
         ▲                    orchestration: Airflow (daily + SLA assertions)
         │                    governance:    Vault · Okta · audit_log
         └── ML features ─────────────────────────────────────────────
```

## Why CDC and not dual-write

The application writes **once**, to PostgreSQL. Every other system is fed from
the WAL by Debezium.

The alternative — having the API write to PostgreSQL *and* publish to Kafka —
is the single most common way to corrupt a payment platform: the two writes are
not atomic, so a crash between them leaves Kafka and PostgreSQL permanently
disagreeing about whether a payment happened. There is no reconciliation job
that fully fixes this, because you cannot distinguish "message lost" from
"message not yet sent".

CDC makes the WAL the only source of truth: if it is in PostgreSQL, it will
reach Kafka; if it is not, it never happened.

## Consistency model per hop

| Hop | Guarantee | Latency | Rationale |
|---|---|---|---|
| API → PostgreSQL | **Strong / ACID** | p99 < 50 ms | Money. Non-negotiable. |
| PostgreSQL → Kafka | At-least-once, ordered per `merchant_id` | < 1 s | Kafka key = distribution key ⇒ per-merchant ordering |
| Kafka → Faust → MongoDB | At-least-once, **idempotent** on `transaction_id` | < 100 ms | Duplicates are inevitable; idempotency makes them harmless |
| Kafka → Snowflake | At-least-once, deduped in dbt staging | ~5-15 min | Analytics tolerate lag; they do not tolerate double-counting |
| Snowflake marts | Eventual, bounded by `TARGET_LAG` | 1 h / 1 d | Declared and *asserted* by Airflow |

Exactly-once is not claimed anywhere. It is claimed by vendors, not achieved by
distributed systems. What is achieved is at-least-once delivery plus idempotent
consumers, which is observationally equivalent and actually implementable.

## Failure domains

| Failure | Blast radius | Response |
|---|---|---|
| Citus worker node down | 1 shard = 1 merchant subset | Streaming replica promoted, RTO < 30 s, RPO 0 (sync replication) |
| Kafka broker down | None | RF=3, min.insync.replicas=2 |
| Debezium connector down | CDC lag grows | Offsets in Kafka; resumes from last committed LSN. **PostgreSQL WAL grows** — alert at 80% disk |
| Faust worker down | Fraud scoring degrades | Fallback: rules-only scoring; transaction still processed (fail-open, logged) |
| Snowflake unavailable | Analytics only | Payments unaffected — OLAP is off the critical path by design |
| MongoDB unavailable | Fraud features degrade | Fail-open to rules; `fraud_score` null-flagged for backfill |

The deliberate asymmetry: **analytics failures never stop payments; payment
failures stop everything.** The architecture is arranged so that the blast
radius of every non-OLTP component is "we know less", never "we lose money".

## Detailed models

- OLTP → [`02_oltp_model.md`](02_oltp_model.md) · ERD: [`erd_oltp.mermaid`](erd_oltp.mermaid)
- OLAP → [`03_olap_model.md`](03_olap_model.md)
- NoSQL → [`04_nosql_model.md`](04_nosql_model.md)
- Pipeline → [`05_pipeline_architecture.md`](05_pipeline_architecture.md)
- Security → [`06_security_compliance.md`](06_security_compliance.md)
- ML → [`07_ml_strategy.md`](07_ml_strategy.md)
- Cross-system GDPR erasure → [`13_gdpr_cross_system_erasure.md`](13_gdpr_cross_system_erasure.md)
- Trade-offs & alternatives → [`11_architecture_decisions.md`](11_architecture_decisions.md), [`technological-alternatives-evaluated.md`](technological-alternatives-evaluated.md)
