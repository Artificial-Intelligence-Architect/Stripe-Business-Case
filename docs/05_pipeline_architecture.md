# 05 — Data Pipeline Architecture

> This document was previously a set of fragments with no title and several
> claims that contradicted the code (Snowflake "partitioned by transaction_date",
> "dynamic tables refreshed every 5 minutes"). Both are corrected here: Snowflake
> has no user-defined partitions, and the freshness contract is `TARGET_LAG`, not
> a 5-minute refresh. See `sql/olap/schema.sql`.

## Overview

Data flows in one direction, sourced from a single system of record:

```
Payment API ─► PostgreSQL (WAL) ─► Debezium ─► Kafka ─┬─► Faust/Flink ─► MongoDB (real-time)
                                                       └─► S3 ─► Snowpipe ─► dbt ─► Snowflake (batch)
```

The application writes **only** to PostgreSQL. Every downstream system is fed by
Change Data Capture off the write-ahead log — never by a second application write.
The justification (dual-write corruption) is in `01_architecture.md`.

## Streaming path — real-time fraud scoring

| Stage | Technology | Guarantee | Target latency |
|---|---|---|---|
| Capture | Debezium (wal2json) | At-least-once, ordered per table | < 1 s |
| Transport | Kafka, RF=3, `min.insync.replicas=2` | Durable, partitioned by `merchant_id` | < 100 ms |
| Process | Faust (fallback Flink for windowed aggregates) | Idempotent on `transaction_id` | p99 < 50 ms |
| Serve | MongoDB `fraud_events` | Read-your-writes (causal) | < 100 ms |

Kafka is keyed by `merchant_id` — the same distribution key as Citus — so all
events for one merchant land on one partition and are processed in order. Without
that, velocity features ("5th transaction from this merchant in 10 seconds") would
race.

## Batch path — analytics

| Stage | Technology | Cadence | Notes |
|---|---|---|---|
| Land | S3 sink connector | Continuous | Kafka → S3 parquet |
| Ingest | Snowpipe | ~1-2 min after landing | auto-ingest on S3 event |
| Transform | dbt (staging → intermediate → marts) | Daily, orchestrated by Airflow | SCD2 via dbt snapshots |
| Serve | Snowflake Dynamic Tables | `TARGET_LAG` (see below) | declarative incremental refresh |

## Freshness contract (single source of truth)

These three values are declared in `sql/olap/schema.sql` and **asserted** by the
Airflow task `assert_dynamic_table_freshness`. They appear in exactly one place in
code; this table only documents them:

| Object | TARGET_LAG | Audience |
|---|---|---|
| `mv_daily_revenue` | 1 hour | operational merchant dashboards |
| `mv_customer_monthly` | 1 day | finance / segmentation |
| `mv_subscription_mrr` | 1 hour | revenue-critical MRR tracking |

There is **no** 5-minute refresh anywhere. The earlier "every 5 minutes" claim was
inconsistent with both the README (H+1) and the DDL (1 hour). The pipeline does not
force refreshes; Snowflake maintains the lag and Airflow verifies it was met.

## Snowflake physical design (corrected)

Snowflake has no user-defined partitions. It uses immutable **micro-partitions**
plus an optional **clustering key**:

- `fact_transactions` is clustered by `(date_sk, merchant_sk)` — the real filter
  pattern is a date range then a merchant, so pruning works on the leading column.
- The earlier "partitioned by transaction_date (daily)" statement described a
  concept that does not exist in Snowflake and a column that is not on the table.

## CDC failure management

- Schema changes validated through the Schema Registry (Avro, backward-compatible).
- Invalid events routed to a per-topic Dead Letter Queue, not dropped.
- Consumers idempotent on `transaction_id` as the business key.
- Replay supported from Kafka's 7-day retention.
- Debezium offsets stored in a dedicated compacted topic → resumes from the last
  committed LSN after a crash. **Caveat:** while the connector is down the
  PostgreSQL WAL grows and cannot be recycled — alert at 80% disk. Runbook:
  `10_runbooks.md#ingestion-gap`.

## Failure handling and recovery

| Component | Config | On failure |
|---|---|---|
| Kafka | RF=3, 7-day retention | Broker loss transparent (in-sync replicas) |
| Debezium | Offsets in compacted topic | Resume from last LSN; monitor WAL growth |
| Airflow | `retries=3`, exp. back-off 5/15/45 min | Idempotent tasks re-run safely |
| Snowpipe | auto-ingest | S3 event replay from notification queue |

## Pipeline monitoring

Critical metrics: Kafka consumer lag (alert > 1000 msgs), Debezium WAL lag, Faust
worker failure rate, Airflow run duration (alert > 1 h), dynamic-table lag vs
`TARGET_LAG`. Stack: Prometheus + Grafana for the Kafka/Faust tier, Datadog for
Snowflake and Airflow. Detail in `09_observability.md`.
