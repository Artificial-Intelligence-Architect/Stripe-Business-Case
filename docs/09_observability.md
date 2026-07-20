# 09 — Observability

Three questions this layer must answer at 03:00: is money still moving, is fraud
scoring still working, and are we still compliant.

## Golden signals per component

| Component | Metric | Alert threshold | Why it matters |
|---|---|---|---|
| **OLTP** | Transaction p99 latency | > 50 ms | The payment SLA |
| **OLTP** | Replication lag (Citus) | > 5 s | Failover would lose data |
| **OLTP** | WAL disk usage | > 80% | Full WAL takes OLTP down (see `#ingestion-gap`) |
| **Debezium** | CDC lag (LSN behind) | > 30 s | Everything downstream goes stale |
| **Kafka** | Consumer lag (fraud group) | > 1000 msgs | Fraud scoring falling behind |
| **Kafka** | Under-replicated partitions | > 0 | Durability at risk |
| **Faust** | Fraud scoring p99 | > 50 ms | Degrading toward rules-only fallback |
| **Faust** | Worker failure rate | > 0 sustained | Capacity loss |
| **Snowflake** | Dynamic table lag vs TARGET_LAG | > 1.5× | Dashboards stale (see `#dynamic-table-lag`) |
| **Snowflake** | Warehouse credit burn | > budget/day | Cost governance |
| **dbt** | Test failures | any | Data quality (e.g. `refund_has_parent`) |
| **ML** | Inference latency | > 50 ms | Scoring path SLA |
| **ML** | Feature/prediction drift | PSI > 0.2 | Model degrading (Evidently) |
| **Compliance** | GDPR SLA breaches | > 0 | Legal exposure |
| **Compliance** | Stuck erasure propagation | > 0 for 24h | False compliance (see `#gdpr-stuck-propagation`) |

## The three "is it working?" dashboards

1. **Money** — transaction rate, success/failure ratio, p99 latency, revenue vs
   same-hour-last-week. A revenue drop with a flat transaction rate = FX or
   aggregation bug, not a traffic drop.
2. **Fraud** — scoring throughput, score distribution (a sudden shift = model or
   feature-pipeline problem), fallback-mode activations, block rate per merchant
   with the z-score anomaly flag from `queries_analytics.sql` #7.
3. **Compliance** — open erasure requests vs SLA clock, propagation flags,
   audit-log write rate (a drop means the audit trigger may be broken).

## Stack

| Concern | Tool |
|---|---|
| Metrics | Prometheus |
| Dashboards | Grafana |
| Traces | OpenTelemetry (payment → CDC → scoring → decision) |
| Logs | Loki / ELK |
| ML monitoring | Evidently AI |
| Alerting | PagerDuty (SEV-1/2) + Slack (SEV-3) |

## Alert philosophy

Alert on **symptoms users feel** (payment latency, stale revenue, fraud fallback),
not on causes (CPU, memory) — causes go on dashboards for diagnosis. Every
SEV-1/SEV-2 alert names its runbook anchor in `10_runbooks.md`. An alert without a
runbook is a bug in this document.

## Distributed tracing

One trace ID follows a payment across every hop: API → PostgreSQL → Debezium →
Kafka → Faust → fraud decision → writeback. When fraud scoring slows, the trace
shows *which* hop — feature lookup, model inference, or the Mongo transaction —
rather than leaving on-call to guess.
