## CDC Failure Management

- Schema changes are validated through Schema Registry.
- Invalid events are routed to a Dead Letter Queue.
- Consumers are idempotent using transaction_id as business key.
- Replay is supported from Kafka retention.
- High-water marks are stored for batch recovery.

### Additional Operational Details

#### Partitioning Strategy in Snowflake
- The `fact_transactions` table is partitioned by `transaction_date` (daily).
- Dynamic tables are refreshed every 5 minutes for near real‑time metrics.
- Recommendation: enable automatic clustering on `(merchant_id, transaction_date)`.

#### Failure Handling and Recovery
- **Kafka**: partition replication across 3 brokers, 7‑day retention.
- **Debezium**: stores offsets in a dedicated topic with fault tolerance.
- **Airflow**: DAGs are configured with `retries=3` and `retry_delay=5min`. Use task SLAs for deadlines.

#### Pipeline Monitoring
- Critical metrics: Kafka lag (max 1000 messages), Airflow run duration (alert > 1h), Faust worker failure rate.
- Proposed tools: **Prometheus + Grafana** for the Kafka/Faust stack, **Datadog** for Snowflake and Airflow.