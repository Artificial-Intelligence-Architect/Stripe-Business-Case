## Runbooks

### Kafka consumer lag
1. Check consumer group lag.
2. Verify broker health.
3. Scale consumers horizontally.
4. Reprocess from committed offset if needed.

### Airflow DAG failure
1. Inspect failed task logs.
2. Validate source freshness.
3. Re-run only failed task when idempotent.
4. Escalate if SLA breach risk.

### GDPR deletion request
1. Validate customer identity.
2. Run anonymization procedure.
3. Propagate deletion/anonymization event to Kafka.
4. Confirm deletion in PostgreSQL, MongoDB and Snowflake.