## CDC Failure Management

- Schema changes are validated through Schema Registry.
- Invalid events are routed to a Dead Letter Queue.
- Consumers are idempotent using transaction_id as business key.
- Replay is supported from Kafka retention.
- High-water marks are stored for batch recovery.