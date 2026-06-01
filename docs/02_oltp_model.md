# 02 — OLTP Data Model

## Objective

The OLTP layer supports high-volume payment transactions with strong consistency, low latency and auditability.

## Technology Choice

**PostgreSQL with Citus** is selected for:

- ACID transactions
- mature relational modelling
- horizontal sharding by `merchant_id`
- native indexing and partitioning
- compatibility with CDC through logical replication

## Core Entities

| Table             | Purpose                                                   |
|-------------------|-----------------------------------------------------------|
| `customers`       | Stores customer reference data                            |
| `merchants`       | Stores merchant metadata                                  |
| `transactions`    | Stores payments, refunds, chargebacks and failed attempts |
| `currencies`      | Stores currency exchange rates                            |
| `audit_log`       | Stores immutable change history                           |

## Normalisation Strategy

The model is normalised to reduce duplication and preserve transactional integrity.

- customers and merchants are stored separately
- transactions reference both entities
- currencies are managed as reference data
- audit records are append-only

## Performance Strategy

| Technique                     | Purpose                                         |
|-------------------------------|-------------------------------------------------|
| Partitioning by `created_at`  | Improves pruning and archival                   |
| Sharding by `merchant_id`     | Enables horizontal scale-out                    |
| Partial indexes               | Optimises fraud and pending transaction queries |
| Connection pooling            | Supports high concurrency                       |
| Logical replication           | Feeds Kafka through CDC                         |

## Consistency and Recovery

The OLTP system is designed for:

- ACID compliance
- synchronous replication for critical writes
- point-in-time recovery
- audit logging
- CDC-based downstream synchronisation

## Trade-offs

| Choice            | Benefit               | Trade-off                             |
|-------------------|-----------------------|---------------------------------------|
| PostgreSQL        | mature ACID database  | requires careful scaling              |
| Citus             | horizontal sharding   | cross-shard joins must be avoided     |
| Normalised model  | consistency           | more joins for analytical use cases   |

## Alignment with Project Requirements

This model satisfies the OLTP requirements by providing:

- high-volume transactional processing
- ACID guarantees
- real-time replication support
- failover readiness
- fraud-related transactional attributes