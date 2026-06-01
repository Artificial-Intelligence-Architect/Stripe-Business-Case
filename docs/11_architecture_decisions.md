## Architectural Trade-offs

### PostgreSQL + Citus
Pros:
- Strong ACID guarantees
- mature ecosystem
- horizontal sharding by merchant_id

Cons:
- operational complexity
- cross-shard joins must be avoided
- not a drop-in solution for every OLTP workload

### Snowflake
Pros:
- elastic compute
- strong analytical performance
- Time Travel for audit/recovery

Cons:
- vendor lock-in
- cost control required
- not suitable for low-latency OLTP