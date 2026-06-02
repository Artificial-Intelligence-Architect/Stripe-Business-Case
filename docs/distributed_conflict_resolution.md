# Distributed Conflict Resolution — Stripe Data Architecture

## Context

Stripe's OLTP layer runs **PostgreSQL + Citus** (horizontal sharding by
`merchant_id`) with synchronous streaming replication to read replicas.
Three conflict scenarios can occur in this distributed setup.

---

## Scenario 1 — Concurrent updates on the same transaction row

**When it happens:** Two Citus worker nodes receive an UPDATE on the same
`transaction_id` within the same millisecond (e.g., a payment status update
races with a fraud-score update).

**Strategy: Serializable Snapshot Isolation (SSI)**

PostgreSQL's default isolation level is `READ COMMITTED`. For financial
transactions, Stripe sets `ISOLATION LEVEL SERIALIZABLE` at the session
level. SSI detects read/write dependencies between concurrent transactions
and aborts the one that would produce an anomaly, returning error code
`40001` (`serialization_failure`).

```sql
-- Applied at the application layer (e.g., Stripe's payment service)
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;

UPDATE transactions
SET    status     = 'success',
       updated_at = NOW()
WHERE  transaction_id = $1
  AND  status = 'pending';   -- optimistic lock: only update if still pending

-- If 0 rows updated → status already changed by a concurrent tx → retry
GET DIAGNOSTICS updated_count = ROW_COUNT;
COMMIT;
```

**Retry policy:** exponential backoff with jitter, max 3 retries, then
dead-letter to a Kafka topic `stripe.tx.conflict_dlq` for manual review.

---

## Scenario 2 — Citus shard rebalancing during a write

**When it happens:** The Citus coordinator moves shards between worker nodes
(scale-out event) while transactions are in-flight.

**Strategy: Distributed 2-Phase Commit (2PC)**

Citus uses PostgreSQL's native 2PC (`PREPARE TRANSACTION` / `COMMIT
PREPARED`) to guarantee atomicity across shards during rebalancing.
No application-level change is needed — the Citus coordinator handles
2PC transparently. The key configuration to enable:

```sql
-- postgresql.conf on all Citus nodes
max_prepared_transactions = 200   -- must be > max_connections
```

**Monitoring:** Stale prepared transactions (coordinator crashed mid-2PC)
are detected via:

```sql
SELECT gid, prepared, owner
FROM   pg_prepared_xacts
WHERE  prepared < NOW() - INTERVAL '5 minutes';
-- Alert if any row returned → manual ROLLBACK PREPARED 'gid' required
```

---

## Scenario 3 — WAL replication lag causing stale reads

**When it happens:** A read replica lags behind the primary (e.g., during
a network partition). A fraud check reads a stale `fraud_score` from the
replica while the primary has already updated it.

**Strategy: Synchronous replication + application-level read routing**

```sql
-- postgresql.conf on primary
synchronous_standby_names = 'FIRST 1 (replica_1, replica_2)'
synchronous_commit = on   -- default; ensures WAL flushed before ACK
```

For **fraud-critical reads**, the application bypasses the replica and
reads directly from the primary using a dedicated connection pool
(PgBouncer `server_reset_query` = `SET SESSION CHARACTERISTICS AS
TRANSACTION READ WRITE`).

For **non-critical reads** (dashboards, reporting), replica lag up to
500 ms is acceptable — this is within the Debezium CDC SLA.

**Monitoring query (run every 30 s via Datadog check):**

```sql
SELECT client_addr,
       state,
       pg_size_pretty(
           pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn)
       ) AS send_lag,
       pg_size_pretty(
           pg_wal_lsn_diff(sent_lsn, replay_lsn)
       ) AS replay_lag,
       write_lag,
       replay_lag AS replay_delay
FROM   pg_stat_replication;
-- Alert if replay_lag > 500 ms or send_lag > 10 MB
```

---

## Summary table

| Scenario | Root cause | Strategy | Recovery |
|---|---|---|---|
| Concurrent UPDATE | Race condition on same row | Serializable SSI + optimistic lock | Retry with backoff → DLQ |
| Shard rebalancing | Citus 2PC mid-flight | Native PostgreSQL 2PC | Monitor `pg_prepared_xacts` |
| Replica lag | WAL delivery delay | Sync replication + read routing | Primary reads for fraud path |
