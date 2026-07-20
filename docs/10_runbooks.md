# 10 — Runbooks

Operational procedures for on-call. Each runbook has an anchor referenced from
code and other docs (e.g. `#ingestion-gap`, `#dynamic-table-lag`).

---

## #ingestion-gap — no data in staging

**Symptom:** `validate_sources` raised "No rows in staging.raw_transactions".
The Debezium → Kafka → S3 → Snowpipe chain is broken somewhere.

1. **Is it real?** Check whether *any* payments occurred in the window (a genuine
   zero at 03:00 on a low-volume merchant is possible). `SELECT max(created_at)
   FROM transactions;` on OLTP.
2. **Debezium alive?** `curl localhost:8083/connectors/stripe-cdc/status`. If
   `FAILED`, read the trace. Most common: PostgreSQL WAL slot dropped, or a schema
   change the connector could not handle.
3. **WAL growth.** If the connector was down, the replication slot held the WAL:
   `SELECT slot_name, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(),
   restart_lsn)) FROM pg_replication_slots;`. Above ~80% disk, this is now the
   priority — a full WAL disk takes OLTP down.
4. **Kafka flowing?** `kafka-console-consumer --topic stripe.public.transactions
   --max-messages 5`. Messages = capture works, sink is the problem. Silence =
   capture is the problem (back to step 2).
5. **Snowpipe.** `SELECT SYSTEM$PIPE_STATUS('stripe_pipe');` — check
   `pendingFileCount` and `lastReceivedMessageTimestamp`.
6. **Recovery:** restart the failed stage. Kafka's 7-day retention means no data
   is lost as long as the fix lands within the window. Do **not** manually
   backfill Snowflake — re-enabling Snowpipe replays from the S3 notification queue.

---

## #dynamic-table-lag — Snowflake marts stale

**Symptom:** `assert_dynamic_table_freshness` raised; a dynamic table exceeded
1.5× its `TARGET_LAG`.

1. **Refresh history:** `SELECT * FROM TABLE(INFORMATION_SCHEMA
   .DYNAMIC_TABLE_REFRESH_HISTORY(NAME => 'mv_daily_revenue')) ORDER BY
   refresh_start_time DESC LIMIT 10;`. Look for `FAILED` / `state_message`.
2. **Warehouse suspended or queued?** A resized/suspended `ANALYTICS_WH` stalls
   refreshes. Check `SHOW WAREHOUSES;` and query load.
3. **Upstream lag:** a dynamic table cannot be fresher than its sources. If
   `fact_transactions` is behind (dbt still running / failed), fix that first —
   the lag is a symptom, not the cause.
4. **Do NOT** issue `ALTER DYNAMIC TABLE ... REFRESH` as a reflex. That contradicts
   the declared `TARGET_LAG` and doubles warehouse cost. Fix the cause; Snowflake
   catches up on its own.
5. **Escalate** if a revenue-critical table (`mv_daily_revenue`,
   `mv_subscription_mrr`) is > 3× target — merchant dashboards are showing stale
   numbers.

---

## #kafka-consumer-lag

1. Check consumer group lag: `kafka-consumer-groups --describe --group fraud-scorer`.
2. Verify broker health and in-sync replicas (`min.insync.replicas=2`).
3. Scale consumers horizontally (add Faust workers) up to the partition count.
4. If lag is on the fraud path, scoring is degrading toward the rules-only
   fallback — customers still transact, but with weaker fraud protection. Treat as
   SEV-2.
5. Reprocess from the committed offset once caught up; consumers are idempotent on
   `transaction_id`, so replay is safe.

---

## #airflow-dag-failure

1. Identify the failed task in the Airflow UI; read its log.
2. If `validate_sources` → go to [#ingestion-gap](#ingestion-gap).
3. If `assert_dynamic_table_freshness` → [#dynamic-table-lag](#dynamic-table-lag).
4. If a dbt task → `dbt test` output names the failing model/test. Data-quality
   failures (e.g. `refund_has_parent`) mean bad upstream data — do not merely retry.
5. Tasks are idempotent: re-run the single failed task. The **compliance branch runs
   independently** (`trigger_rule=ALL_DONE`), so a dbt failure does **not** block the
   GDPR/CCPA report — verify it still ran.

---

## #gdpr-erasure-request

1. Verify the data subject's identity (art.12(6)) before doing anything.
2. `INSERT INTO gdpr_erasure_requests (...)`; check `legal_hold` — an open AML/PCI
   retention obligation means the request is **rejected with a documented reason**,
   not silently ignored.
3. `CALL gdpr_erase_customer(merchant_id, customer_id, request_id);` → sets
   `postgres_done`.
4. Confirm propagation: the Kafka erasure event drives MongoDB (Faust) and
   Snowflake (Airflow). Watch `mongodb_done` / `snowflake_done`.
5. `CALL gdpr_close_request(request_id);` — succeeds **only** when all three
   systems confirm. If it raises, one system is behind: see
   [#gdpr-stuck-propagation](#gdpr-stuck-propagation).
6. Full cross-system detail: `13_gdpr_cross_system_erasure.md`.

---

## #gdpr-stuck-propagation

**Symptom:** compliance report flags an erasure with `postgres_done=true` but
Mongo/Snowflake incomplete for > 24h. **This is the state where the org believes
it is compliant but is not.** Treat as SEV-2 (legal exposure).

1. Which system? Query the flags on `gdpr_erasure_requests`.
2. **MongoDB behind:** is the Faust erasure consumer alive? Check its lag on
   `stripe.gdpr.erasure_requests`. Restart; it is idempotent.
3. **Snowflake behind:** expected up to `retention + 1 day` (Time Travel). If
   beyond that, check the Airflow erasure task.
4. Re-run `gdpr_close_request`. Document the delay in the request record for audit.

---

## #dr-drill (quarterly)

1. Restore the latest PITR backup to an isolated environment.
2. **Immediately re-apply the GDPR erasure replay list** before opening any
   traffic — a restored backup resurrects erased customers (see `13`).
3. Validate: no `is_erased=true` customer has a real email; row counts within
   tolerance; a sample of erased IDs absent from MongoDB.
4. Record RTO/RPO achieved vs target (RTO < 30 s failover / RPO 0 sync replication).
