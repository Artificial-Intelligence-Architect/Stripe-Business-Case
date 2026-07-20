# 13 — Cross-System GDPR Erasure

## The gap this document closes

v1 implemented `gdpr_erase_customer()` in PostgreSQL and stopped there.

That is not erasure. The same customer's personal data also sits in MongoDB,
Snowflake, Kafka, S3 and every backup taken in the last 35 days. A data subject
told "you have been erased" while their session fingerprints, IP geolocation and
free-text reviews remain queryable in four other systems has not been erased —
and the organisation has told them something untrue in writing.

This is the first question any competent auditor or jury asks: **"and your other
systems?"** This document is the answer.

## Where personal data actually lives

| System | Personal data | Erasure mechanism | Lag |
|---|---|---|---|
| **PostgreSQL** `customers` | email, email_hash | Anonymise in place, `is_erased = true` | Immediate |
| **PostgreSQL** `transactions` | ip_country, device_type | Null out. **Amounts/dates retained** (art.17(3)(b) — legal obligation) | Immediate |
| **MongoDB** `fraud_events` | customer_id, ML features | Anonymise `customer_id` → `erased_<hash>` | < 1 h |
| **MongoDB** `user_sessions` | ip_address_hash, device fingerprint, clickstream | Delete document (behavioural data has no retention obligation) | < 1 h |
| **MongoDB** `customer_feedback` | free-text body, `customer_id` | Redact body, null customer_id, `is_erased = true` — keep rating for aggregates | < 1 h |
| **MongoDB** `dispute_documents` | evidence PDFs/images | **Legal hold**: retained while dispute is open, purged on close + 13 months (card-scheme rule) | Deferred |
| **Snowflake** `dim_customer` | country, segment | `is_erased = true`; downstream marts filter it out | < 24 h |
| **Snowflake** Time Travel | Everything, retroactively | ⚠️ see below | Up to 90 days |
| **Kafka** topics | Full CDC payloads | ⚠️ see below | Up to 7 days |
| **S3 / backups** | Everything | ⚠️ see below | Up to 35 days |

## The three honest problems

Most compliance plans quietly skip these. Naming them is stronger than pretending
they do not exist.

### 1. Kafka topics are immutable

A Kafka log cannot be edited. The customer's PII sits in `stripe.public.customers`
for the full 7-day retention, and there is no `DELETE FROM topic`.

**What we do:** the erasure event is published as a **tombstone** — a record with
the customer key and a `null` value. On a topic with `cleanup.policy=compact`, log
compaction eventually removes all prior records for that key.

**The honest caveat:** compaction is asynchronous and gives no deadline. It is
therefore a best-effort control, not a guarantee. The actual guarantee is the
7-day retention: after 7 days the data is gone regardless. Since GDPR allows one
month to respond (art.12(3)), a 7-day window is **inside** the legal deadline. We
document this as "erasure completes within retention" rather than claiming
instant deletion — which would be false.

**The alternative we rejected:** crypto-shredding (encrypt each customer's PII
with a per-customer key, delete the key on erasure). It is the technically
superior answer and genuinely makes the data unrecoverable everywhere at once,
including backups. We rejected it for *this* design because it requires
per-customer key management across five systems and turns every analytical read
into a key lookup. **For a real Stripe-scale deployment, this is the correct
choice and the 7-day tombstone approach is a compromise.** Stated plainly so the
trade-off is visible rather than hidden.

### 2. Snowflake Time Travel resurrects deleted data

`dim_customer` at `TARGET_LAG` is anonymised — but `SELECT * FROM dim_customer AT
(OFFSET => -86400)` returns the pre-erasure row. Time Travel is a retention
period, and retention is exactly what art.17 forbids.

**What we do:**
- `DATA_RETENTION_TIME_IN_DAYS = 1` on every table containing PII (default is 1
  for standard tables, but **90 for Enterprise** — this must be set explicitly, it
  is the trap).
- Fail-safe (7 further days, Snowflake-internal, **not disableable**) is disclosed
  in the DPA as a documented sub-processor retention window.
- The erasure request is only closed after `retention + 1 day`.

The `snowflake_done` flag in `gdpr_erasure_requests` is therefore set on a delay,
not immediately. That delay is deliberate and defensible.

### 3. Backups cannot be selectively edited

A PITR backup from 20 days ago contains the customer. Rewriting backups is neither
possible nor desirable — it would destroy their integrity as a recovery mechanism.

**What we do:** the recognised industry position, and the one the CNIL and EDPB
accept — backups are **frozen**, not edited. The erasure is recorded in a
**replay list**; if a backup is ever restored, the erasure list is re-applied
immediately post-restore, before the system is opened to traffic. Backup rotation
(35 days) then removes the data naturally.

This is documented in the DPA and the Record of Processing Activities. It is a
recognised limitation, not a violation — *provided* the replay list exists and is
tested. Ours is exercised in the quarterly DR drill (see `10_runbooks.md`).

## Propagation flow

```
   Data subject request (verified identity — art.12(6))
              │
              ▼
   INSERT gdpr_erasure_requests (status=pending, sla_due_at=+30d)
              │
              ├── legal_hold? ──yes──▶ status=rejected + documented reason
              │                        (art.17(3)(b): AML/PCI retention)
              no
              ▼
   CALL gdpr_erase_customer()            ──▶ postgres_done = true
              │
              ▼
   Kafka topic: stripe.gdpr.erasure_requests
   {customer_id, merchant_id, request_id, requested_at}
              │
      ┌───────┴────────┬─────────────────┐
      ▼                ▼                 ▼
  Faust consumer   Airflow task      Tombstone to
  → MongoDB        → Snowflake       stripe.public.customers
  (4 collections)  (dim_customer)    (compaction)
      │                ▼
      │          snowflake_done=true (after retention+1d)
      ▼
  mongodb_done = true
      │
      └────────────▶ CALL gdpr_close_request()
                     ─ requires ALL THREE flags ─
                     status = completed
                     audit_log ← 'GDPR_erasure_completed'
```

The request is **not** closed when PostgreSQL finishes. It is closed when every
system confirms. The Airflow check `gdpr_stuck_propagation` alerts on requests
where `postgres_done = true` but Mongo or Snowflake have not confirmed for >24h —
because that state is precisely how an organisation comes to *believe* it is
compliant while it is not.

## What is deliberately NOT erased

Erasure is not absolute. Art.17(3) provides exemptions, and financial services
rely on them:

| Data | Retained | Legal basis |
|---|---|---|
| Transaction amount, currency, date | 10 years | Art.17(3)(b) — accounting obligation (Code de commerce L123-22) |
| AML/KYC records | 5 years after relationship ends | AMLD5 art.40 |
| `audit_log` entries | 7 years | PCI-DSS req.10.7 + evidentiary value |
| The erasure record itself | Indefinitely | Proof of compliance — deleting it destroys the evidence that we complied |

The customer becomes **pseudonymous**, not absent: transactions still reference a
`customer_id` UUID, but nothing links that UUID to a person. This is the correct
reading of art.17 for a payment institution, and it is why the procedure
anonymises rather than deletes.

Telling a data subject "we deleted everything" would be both false and illegal —
we are legally *required* to keep their financial records. The response template
says so explicitly.

## Verification

```sql
-- No completed request may leave data behind
SELECT r.request_id, r.postgres_done, r.mongodb_done, r.snowflake_done
FROM gdpr_erasure_requests r
WHERE r.status = 'completed'
  AND NOT (r.postgres_done AND r.mongodb_done AND r.snowflake_done);
-- MUST return 0 rows. Enforced by gdpr_close_request().

-- No erased customer may retain an email
SELECT COUNT(*) FROM customers
WHERE is_erased = true AND email NOT LIKE 'erased_%@deleted.invalid';
-- MUST return 0.

-- SLA compliance
SELECT COUNT(*) FROM gdpr_erasure_requests
WHERE status IN ('pending','processing') AND sla_due_at < now();
-- MUST return 0. Airflow raises AirflowException otherwise.
```

```javascript
// MongoDB: no erased customer may retain identifiers
db.user_sessions.countDocuments({ customer_id: { $in: erasedIds } })      // → 0
db.fraud_events.countDocuments({ customer_id: { $in: erasedIds } })       // → 0
db.customer_feedback.countDocuments({ customer_id: { $in: erasedIds },
                                      is_erased: { $ne: true } })          // → 0
```
