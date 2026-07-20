# 04 — NoSQL Data Model

> Aligned with the code. The previous version described **3** collections and
> indexes that do not exist in `mongodb_schema.py` (TTL on `ttl_expires_at` and
> `created_at`; index `{customer_id, started_at}`). The schema actually defines
> **6** collections plus a GridFS bucket, with the TTL and index definitions
> reproduced faithfully below.

## Objective

The NoSQL layer stores semi-structured and unstructured data required for fraud
detection, behavioural analysis, customer feedback, dispute evidence and
operational monitoring.

## Technology choice

**MongoDB Atlas**, for: flexible JSON documents, nested fraud signals and ML
features, aggregation pipelines, TTL indexes, GridFS for binary/XML payloads
larger than the 16 MB BSON limit, horizontal sharding, and native Kafka/Python
integration.

## Collections

| Collection | Purpose | TTL |
|---|---|---|
| `fraud_events` | Fraud scores, signals, decisions, model metadata | 90 days |
| `user_sessions` | Clickstream and checkout journey | 180 days |
| `app_logs` | Application and operational logs | 30 days |
| `customer_feedback` | Reviews + survey responses + NLP sentiment | none (business memory) |
| `dispute_documents` | Metadata for XML + binary evidence; payloads in GridFS `dispute_evidence` | legal-hold based |
| `recommendations` | Precomputed per-customer recommendations (personalisation) | 30 days |

`customer_feedback` and `dispute_documents` satisfy "Customer Feedback" as a
NoSQL data source and "JSON, XML, and binary data" respectively. `recommendations` is the serving collection for the personalisation ML use case
(docs/07) — the recommender writes here, checkout reads here.

## Document design

**`fraud_events`** — fraud data kept close to the transaction: fraud score,
model version, velocity features, IP risk, device fingerprint match, decision
outcome. Written transactionally with the session flag and audit log (see
`mongodb_multidoc_transactions.py`).

**`user_sessions`** — embedded event array (session events are read together):
checkout funnel, conversion, behavioural features.

**`app_logs`** — operational logs with contextual metadata for troubleshooting,
latency analysis and incident investigation.

**`customer_feedback`** — rating (1-5) and NPS (0-10) kept as distinct scales;
survey answers as an array of `{question_id, answer}` so quarterly survey changes
need no migration; `sentiment` sub-document written back by the NLP model, making
the document both raw record and feature-store entry.

**`dispute_documents`** — one metadata document per evidence file. Small payloads
(< 1 MB) inline as BinData; large ones (5-50 MB scanned bundles) in the GridFS
bucket `dispute_evidence`. XML representment files stored **raw** (byte-identical
legal evidence) and **parsed** (queryable projection) side by side.

## Embedding vs referencing

| Pattern | Used for | Reason |
|---|---|---|
| Embedding | fraud signals, session events, survey answers, sentiment | read together, low join requirement |
| Referencing | `transaction_id`, `merchant_id`, `customer_id` | links documents to OLTP/OLAP entities |
| GridFS | dispute evidence > 16 MB | exceeds BSON document limit |

## Index strategy (as implemented)

| Collection | Index | Purpose |
|---|---|---|
| `fraud_events` | TTL on `timestamp`, 90 d (`expireAfterSeconds=7776000`) | GDPR automatic purge |
| `fraud_events` | `{transaction_id}` unique | idempotent upserts |
| `fraud_events` | `{merchant_id, timestamp ↓}` | merchant risk queries |
| `fraud_events` | `{customer_id, timestamp ↓}` | per-customer velocity |
| `fraud_events` | `{fraud_signals.score ↓}` | threshold filtering |
| `user_sessions` | TTL on `started_at`, 180 d (`expireAfterSeconds=15552000`) | retention |
| `user_sessions` | `{session_id}` unique | primary lookup |
| `user_sessions` | `{customer_id, ended_at ↓}` | `$lookup` join key to OLAP |
| `user_sessions` | `{device.fingerprint}` | device-based fraud correlation |
| `app_logs` | TTL on `timestamp`, 30 d (`expireAfterSeconds=2592000`) | log lifecycle |
| `app_logs` | `{service, level, timestamp ↓}` | operational filtering |
| `customer_feedback` | `{title, body}` TEXT | full-text search over prose |
| `customer_feedback` | `{sentiment.label, submitted_at ↓}` | sentiment trend |
| `dispute_documents` | `{sha256}` unique sparse | integrity + de-duplication |
| `dispute_documents` | `{parsed_xml.reason_code}`, `{parsed_xml.deadline}` | dispute triage |

The correct join key for the OLAP `$lookup` is `{customer_id, ended_at}` — a
session is joined to analytics by when it *ended*, not when it started. The
earlier doc listed `{customer_id, started_at}`, which is not the index the code
builds.

## Data retention

TTL indexes purge automatically: fraud events 90 days, sessions 180 days, logs
30 days. `customer_feedback` has **no** TTL — it is business memory (NPS trend and
the sentiment model's training set), not telemetry. `dispute_documents` retention
is legal-hold based (kept while a dispute is open, purged on close + 13 months per
card-scheme rules), never TTL. GDPR erasure across these collections is handled
separately — see `13_gdpr_cross_system_erasure.md`.

## Trade-offs

| Choice | Benefit | Trade-off |
|---|---|---|
| MongoDB | flexible schema | weaker relational constraints (mitigated by `$jsonSchema` validators) |
| Embedded documents | fast single-read | possible duplication |
| TTL indexes | automated retention | must exempt legal-hold data (feedback, disputes) |
| GridFS | handles > 16 MB binary | extra round-trip vs inline BinData |

## Alignment with requirements

Flexible schema design; semi-structured **and** unstructured support (JSON,
full-text prose, XML, binary via GridFS); embedded and referenced relationships;
purpose-built indexes including full-text; multi-document ACID transactions where
atomicity must span collections (`mongodb_multidoc_transactions.py`).
