# 14 — Documentation Accessibility

> **Why this document exists.** An architecture is only useful if it can be
> understood, maintained and audited by the *whole* team — including people with
> disabilities. This document sets out the choices made to keep the entire
> documentation accessible, and provides the **text alternatives** for every
> diagram so that visual information remains available without sight.
>
> Target standard: accessibility aligned with **WCAG 2.1 level AA** (and its French
> transposition, **RGAA 4.1**).

## 1. Principles applied

| Axis (WCAG / RGAA) | Concrete decision in this project |
|---|---|
| **Perceivable** | Every information-bearing image has a text alternative (§4 below). No information is conveyed by colour alone. |
| **Operable** | Documentation in Markdown: keyboard-navigable, screen-reader compatible, structured with hierarchical headings (`#`, `##`, `###`) with no skipped levels. |
| **Understandable** | Plain language, acronyms expanded on first use, a glossary at the end of `00_overview.md`. |
| **Robust** | Open, durable formats (Markdown, SVG, text) readable by assistive technologies and independent of any proprietary software. |

## 2. Navigable structure

- **Strict heading hierarchy**: each document follows a `#` → `##` → `###` tree
  with no skipped levels. A screen reader can therefore announce the outline and
  jump straight to any section (the "list of headings" shortcut).
- **File numbering** (`00_`, `01_`, … `14_`): the reading order is explicit and
  linear, without relying on a visual menu.
- **Tables of contents** at the head of longer documents, with clickable anchors.
- **Descriptive links**: links describe their target ("see
  `13_gdpr_cross_system_erasure.md`") rather than "click here", so they remain
  meaningful out of context for a screen reader.

## 3. Colour and contrast

- **Information never depends on colour alone.** In status tables, a state is
  always paired with a word or a textual symbol ("✅ PASS", "❌ FAIL", "⚠️"),
  never a coloured dot on its own.
- **Diagrams** are rendered on a white background with dark text for high contrast
  (ratio ≥ 4.5:1, the WCAG AA threshold for text).
- **Status emojis** are doubled with a word: a colour-blind reader sees "PASS", not
  just a green tick.

## 4. Text alternatives for the diagrams

> These descriptions make each diagram understandable without seeing it. They
> should be placed in the caption of the corresponding diagram (the `alt`
> attribute in HTML, or adjacent text in Markdown).

### 4.1 — `data_pipeline.png` (end-to-end data flow)

> **Text alternative.** Left-to-right flow diagram in five tiers. **Sources**:
> PostgreSQL OLTP (transactions, subscriptions, customers, products) and the Stripe
> API. **Ingestion**: Debezium captures changes from PostgreSQL's write-ahead log
> (CDC) and publishes them to Kafka topics (transactions, subscriptions, GDPR
> erasure requests). **Processing**: from Kafka, two routes — a real-time route via
> Faust/Flink for fraud scoring (under 50 ms), and a batch route via Kafka Connect
> to S3 then Snowpipe. **Destinations**: MongoDB Atlas (six collections:
> fraud_events, user_sessions, app_logs, customer_feedback, dispute_documents,
> recommendations, plus a GridFS bucket) and Snowflake OLAP (fact_transactions,
> fact_subscription_events, SCD2 dimensions, Dynamic Tables). **Orchestration**:
> Airflow and dbt drive the daily batch. **Monitoring**: Evidently AI (model drift)
> and Slack/PagerDuty alerts. The guiding principle: the application writes only to
> PostgreSQL; everything else is fed by CDC.

### 4.2 — `erd_oltp.png` (OLTP entity-relationship model)

> **Text alternative.** Entity-relationship diagram of the transactional model.
> **Reference tables** (replicated): countries, currencies, product_categories.
> **Entities distributed by merchant_id**: merchants (primary key merchant_id);
> customers (composite key merchant_id + customer_id, email unique per merchant);
> products; subscriptions; subscription_events (status history). **Central table**:
> transactions, composite primary key (merchant_id, transaction_id, created_at),
> partitioned by date. A transaction carries a "kind" field (payment, refund,
> chargeback) and, for refunds, a parent_transaction_id link back to the original
> payment. **Journal**: audit_log, append-only. **Relationships**: a merchant owns
> many customers, products, subscriptions and transactions; a subscription
> generates many events and transactions; a refund transaction references its
> parent transaction.

### 4.3 — `architecture_diagram.png` (overall three-pillar architecture)

> **Text alternative.** Three-pillar architecture view connected by a central
> pipeline. Pillar 1, OLTP: PostgreSQL + Citus, the system of record ("where the
> money lives"). Pillar 2, OLAP: Snowflake, storage/compute separation, off the
> payment critical path. Pillar 3, NoSQL: MongoDB Atlas, flexible data and machine
> learning features. At the centre, the integration layer: Debezium (CDC), Kafka,
> Airflow, dbt. Cross-cutting: governance (Vault for secrets, Okta for
> authentication, audit log) and monitoring (Prometheus, Grafana, Evidently). Key
> message: failures in the analytical layers never interrupt payments.

### 4.4 — `data_ingestion.png` (CDC ingestion detail)

> **Text alternative.** Close-up of change data capture. PostgreSQL's write-ahead
> log (WAL) is read by Debezium via logical replication, with no impact on
> transactional performance. Events are serialised in Avro (validated by a Schema
> Registry) and published to Kafka, keyed by merchant_id to guarantee per-merchant
> ordering. Invalid events are routed to a dead-letter queue rather than lost.

### 4.5 — Demonstration screenshots (`demo/screenshots/`)

> **`airflow_dag_grid_run.png`** — Text alternative: grid view of the Airflow
> interface showing the daily runs of the `stripe_daily_etl` DAG, with each task
> (source validation, dbt run, dbt test, freshness check, compliance report) in
> "success" status (green), across several days.
>
> **`airflow_graph_run.png`** — Text alternative: graph view of the same DAG,
> showing the task order and the independent compliance branch that runs even if
> the analytics pipeline fails.
>
> **`sql_query_result.png`** — Text alternative: result of an analytical SQL query
> (monthly net revenue per merchant with month-on-month growth), displayed as a
> table of rows and columns.

## 5. Plain language

- Acronyms expanded on first use (CDC = Change Data Capture; SCD2 = Slowly Changing
  Dimension type 2; RBAC = role-based access control).
- Short sentences, one idea per sentence in the explanatory sections.
- A **glossary** collects the recurring technical terms.
- Architecture decisions are stated together with their *reason* ("we choose X
  because Y"), which helps non-specialist readers follow the reasoning.

## 6. Limitations and continuous improvement

Honestly, accessibility is a process, not a box to tick. As it stands:

- Diagrams are provided as **PNG + Mermaid source**; the Mermaid source is text, so
  readable by a screen reader, but an **SVG export with `<title>`/`<desc>` tags**
  would be superior and is the next step.
- The text alternatives above are written but should eventually be embedded
  directly as **`alt` attributes** in an HTML version of the documentation, rather
  than sitting in a separate document.
- No formal third-party RGAA audit has been carried out; this document describes an
  **accessible design**, not audited conformance — the same honesty applied to
  PCI-DSS (see `06_security_compliance.md`).
