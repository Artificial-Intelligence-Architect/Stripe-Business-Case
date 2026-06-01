# 04 — NoSQL Data Model

## Objective

The NoSQL layer stores semi-structured and unstructured data required for fraud detection, user behaviour analysis and operational monitoring.

## Technology Choice

**MongoDB Atlas** is selected because it supports:

- flexible JSON document structures
- nested fraud signals and ML features
- aggregation pipelines
- TTL indexes
- horizontal scaling through sharding
- integration with Kafka and Python workflows

## Collections

| Collection        | Purpose                                                       |
|-------------------|---------------------------------------------------------------|
| `fraud_events`    | Stores fraud scores, signals, decisions and model metadata    |
| `user_sessions`   | Stores clickstream and checkout journey data                  |
| `app_logs`        | Stores application and operational logs                       |

## Document Design

### `fraud_events`

Stores fraud-related information close to the transaction.

Embedded fields include:

- fraud score
- model version
- velocity features
- IP risk
- device fingerprint match
- decision outcome

### `user_sessions`

Uses embedded events because session events are usually read together.

This supports:

- checkout funnel analysis
- conversion tracking
- behavioural features

### `app_logs`

Stores operational logs with contextual metadata.

This supports:

- troubleshooting
- latency analysis
- incident investigation

## Embedding vs Referencing

| Pattern       | Used For                                  | Reason                                            |
|---------------|-------------------------------------------|---------------------------------------------------|
| Embedding     | fraud signals, session events             | read together, low join requirement               |
| Referencing   | transaction_id, merchant_id, customer_id  | links MongoDB documents to OLTP and OLAP entities |

## Index Strategy

| Collection        | Index                         | Purpose                   |
|-------------------|-------------------------------|---------------------------|
| `fraud_events`    | `{ merchant_id, timestamp }`  | merchant risk queries     |
| `fraud_events`    | `{ fraud_signals.score }`     | fraud threshold filtering |
| `fraud_events`    | TTL on `ttl_expires_at`       | retention and privacy     |
| `user_sessions`   | `{ customer_id, started_at }` | customer journey analysis |
| `app_logs`        | TTL on `created_at`           | log lifecycle management  |

## Data Retention

Retention is managed through TTL indexes and compliance workflows.

Examples:

- fraud events: 90 days unless legally required
- application logs: 30 days
- anonymised references retained for audit aggregates

## Trade-offs

| Choice                | Benefit               | Trade-off                         |
|-----------------------|-----------------------|-----------------------------------|
| MongoDB               | flexible schema       | weaker relational constraints     |
| Embedded documents    | fast reads            | possible duplication              |
| TTL indexes           | automated retention   | requires careful legal validation |

## Alignment with Project Requirements

This model satisfies the NoSQL requirements by providing:

- flexible schema design
- semi-structured data support
- embedded and referenced relationships
- indexes for efficient querying
- integration with OLTP, OLAP and ML workflows