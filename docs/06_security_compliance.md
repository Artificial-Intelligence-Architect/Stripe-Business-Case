# 06 — Security & Compliance

> This file previously began mid-sentence (no title, indented as a code block)
> and carried a "Compliance Readiness Scores" table claiming **PCI-DSS 100% —
> Fully compliant**. A design cannot certify itself; compliance is attested by a
> QSA/auditor against a running system, not asserted in a README. The scores
> below are reframed as **design maturity**, which is what this repository can
> honestly claim.

## Encryption

| Layer | Mechanism |
|---|---|
| In transit | TLS 1.3 everywhere (client↔API, API↔DB, inter-service) |
| At rest | AES-256 — PostgreSQL TDE, Snowflake native, MongoDB Atlas encrypted storage |
| Key management | AWS KMS; keys rotated every 90 days (procedure below) |
| Application-level | PAN/CVV never touch our systems — tokenised by Stripe upstream |

## Secrets management

Secrets are never stored in code or in environment files committed to Git.

- HashiCorp Vault (or AWS Secrets Manager) as the store
- Automatic rotation for database credentials
- Short-lived tokens for service accounts (no static long-lived keys)

## Access control

Role-based, least-privilege, enforced in the database itself — see
`sql/security/rbac_setup.sql`. Four roles: `analyst_read` (column- and
row-restricted, no PII), `engineer_write` (no DELETE, no audit access),
`compliance_officer` (full read incl. audit), `ml_service` (feature columns +
pseudonymous `customer_id` only). Row-Level Security scopes analysts to their own
merchants. Column privileges are granted per-column, not granted-then-revoked (the
revoke-after-grant pattern is a no-op in PostgreSQL — documented in that file).

## Data classification

| Class | Examples | Controls |
|---|---|---|
| **Public** | documentation | none |
| **Internal** | technical metrics | authenticated access |
| **Confidential** | merchant metadata, analytical aggregates | RBAC, encrypted at rest |
| **Restricted** | customer PII, payment identifiers, fraud signals | RBAC + column grants + RLS + audit log + encryption |

## KMS key rotation (every 90 days)

1. Generate a new master key in AWS KMS (`stripe-snowflake-key-v2`).
2. Update the key policy to authorise Snowflake.
3. In Snowflake: `ALTER ACCOUNT SET MASTER_KEY = '<new_key_arn>';`
4. Re-key active objects; retire the previous key after the grace window.

## Audit logging

Every write to `transactions` and `subscriptions` is captured in an append-only
`audit_log` (enforced by trigger — UPDATE/DELETE on the log raise). Compliance
events carry an indexed `reason` column so the daily report filters on it directly
rather than on a JSONB path. See `sql/oltp/schema.sql`.

## GDPR

Erasure is implemented across **all** systems, not just PostgreSQL — the design's
hardest problem and the one most projects skip. Full treatment in
`13_gdpr_cross_system_erasure.md`. Data portability (art.20) is implemented as
`gdpr_export_customer()` in `sql/security/gdpr_erasure.sql`.

## Compliance design maturity

Not an audit result — a self-assessment of how much of each framework the
**design** addresses. Real compliance requires an external assessor against a
running system.

| Framework | Design coverage | What is in place | What a real attestation still needs |
|---|---|---|---|
| **PCI-DSS** | SAQ-A scope | No PAN/CVV stored (Stripe tokenisation removes them from scope); TLS 1.3 + AES-256; full access logging | QSA assessment; network segmentation evidence; pen-test |
| **GDPR** | High | Cross-system erasure, portability export, consent/DPA model, audit trail | DPO sign-off; DPIA on the fraud model |
| **CCPA** | High | Opt-out flag + procedure; no data selling; access/deletion requests | "Do Not Sell" UX; verification workflow hardening |
| **SOC 2 Type II** | Partial | Controls documented; access restrictions | External audit over a 6-12 month observation window |
| **ISO 27001** | Partial | Policies and processes defined | Formal certification body |

> The gaps are deliberately shown. A design claiming to be "100% compliant" before
> any external assessment is a red flag; this table is the honest version.
