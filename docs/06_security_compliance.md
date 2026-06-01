## Secrets Management

Secrets are never stored in code or environment files committed to Git.

Recommended tools:
- HashiCorp Vault or AWS Secrets Manager
- automatic rotation for database credentials
- short-lived tokens for service accounts

## Data Classification

Public:
- documentation

Internal:
- technical metrics

Confidential:
- merchant metadata
- analytical aggregates

Restricted:
- customer PII
- payment identifiers
- fraud signals