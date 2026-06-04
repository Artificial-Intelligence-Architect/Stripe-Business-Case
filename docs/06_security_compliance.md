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

### KMS Key Rotation (Snowflake & AWS)

**Automated procedure every 90 days:**

1. Generate a new master key in AWS KMS (alias `stripe-snowflake-key-v2`).
2. Update the key policy to authorise Snowflake to use it.
3. In Snowflake, run:
   ```sql
   ALTER ACCOUNT SET MASTER_KEY = 'new_key_arn';