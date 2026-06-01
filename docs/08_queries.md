# 08 — SQL and NoSQL Queries

## Objective

This document demonstrates how the proposed data models answer key business questions across OLTP, OLAP and NoSQL systems.

## 1. OLTP — Abnormal Transaction Velocity

Business question:

> Which customers have unusually high transaction activity in the last hour?

```sql
WITH tx_velocity AS (
    SELECT
        customer_id,
        COUNT(*) AS tx_count_1h,
        SUM(amount_usd) AS amount_1h
    FROM transactions
    WHERE created_at >= now() - INTERVAL '1 hour'
      AND status <> 'failed'
    GROUP BY customer_id
)
SELECT
    customer_id,
    tx_count_1h,
    amount_1h
FROM tx_velocity
WHERE tx_count_1h > 10
   OR amount_1h > 5000
ORDER BY amount_1h DESC;