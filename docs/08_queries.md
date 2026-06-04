# 08 — SQL and NoSQL Queries

## Objective

This document demonstrates how the proposed data models answer key business questions across OLTP, OLAP and NoSQL systems.

## 1. OLTP Queries (PostgreSQL + Citus)

### 1.1 Abnormal transaction velocity

Business question: *Which customers have unusually high transaction activity in the last hour?*

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

### 1.2 Reliable job queue with FOR UPDATE SKIP LOCKED

Business question: How to process pending transactions concurrently without conflicts?

-- Worker picks up 10 pending transactions atomically
BEGIN;

SELECT transaction_id, payload
FROM transaction_queue
WHERE status = 'pending'
ORDER BY created_at
LIMIT 10
FOR UPDATE SKIP LOCKED;

-- Then update them to 'processing'
UPDATE transaction_queue
SET status = 'processing', locked_by = pg_backend_pid(), locked_at = NOW()
WHERE transaction_id IN ( ... ids from previous select ... );

COMMIT;

## 2. OLAP Queries (Snowflake)

### 2.1 Customer retention cohort analysis

Business question: What is the month‑over‑month retention rate for each customer cohort?

WITH first_payment AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', MIN(transaction_date)) AS cohort_month
    FROM fact_transactions
    GROUP BY customer_id
),
cohort_activity AS (
    SELECT
        f.cohort_month,
        DATE_TRUNC('month', t.transaction_date) AS activity_month,
        COUNT(DISTINCT t.customer_id) AS active_customers
    FROM fact_transactions t
    JOIN first_payment f ON t.customer_id = f.customer_id
    GROUP BY 1, 2
)
SELECT
    cohort_month,
    activity_month,
    active_customers,
    LAG(active_customers) OVER (PARTITION BY cohort_month ORDER BY activity_month) AS previous_month_active,
    ROUND(100.0 * active_customers / NULLIF(LAG(active_customers) OVER (PARTITION BY cohort_month ORDER BY activity_month), 0), 2) AS retention_rate_pct
FROM cohort_activity
ORDER BY cohort_month, activity_month;

### 2.2 Performance by payment method

Business question: Which payment methods have the highest failure rate?

SELECT
    d_pm.payment_method,
    COUNT(*) AS total_transactions,
    SUM(t.amount_usd) AS total_volume_usd,
    AVG(t.amount_usd) AS avg_ticket_usd,
    SUM(CASE WHEN t.status = 'failed' THEN 1 ELSE 0 END) AS failed_transactions,
    ROUND(100.0 * SUM(CASE WHEN t.status = 'failed' THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_rate_pct
FROM fact_transactions t
JOIN dim_payment_method d_pm ON t.payment_method_id = d_pm.payment_method_id
WHERE t.transaction_date >= DATEADD('month', -6, CURRENT_DATE)
GROUP BY d_pm.payment_method
ORDER BY total_volume_usd DESC;

## 3. NoSQL Query (MongoDB)

### 3.1 Average session duration by merchant

Business question: How long do users stay on the platform per merchant?

MongoDB aggregation (run in mongosh or Compass):

db.user_sessions.aggregate([
    {
        $match: {
            session_start: { $exists: true, $ne: null },
            session_end: { $exists: true, $ne: null }
        }
    },
    {
        $addFields: {
            session_duration_minutes: {
                $divide: [
                    { $subtract: ["$session_end", "$session_start"] },
                    1000 * 60
                ]
            }
        }
    },
    {
        $group: {
            _id: "$merchant_id",
            avg_duration_min: { $avg: "$session_duration_minutes" },
            max_duration_min: { $max: "$session_duration_minutes" },
            min_duration_min: { $min: "$session_duration_minutes" },
            session_count: { $sum: 1 }
        }
    },
    { $sort: { avg_duration_min: -1 } }
])

# Equivalent Python (pymongo) for integration in Airflow:
from pymongo import MongoClient

def get_avg_session_duration():
    client = MongoClient("mongodb://localhost:27017")
    collection = client["stripe_nosql"]["user_sessions"]
    pipeline = [ ... ]  # same as above
    return list(collection.aggregate(pipeline))

    Summary
|System	|Business Question          |Query Type                           |
|-------|---------------------------|-------------------------------------|
|OLTP	|High transaction velocity	|Aggregation with window              |
|OLTP	|Concurrent job processing	|SELECT ... FOR UPDATE SKIP LOCKED    |
|OLAP	|Customer retention	        |Cohort analysis with window functions|
|OLAP	|Payment method performance	|Join and aggregation                 |
N|oSQL	|Session duration	        |MongoDB aggregation pipeline         |