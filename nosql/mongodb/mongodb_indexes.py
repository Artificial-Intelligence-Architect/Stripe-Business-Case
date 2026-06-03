"""
mongodb_indexes.py
==================
Python replacement for nosql/mongodb/index.js

Creates all secondary indexes on the 3 collections.
Safe to run multiple times (create_index is idempotent).

Usage:
    python mongodb_indexes.py

Dependencies:
    pip install pymongo python-dotenv
"""

import os
from pymongo import MongoClient, ASCENDING, DESCENDING

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("MONGO_DB",  "stripe_nosql")

client = MongoClient(MONGO_URI)
db     = client[MONGO_DB]


# ── fraud_events ─────────────────────────────────────────────────────────────

# 1. Composite index on merchant_id + timestamp
#    Used for merchant monitoring queries
db["fraud_events"].create_index(
    [("merchant_id", ASCENDING), ("timestamp", DESCENDING)],
    name="idx_merchant_time"
)

# 2. Partial index on high fraud scores (>= 0.7)
#    Optimises real-time detection without indexing the full field
db["fraud_events"].create_index(
    [("fraud_signals.score", ASCENDING)],
    name="idx_fraud_score_high",
    partialFilterExpression={"fraud_signals.score": {"$gte": 0.7}}
)

# 3. Partial index for pending decisions (flagged, not yet reviewed)
#    Speeds up the manual review queue
db["fraud_events"].create_index(
    [("decision", ASCENDING), ("timestamp", DESCENDING)],
    name="idx_decision_pending",
    partialFilterExpression={"decision": "flagged", "reviewed_by": None}
)

print("✅ fraud_events indexes created")

# ── user_sessions ─────────────────────────────────────────────────────────────

# 4. TTL index — automatic expiry after 90 days
db["user_sessions"].create_index(
    [("started_at", ASCENDING)],
    expireAfterSeconds=7_776_000,
    name="idx_ttl_sessions"
)

# 5. Simple index on customer_id
db["user_sessions"].create_index(
    [("customer_id", ASCENDING)],
    name="idx_customer_id"
)

# 6. Compound index customer_id + ended_at
#    Required for post-session fraud $lookup (aggregation_queries.py query #2)
db["user_sessions"].create_index(
    [("customer_id", ASCENDING), ("ended_at", DESCENDING)],
    name="idx_customer_ended_at"
)

# 7. Device fingerprint index
#    Used for device_fingerprint_match feature (ml/feature_engineering.py)
db["user_sessions"].create_index(
    [("device.fingerprint", ASCENDING)],
    name="idx_device_fingerprint"
)

print("✅ user_sessions indexes created")

# ── app_logs ──────────────────────────────────────────────────────────────────

# 8. TTL index for logs — 30 days
db["app_logs"].create_index(
    [("timestamp", ASCENDING)],
    expireAfterSeconds=2_592_000,
    name="idx_ttl_logs"
)

# 9. Compound index for filtering by service / level / time
db["app_logs"].create_index(
    [("service", ASCENDING), ("level", ASCENDING), ("timestamp", DESCENDING)],
    name="idx_service_level_time"
)

# 10. Index for merchant error monitoring
db["app_logs"].create_index(
    [("context.merchant_id", ASCENDING), ("timestamp", DESCENDING)],
    name="idx_merchant_errors"
)

print("✅ app_logs indexes created")
print("\n✅ All indexes ready.\n")
