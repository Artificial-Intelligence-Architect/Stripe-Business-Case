// ============================================================
// NoSQL/MongoDB: Index Creation
// ============================================================

// Connect to the target database (to be adapted)
// use stripe_nosql;

// ---------- COLLECTION fraud_events ----------

// 1. Composite index on merchant_id + timestamp
// Used for merchant monitoring queries.
db.fraud_events.createIndex(
  { merchant_id: 1, timestamp: -1 },
  { name: "idx_merchant_time" }
);

// 2. Partial index on high fraud scores
// Optimises real-time detection without indexing the entire field.
db.fraud_events.createIndex(
  { "fraud_signals.score": 1 },
  {
    name: "idx_fraud_score_high",
    partialFilterExpression: { "fraud_signals.score": { $gte: 0.7 } }
  }
);

// 3. Index for pending decisions (flagged, not yet reviewed)
// Speeds up the manual review queue.
db.fraud_events.createIndex(
  { decision: 1, timestamp: -1 },
  {
    name: "idx_decision_pending",
    partialFilterExpression: { decision: "flagged", reviewed_by: null }
  }
);

// ---------- COLLECTION user_sessions ----------

// 4. TTL index (automatic expiry after 90 days)
// started_at + expireAfterSeconds in seconds: 90 days = 7776000
db.user_sessions.createIndex(
  { started_at: 1 },
  { expireAfterSeconds: 7776000, name: "idx_ttl_sessions" }
);

// 5. Index on customer_id (for lookups or searches)
// (not yet created in the base schema, but useful for joins)
db.user_sessions.createIndex(
  { customer_id: 1 },
  { name: "idx_customer_id" }
);

// ---------- COLLECTION app_logs ----------

// 6. TTL index for logs (30 days)
db.app_logs.createIndex(
  { timestamp: 1 },
  { expireAfterSeconds: 2592000, name: "idx_ttl_logs" }
);

// 7. Compound index for filtering by service/level/time
db.app_logs.createIndex(
  { service: 1, level: 1, timestamp: -1 },
  { name: "idx_service_level_time" }
);
// Compound index customer_id + ended_at
// Required for post-session fraud $lookup (aggregation_queries.js query #2)
db.user_sessions.createIndex(
  { customer_id: 1, ended_at: -1 },
  { name: "idx_customer_ended_at" }
);
