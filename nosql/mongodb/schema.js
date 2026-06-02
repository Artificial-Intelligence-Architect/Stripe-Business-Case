// ============================================================
// NoSQL/MongoDB: Collection Schemas (Stripe Business Case)
// ============================================================
// Run with: mongosh stripe_nosql --file schema.js
// ============================================================

// ── 1. fraud_events ──────────────────────────────────────────
// One document per transaction evaluated by the fraud engine.
// TTL index = 90 days (GDPR automatic purge).
// ------------------------------------------------------------
db.createCollection("fraud_events", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["transaction_id", "customer_id", "merchant_id",
                 "timestamp", "fraud_signals", "decision", "model_version"],
      properties: {
        transaction_id: { bsonType: "string"  },
        customer_id:    { bsonType: "string"  },
        merchant_id:    { bsonType: "string"  },
        timestamp:      { bsonType: "date"    },
        decision: {
          bsonType: "string",
          enum: ["approved", "flagged", "blocked"]
        },
        model_version:  { bsonType: "string"  },
        fraud_signals: {
          bsonType: "object",
          required: ["score"],
          properties: {
            score:         { bsonType: "double", minimum: 0, maximum: 1 },
            velocity_flag: { bsonType: "bool"   },
            geo_anomaly:   { bsonType: "bool"   },
            device_risk:   { bsonType: "double" }
          }
        },
        ml_features: {
          bsonType: "object",
          properties: {
            txn_count_24h:           { bsonType: "int"    },
            avg_txn_amount_30d:      { bsonType: "double" },
            amount_ratio_30d:        { bsonType: "double" },
            distinct_countries_30d:  { bsonType: "int"    },
            geo_mismatch:            { bsonType: "int"    },
            device_fingerprint_match:{ bsonType: "int"    },
            merchant_fraud_rate_7d:  { bsonType: "double" }
          }
        }
      }
    }
  }
});

db.fraud_events.createIndex({ timestamp: 1 },
  { expireAfterSeconds: 7776000, name: "ttl_90d" });       // GDPR purge
db.fraud_events.createIndex({ transaction_id: 1 }, { unique: true });
db.fraud_events.createIndex({ merchant_id: 1, timestamp: -1 });
db.fraud_events.createIndex({ customer_id: 1, timestamp: -1 });
db.fraud_events.createIndex({ "fraud_signals.score": -1 });
db.fraud_events.createIndex({ model_version: 1 });


// ── 2. user_sessions ─────────────────────────────────────────
// One document per browser/app session.
// Key fix: ended_at is now explicitly defined — required by
// aggregation_queries.js query #2 ($lookup on ended_at).
// ------------------------------------------------------------
db.createCollection("user_sessions", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["session_id", "customer_id", "started_at",
                 "ended_at", "device", "events"],
      properties: {
        session_id:   { bsonType: "string" },
        customer_id:  { bsonType: "string" },
        started_at:   { bsonType: "date"   },
        ended_at: {                                   // ← WAS MISSING
          bsonType: "date",
          description: "Session end timestamp — required for post-session fraud correlation"
        },
        duration_sec: { bsonType: "int"    },
        device: {
          bsonType: "object",
          required: ["type", "fingerprint"],
          properties: {
            type:         { bsonType: "string" },    // mobile | desktop | tablet
            fingerprint:  { bsonType: "string" },    // hashed device ID
            os:           { bsonType: "string" },
            browser:      { bsonType: "string" }
          }
        },
        events: {
          bsonType: "array",
          items: {
            bsonType: "object",
            required: ["type", "url", "ts"],
            properties: {
              type:        { bsonType: "string" },   // click | pageview | form_submit
              url:         { bsonType: "string" },
              ts:          { bsonType: "date"   },
              duration_ms: { bsonType: "int"    }
            }
          }
        },
        ip_address_hash: { bsonType: "string" },     // hashed — GDPR compliant
        ip_country:      { bsonType: "string" }
      }
    }
  }
});

db.user_sessions.createIndex({ session_id: 1  }, { unique: true });
db.user_sessions.createIndex({ customer_id: 1, ended_at: -1 });   // $lookup key
db.user_sessions.createIndex({ started_at:  1 },
  { expireAfterSeconds: 15552000, name: "ttl_180d" });             // 6-month retention
db.user_sessions.createIndex({ "device.fingerprint": 1 });


// ── 3. app_logs ──────────────────────────────────────────────
// Structured application logs — errors, warnings, audit events.
// ------------------------------------------------------------
db.createCollection("app_logs", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["log_id", "service", "level", "message", "timestamp"],
      properties: {
        log_id:    { bsonType: "string" },
        service:   { bsonType: "string" },
        level: {
          bsonType: "string",
          enum: ["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"]
        },
        message:   { bsonType: "string" },
        timestamp: { bsonType: "date"   },
        host:      { bsonType: "string" },
        context: {
          bsonType: "object",
          properties: {
            transaction_id: { bsonType: "string" },
            merchant_id:    { bsonType: "string" },
            customer_id:    { bsonType: "string" },
            trace_id:       { bsonType: "string" }
          }
        }
      }
    }
  }
});

db.app_logs.createIndex({ timestamp: 1 },
  { expireAfterSeconds: 2592000, name: "ttl_30d" });               // 30-day retention
db.app_logs.createIndex({ service: 1, level: 1, timestamp: -1 }); // query #3 & #5
db.app_logs.createIndex({ "context.merchant_id": 1, timestamp: -1 });
