db = db.getSiblingDB("stripe_demo");

/*
----------------------------------
fraud_events
----------------------------------
*/

db.fraud_events.insertMany([
{
    transaction_id: "tx_100008",
    merchant_id: "m_006",
    customer_id: "c_008",
    timestamp: ISODate("2026-01-01T10:12:00Z"),
    fraud_signals: {
        score: 0.83,
        model_version: "xgb-v2.3",
        features: {
            velocity_1h: 14,
            amount_zscore: 3.1,
            ip_risk: 0.84,
            device_fingerprint_match: false
        }
    },
    decision: "block",
    reviewed_by: "auto",
    ttl_expires_at: ISODate("2026-04-01T00:00:00Z")
},
{
    transaction_id: "tx_100010",
    merchant_id: "m_003",
    customer_id: "c_009",
    timestamp: ISODate("2026-01-01T10:25:00Z"),
    fraud_signals: {
        score: 0.94,
        model_version: "xgb-v2.3",
        features: {
            velocity_1h: 18,
            amount_zscore: 4.5,
            ip_risk: 0.95,
            device_fingerprint_match: false
        }
    },
    decision: "block",
    reviewed_by: "auto",
    ttl_expires_at: ISODate("2026-04-01T00:00:00Z")
}
]);

/*
----------------------------------
user_sessions
----------------------------------
*/

db.user_sessions.insertOne({
    session_id: "s_001",
    customer_id: "c_001",
    started_at: ISODate("2026-01-01T09:10:00Z"),
    device: {
        type: "mobile",
        os: "iOS",
        browser: "Safari"
    },
    events: [
        {
            type: "page_view",
            path: "/checkout",
            ts: ISODate("2026-01-01T09:11:00Z")
        },
        {
            type: "payment_attempt",
            amount: 149.99,
            ts: ISODate("2026-01-01T09:15:00Z")
        }
    ],
    converted: true,
    funnel_stage: "payment_success"
});

/*
----------------------------------
app_logs
----------------------------------
*/

db.app_logs.insertOne({
    level: "ERROR",
    service: "payment-processor",
    message: "Timeout connecting to issuer bank",
    context: {
        merchant_id: "m_003",
        latency_ms: 5120
    },
    created_at: ISODate("2026-01-01T10:20:00Z")
});

/*
----------------------------------
Indexes
----------------------------------
*/

db.fraud_events.createIndex(
    { merchant_id: 1, timestamp: -1 }
);

db.fraud_events.createIndex(
    { "fraud_signals.score": 1 }
);

db.fraud_events.createIndex(
    { ttl_expires_at: 1 },
    { expireAfterSeconds: 0 }
);

db.user_sessions.createIndex(
    { customer_id: 1, started_at: -1 }
);

db.app_logs.createIndex(
    { created_at: 1 },
    { expireAfterSeconds: 2592000 }
);

print("Stripe demo MongoDB initialised successfully.");