from pymongo import MongoClient
from datetime import datetime

client = MongoClient("mongodb://localhost:27017")

db = client["stripe_demo"]

# --------------------------------------------------
# fraud_events
# --------------------------------------------------

db.fraud_events.insert_many([
    {
        "transaction_id": "tx_100008",
        "merchant_id": "m_006",
        "customer_id": "c_008",
        "timestamp": datetime.fromisoformat("2026-01-01T10:12:00"),
        "fraud_signals": {
            "score": 0.83,
            "model_version": "xgb-v2.3",
            "features": {
                "velocity_1h": 14,
                "amount_zscore": 3.1,
                "ip_risk": 0.84,
                "device_fingerprint_match": False
            }
        },
        "decision": "block",
        "reviewed_by": "auto",
        "ttl_expires_at": datetime.fromisoformat("2026-04-01T00:00:00")
    }
])

# --------------------------------------------------
# user_sessions
# --------------------------------------------------

db.user_sessions.insert_one({
    "session_id": "s_001",
    "customer_id": "c_001",
    "started_at": datetime.fromisoformat("2026-01-01T09:10:00"),
    "device": {
        "type": "mobile",
        "os": "iOS",
        "browser": "Safari"
    },
    "events": [
        {
            "type": "page_view",
            "path": "/checkout"
        }
    ],
    "converted": True,
    "funnel_stage": "payment_success"
})

# --------------------------------------------------
# app_logs
# --------------------------------------------------

db.app_logs.insert_one({
    "level": "ERROR",
    "service": "payment-processor",
    "message": "Timeout connecting to issuer bank",
    "context": {
        "merchant_id": "m_003",
        "latency_ms": 5120
    },
    "created_at": datetime.utcnow()
})

# --------------------------------------------------
# Indexes
# --------------------------------------------------

db.fraud_events.create_index(
    [("merchant_id", 1), ("timestamp", -1)]
)

db.fraud_events.create_index(
    [("fraud_signals.score", 1)]
)

db.fraud_events.create_index(
    [("ttl_expires_at", 1)],
    expireAfterSeconds=0
)

db.user_sessions.create_index(
    [("customer_id", 1), ("started_at", -1)]
)

db.app_logs.create_index(
    [("created_at", 1)],
    expireAfterSeconds=2592000
)

print("MongoDB demo dataset successfully loaded.")