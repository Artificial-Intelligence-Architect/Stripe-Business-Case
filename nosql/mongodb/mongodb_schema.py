"""
mongodb_schema.py
=================
Python replacement for nosql/mongodb/schema.js

Creates the 3 MongoDB collections with $jsonSchema validators
and all TTL/compound indexes.

Usage:
    python mongodb_schema.py

Dependencies:
    pip install pymongo python-dotenv

Environment variables:
    MONGO_URI    MongoDB connection string   default: mongodb://localhost:27017
    MONGO_DB     Database name               default: stripe_nosql
"""

import os
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import CollectionInvalid

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("MONGO_DB",  "stripe_nosql")

client = MongoClient(MONGO_URI)
db     = client[MONGO_DB]


def create_fraud_events():
    """
    One document per transaction evaluated by the fraud engine.
    TTL index = 90 days (GDPR automatic purge).
    """
    validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": [
                "transaction_id", "customer_id", "merchant_id",
                "timestamp", "fraud_signals", "decision", "model_version"
            ],
            "properties": {
                "transaction_id": {"bsonType": "string"},
                "customer_id":    {"bsonType": "string"},
                "merchant_id":    {"bsonType": "string"},
                "timestamp":      {"bsonType": "date"},
                "decision": {
                    "bsonType": "string",
                    "enum": ["approved", "flagged", "blocked"]
                },
                "model_version": {"bsonType": "string"},
                "fraud_signals": {
                    "bsonType": "object",
                    "required": ["score"],
                    "properties": {
                        "score":         {"bsonType": "double", "minimum": 0, "maximum": 1},
                        "velocity_flag": {"bsonType": "bool"},
                        "geo_anomaly":   {"bsonType": "bool"},
                        "device_risk":   {"bsonType": "double"},
                    }
                },
                "ml_features": {
                    "bsonType": "object",
                    "properties": {
                        "txn_count_24h":            {"bsonType": "int"},
                        "avg_txn_amount_30d":       {"bsonType": "double"},
                        "amount_ratio_30d":         {"bsonType": "double"},
                        "distinct_countries_30d":   {"bsonType": "int"},
                        "geo_mismatch":             {"bsonType": "int"},
                        "device_fingerprint_match": {"bsonType": "int"},
                        "merchant_fraud_rate_7d":   {"bsonType": "double"},
                    }
                }
            }
        }
    }

    try:
        db.create_collection("fraud_events", validator=validator)
        print("✅ Collection fraud_events created")
    except CollectionInvalid:
        db.command("collMod", "fraud_events", validator=validator)
        print("⚠️  Collection fraud_events already exists — validator updated")

    col = db["fraud_events"]
    col.create_index([("timestamp", ASCENDING)],
                     expireAfterSeconds=7_776_000, name="ttl_90d")      # GDPR purge
    col.create_index([("transaction_id", ASCENDING)], unique=True)
    col.create_index([("merchant_id", ASCENDING), ("timestamp", DESCENDING)])
    col.create_index([("customer_id", ASCENDING), ("timestamp", DESCENDING)])
    col.create_index([("fraud_signals.score", DESCENDING)])
    col.create_index([("model_version", ASCENDING)])
    print("✅ Indexes created on fraud_events")


def create_user_sessions():
    """
    One document per browser/app session.
    ended_at is required — used by aggregation_queries.py query #2.
    TTL index = 180 days.
    """
    validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": [
                "session_id", "customer_id", "started_at",
                "ended_at", "device", "events"
            ],
            "properties": {
                "session_id":  {"bsonType": "string"},
                "customer_id": {"bsonType": "string"},
                "started_at":  {"bsonType": "date"},
                "ended_at": {
                    "bsonType": "date",
                    "description": "Session end — required for post-session fraud $lookup"
                },
                "duration_sec": {"bsonType": "int"},
                "device": {
                    "bsonType": "object",
                    "required": ["type", "fingerprint"],
                    "properties": {
                        "type":        {"bsonType": "string"},  # mobile|desktop|tablet
                        "fingerprint": {"bsonType": "string"},  # hashed device ID
                        "os":          {"bsonType": "string"},
                        "browser":     {"bsonType": "string"},
                    }
                },
                "events": {
                    "bsonType": "array",
                    "items": {
                        "bsonType": "object",
                        "required": ["type", "url", "ts"],
                        "properties": {
                            "type":        {"bsonType": "string"},
                            "url":         {"bsonType": "string"},
                            "ts":          {"bsonType": "date"},
                            "duration_ms": {"bsonType": "int"},
                        }
                    }
                },
                "ip_address_hash": {"bsonType": "string"},  # hashed — GDPR compliant
                "ip_country":      {"bsonType": "string"},
            }
        }
    }

    try:
        db.create_collection("user_sessions", validator=validator)
        print("✅ Collection user_sessions created")
    except CollectionInvalid:
        db.command("collMod", "user_sessions", validator=validator)
        print("⚠️  Collection user_sessions already exists — validator updated")

    col = db["user_sessions"]
    col.create_index([("session_id", ASCENDING)], unique=True)
    col.create_index(                                                    # $lookup key
        [("customer_id", ASCENDING), ("ended_at", DESCENDING)],
        name="idx_customer_ended_at"
    )
    col.create_index([("started_at", ASCENDING)],
                     expireAfterSeconds=15_552_000, name="ttl_180d")    # 6-month retention
    col.create_index([("device.fingerprint", ASCENDING)])
    print("✅ Indexes created on user_sessions")


def create_app_logs():
    """
    Structured application logs — errors, warnings, audit events.
    TTL index = 30 days.
    """
    validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["log_id", "service", "level", "message", "timestamp"],
            "properties": {
                "log_id":    {"bsonType": "string"},
                "service":   {"bsonType": "string"},
                "level": {
                    "bsonType": "string",
                    "enum": ["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"]
                },
                "message":   {"bsonType": "string"},
                "timestamp": {"bsonType": "date"},
                "host":      {"bsonType": "string"},
                "context": {
                    "bsonType": "object",
                    "properties": {
                        "transaction_id": {"bsonType": "string"},
                        "merchant_id":    {"bsonType": "string"},
                        "customer_id":    {"bsonType": "string"},
                        "trace_id":       {"bsonType": "string"},
                    }
                }
            }
        }
    }

    try:
        db.create_collection("app_logs", validator=validator)
        print("✅ Collection app_logs created")
    except CollectionInvalid:
        db.command("collMod", "app_logs", validator=validator)
        print("⚠️  Collection app_logs already exists — validator updated")

    col = db["app_logs"]
    col.create_index([("timestamp", ASCENDING)],
                     expireAfterSeconds=2_592_000, name="ttl_30d")      # 30-day retention
    col.create_index(
        [("service", ASCENDING), ("level", ASCENDING), ("timestamp", DESCENDING)],
        name="idx_service_level_time"
    )
    col.create_index(
        [("context.merchant_id", ASCENDING), ("timestamp", DESCENDING)]
    )
    print("✅ Indexes created on app_logs")


if __name__ == "__main__":
    print(f"\n🔌 Connected to {MONGO_DB} @ {MONGO_URI}\n")
    create_fraud_events()
    create_user_sessions()
    create_app_logs()
    print("\n✅ All collections and indexes ready.\n")
