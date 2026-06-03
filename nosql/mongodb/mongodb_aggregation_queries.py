"""
mongodb_aggregation_queries.py
==============================
Python replacement for nosql/mongodb/aggregation_queries.js

5 analytical queries — exact parity with the JS version.

Usage:
    python mongodb_aggregation_queries.py

Dependencies:
    pip install pymongo python-dotenv
"""

import os
import re
from datetime import datetime, timezone, timedelta
from pprint import pprint

from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("MONGO_DB",  "stripe_nosql")

client = MongoClient(MONGO_URI)
db     = client[MONGO_DB]

now = datetime.now(timezone.utc)


# ── Query 1 ───────────────────────────────────────────────────────────────────
# Top 10 at-risk merchants (last hour)
# Based on fraud signals from recent transactions.

def query_top_risky_merchants():
    print("\n── Query 1: Top 10 at-risk merchants (last 1 hour) ──")
    pipeline = [
        {
            "$match": {
                "timestamp":            {"$gte": now - timedelta(hours=1)},
                "fraud_signals.score":  {"$gte": 0.7}
            }
        },
        {
            "$group": {
                "_id":                 "$merchant_id",
                "avg_fraud_score":     {"$avg": "$fraud_signals.score"},
                "flagged_count":       {"$sum": 1},
                "total_amount_avg":    {"$avg": "$ml_features.avg_txn_amount_30d"},
                "geo_anomaly_count":   {"$sum": {"$cond": ["$fraud_signals.geo_anomaly", 1, 0]}},
                "velocity_flag_count": {"$sum": {"$cond": ["$fraud_signals.velocity_flag", 1, 0]}},
            }
        },
        {"$sort": {"avg_fraud_score": -1}},
        {"$limit": 10},
        {
            "$project": {
                "merchant_id":       "$_id",
                "avg_fraud_score":   {"$round": ["$avg_fraud_score", 3]},
                "flagged_count":     1,
                "geo_anomaly_pct": {
                    "$round": [{"$divide": ["$geo_anomaly_count", "$flagged_count"]}, 2]
                },
                "velocity_flag_pct": {
                    "$round": [{"$divide": ["$velocity_flag_count", "$flagged_count"]}, 2]
                },
            }
        },
    ]
    results = list(db["fraud_events"].aggregate(pipeline))
    pprint(results)
    return results


# ── Query 2 ───────────────────────────────────────────────────────────────────
# User sessions that led to a fraudulent transaction
# within the following 5 minutes.

def query_sessions_before_fraud():
    print("\n── Query 2: Sessions followed by fraud within 5 minutes ──")
    pipeline = [
        {
            "$lookup": {
                "from": "fraud_events",
                "let":  {"cid": "$customer_id", "sess_end": "$ended_at"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq":  ["$customer_id", "$$cid"]},
                                    {"$gte": ["$timestamp",   "$$sess_end"]},
                                    {"$lte": ["$timestamp",
                                              {"$add": ["$$sess_end", 300_000]}]},  # 5 min
                                ]
                            },
                            "decision": "flagged"
                        }
                    }
                ],
                "as": "fraud_after_session"
            }
        },
        {"$match": {"fraud_after_session.0": {"$exists": True}}},
        {
            "$project": {
                "session_id":   1,
                "customer_id":  1,
                "duration_sec": 1,
                "device_type":  "$device.type",
                "events_count": {"$size": "$events"},
                "checkout_time_ms": {
                    "$sum": {
                        "$map": {
                            "input": {
                                "$filter": {
                                    "input": "$events",
                                    "cond": {
                                        "$regexMatch": {
                                            "input": "$$this.url",
                                            "regex": "/checkout"
                                        }
                                    }
                                }
                            },
                            "as": "e",
                            "in": "$$e.duration_ms"
                        }
                    }
                },
                "fraud_score": {
                    "$arrayElemAt": ["$fraud_after_session.fraud_signals.score", 0]
                },
            }
        },
    ]
    results = list(db["user_sessions"].aggregate(pipeline))
    pprint(results)
    return results


# ── Query 3 ───────────────────────────────────────────────────────────────────
# Error distribution by service (last 24 hours)

def query_error_distribution():
    print("\n── Query 3: Error distribution by service (last 24h) ──")
    pipeline = [
        {
            "$match": {
                "level":     {"$in": ["ERROR", "CRITICAL"]},
                "timestamp": {"$gte": now - timedelta(hours=24)}
            }
        },
        {
            "$group": {
                "_id":            {"service": "$service", "level": "$level"},
                "count":          {"$sum": 1},
                "sample_messages": {
                    "$addToSet": {"$substr": ["$message", 0, 100]}
                },
            }
        },
        {"$sort": {"count": -1}},
        {
            "$group": {
                "_id":          "$_id.service",
                "total_errors": {"$sum": "$count"},
                "by_level": {
                    "$push": {"level": "$_id.level", "count": "$count"}
                },
                "examples": {"$first": "$sample_messages"},
            }
        },
        {"$sort": {"total_errors": -1}},
    ]
    results = list(db["app_logs"].aggregate(pipeline))
    pprint(results)
    return results


# ── Query 4 ───────────────────────────────────────────────────────────────────
# Average fraud score by model version
# (ML model performance evaluation)

def query_model_performance():
    print("\n── Query 4: Fraud score by model version ──")
    pipeline = [
        {
            "$group": {
                "_id":           "$model_version",
                "avg_score":     {"$avg": "$fraud_signals.score"},
                "count":         {"$sum": 1},
                "flagged_count": {
                    "$sum": {"$cond": [{"$eq": ["$decision", "flagged"]}, 1, 0]}
                },
                "blocked_count": {
                    "$sum": {"$cond": [{"$eq": ["$decision", "blocked"]}, 1, 0]}
                },
            }
        },
        {
            "$project": {
                "model_version": "$_id",
                "avg_score":     {"$round": ["$avg_score", 3]},
                "count":         1,
                "flagged_pct": {
                    "$round": [{"$divide": ["$flagged_count", "$count"]}, 2]
                },
                "blocked_pct": {
                    "$round": [{"$divide": ["$blocked_count", "$count"]}, 2]
                },
            }
        },
        {"$sort": {"count": -1}},
    ]
    results = list(db["fraud_events"].aggregate(pipeline))
    pprint(results)
    return results


# ── Query 5 ───────────────────────────────────────────────────────────────────
# Merchants with more than 100 errors in the last 24 hours
# (IP anomaly detection)

def query_merchant_anomalies():
    print("\n── Query 5: Merchants with > 100 errors in 24h ──")
    pipeline = [
        {
            "$match": {
                "timestamp": {"$gte": now - timedelta(hours=24)},
                "level":     {"$in": ["ERROR", "CRITICAL"]}
            }
        },
        {
            "$group": {
                "_id":               "$context.merchant_id",
                "ip":                {"$first": "$host"},
                "error_count":       {"$sum": 1},
                "distinct_services": {"$addToSet": "$service"},
            }
        },
        {"$match": {"error_count": {"$gt": 100}}},
        {"$sort": {"error_count": -1}},
    ]
    results = list(db["app_logs"].aggregate(pipeline))
    pprint(results)
    return results


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"🔌 Connected to {MONGO_DB} @ {MONGO_URI}")
    query_top_risky_merchants()
    query_sessions_before_fraud()
    query_error_distribution()
    query_model_performance()
    query_merchant_anomalies()
    print("\n✅ All queries executed.\n")
