"""
mongodb_schema.py
=================
Python replacement for nosql/mongodb/schema.js

Creates the 5 MongoDB collections with $jsonSchema validators
and all TTL/compound indexes, plus the GridFS bucket for binary/XML payloads.

Collections:
    fraud_events      fraud scores, signals, decisions, model metadata
    user_sessions     clickstream / checkout journey
    app_logs          operational logs
    customer_feedback reviews + survey responses   (brief: Data Sources / NoSQL)
    dispute_documents GridFS metadata: XML + binary (brief: 'JSON, XML, binary')

Usage:
    python mongodb_schema.py

Dependencies:
    pip install pymongo python-dotenv

Environment variables:
    MONGO_URI    MongoDB connection string   default: mongodb://localhost:27017
    MONGO_DB     Database name               default: stripe_nosql
"""

import os
from pymongo import MongoClient, ASCENDING, DESCENDING, TEXT
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




def create_customer_feedback():
    """
    Reviews and survey responses.

    REQUIRED BY THE BRIEF — "Customer Feedback (reviews, survey responses)" is
    listed as a NoSQL data source and was missing from v1.

    Design notes:
      - `rating` and free text live together: feedback is always read as a unit.
      - `nps_score` 0-10 is kept separate from `rating` 1-5; conflating them is
        the classic survey-modelling error (different scales, different questions).
      - Survey answers are an ARRAY of {question_id, answer}, not one field per
        question: surveys change every quarter and a fixed schema would force a
        migration each time. This is precisely why this data belongs in MongoDB
        and not in the OLTP star schema.
      - `sentiment` is written back by the NLP model — the document is both the
        raw record and the ML feature store entry.
    """
    validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["feedback_id", "merchant_id", "source", "submitted_at"],
            "properties": {
                "feedback_id":  {"bsonType": "string"},
                "customer_id":  {"bsonType": ["string", "null"],
                                 "description": "null for anonymous surveys"},
                "merchant_id":  {"bsonType": "string"},
                "transaction_id": {"bsonType": ["string", "null"],
                                   "description": "reference to OLTP — enables "
                                                  "'did refunded customers rate lower?'"},
                "source": {
                    "bsonType": "string",
                    "enum": ["review", "survey", "support_ticket", "app_store", "nps_email"]
                },
                "submitted_at": {"bsonType": "date"},
                "locale":       {"bsonType": "string"},
                "rating":       {"bsonType": ["int", "null"], "minimum": 1, "maximum": 5},
                "nps_score":    {"bsonType": ["int", "null"], "minimum": 0, "maximum": 10},
                "title":        {"bsonType": ["string", "null"]},
                "body":         {"bsonType": ["string", "null"]},
                "survey_answers": {
                    "bsonType": "array",
                    "items": {
                        "bsonType": "object",
                        "required": ["question_id", "answer"],
                        "properties": {
                            "question_id":   {"bsonType": "string"},
                            "question_text": {"bsonType": "string"},
                            "answer":        {"bsonType": ["string", "int", "bool", "array"]},
                        }
                    }
                },
                # Written back by the NLP pipeline — ML features live with the data.
                "sentiment": {
                    "bsonType": "object",
                    "properties": {
                        "label":         {"bsonType": "string",
                                          "enum": ["positive", "neutral", "negative"]},
                        "score":         {"bsonType": "double", "minimum": -1, "maximum": 1},
                        "model_version": {"bsonType": "string"},
                        "scored_at":     {"bsonType": "date"},
                        "topics":        {"bsonType": "array",
                                          "items": {"bsonType": "string"}},
                    }
                },
                "is_erased": {"bsonType": "bool",
                              "description": "GDPR art.17 tombstone — see docs/13"},
            }
        }
    }

    try:
        db.create_collection("customer_feedback", validator=validator)
        print("✅ Collection customer_feedback created")
    except CollectionInvalid:
        db.command("collMod", "customer_feedback", validator=validator)
        print("⚠️  Collection customer_feedback already exists — validator updated")

    col = db["customer_feedback"]
    col.create_index([("feedback_id", ASCENDING)], unique=True)
    col.create_index([("merchant_id", ASCENDING), ("submitted_at", DESCENDING)])
    col.create_index([("customer_id", ASCENDING), ("submitted_at", DESCENDING)],
                     sparse=True)
    col.create_index([("transaction_id", ASCENDING)], sparse=True)
    col.create_index([("sentiment.label", ASCENDING), ("submitted_at", DESCENDING)])
    col.create_index([("nps_score", ASCENDING)], sparse=True)
    # Full-text search over free text — the brief asks for "efficient querying
    # of nested and unstructured data"; a compound index cannot search prose.
    col.create_index([("title", TEXT), ("body", TEXT)],
                     name="idx_feedback_fulltext",
                     default_language="english",
                     weights={"title": 3, "body": 1})
    # NO TTL: feedback is business memory, not telemetry. Deleting it after
    # 90 days would destroy the NPS trend AND the sentiment model's training set.
    print("✅ Indexes created on customer_feedback (incl. full-text)")


def create_dispute_documents():
    """
    GridFS bucket + metadata for BINARY and XML payloads.

    REQUIRED BY THE BRIEF — "handle diverse data types, including JSON, XML,
    and binary data". v1 handled JSON only.

    Real Stripe use case: a chargeback dispute carries evidence — the merchant's
    PDF invoice, a signed delivery slip (image), and the card network's
    representment file, which arrives as XML (ISO 20022 / card-scheme format).

    Why GridFS and not a plain binary field:
      BSON documents are capped at 16 MB. A scanned evidence bundle blows past
      that. GridFS chunks the file (255 KB default) across `fs.chunks` and keeps
      searchable metadata in `fs.files`. Files under ~1 MB could sit inline as
      BinData — GridFS is the right call here because dispute evidence is
      routinely 5-50 MB.

    Why the XML is stored raw AND parsed:
      The raw bytes are the legal evidence (must be byte-identical for the
      network's audit). The parsed projection is what we query. Storing only
      the parsed form would lose the evidentiary value; storing only the raw
      form would make it unqueryable.
    """
    validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["dispute_id", "merchant_id", "transaction_id",
                         "doc_type", "uploaded_at"],
            "properties": {
                "dispute_id":     {"bsonType": "string"},
                "merchant_id":    {"bsonType": "string"},
                "transaction_id": {"bsonType": "string",
                                   "description": "FK to OLTP transactions"},
                "doc_type": {
                    "bsonType": "string",
                    "enum": ["invoice_pdf", "delivery_proof_image",
                             "representment_xml", "correspondence_eml",
                             "receipt_scan"]
                },
                "mime_type":  {"bsonType": "string"},
                "uploaded_at": {"bsonType": "date"},
                "size_bytes": {"bsonType": ["int", "long"]},
                "sha256":     {"bsonType": "string",
                               "description": "integrity + de-duplication"},
                # Pointer into GridFS (fs.files._id) for payloads > 16 MB
                "gridfs_id":  {"bsonType": ["objectId", "null"]},
                # Small payloads (< 1 MB) inline as BinData — avoids a GridFS round-trip
                "inline_data": {"bsonType": ["binData", "null"]},
                # Raw XML preserved verbatim for legal evidence
                "raw_xml":    {"bsonType": ["string", "null"]},
                # Queryable projection of the same XML
                "parsed_xml": {
                    "bsonType": ["object", "null"],
                    "properties": {
                        "scheme":          {"bsonType": "string"},
                        "reason_code":     {"bsonType": "string"},
                        "disputed_amount": {"bsonType": "double"},
                        "currency":        {"bsonType": "string"},
                        "deadline":        {"bsonType": "date"},
                    }
                },
            }
        }
    }

    try:
        db.create_collection("dispute_documents", validator=validator)
        print("✅ Collection dispute_documents created")
    except CollectionInvalid:
        db.command("collMod", "dispute_documents", validator=validator)
        print("⚠️  Collection dispute_documents already exists — validator updated")

    col = db["dispute_documents"]
    col.create_index([("dispute_id", ASCENDING), ("doc_type", ASCENDING)])
    col.create_index([("transaction_id", ASCENDING)])
    col.create_index([("merchant_id", ASCENDING), ("uploaded_at", DESCENDING)])
    col.create_index([("sha256", ASCENDING)], unique=True, sparse=True)
    col.create_index([("parsed_xml.reason_code", ASCENDING)], sparse=True)
    col.create_index([("parsed_xml.deadline", ASCENDING)], sparse=True)
    print("✅ Indexes created on dispute_documents")

    # GridFS bucket for the >16 MB payloads
    from gridfs import GridFSBucket
    GridFSBucket(db, bucket_name="dispute_evidence")
    db["dispute_evidence.files"].create_index(
        [("metadata.dispute_id", ASCENDING), ("uploadDate", DESCENDING)]
    )
    print("✅ GridFS bucket 'dispute_evidence' ready (binary payloads > 16 MB)")


def create_recommendations():
    """
    Precomputed per-customer recommendations (personalisation use case).

    REQUIRED BY THE BRIEF — "customer personalisation" and "real-time
    recommendations" are named ML use cases (docs/07). This is the serving-side
    collection the recommender writes to and the checkout reads from.

    Design notes:
      - One document per (merchant_id, customer_id): the full candidate list is
        read together at checkout, so it is embedded, not referenced.
      - `model_version` + `generated_at` make staleness observable and let the
        monitoring layer measure how old served recommendations are.
      - The recommender writes here in BATCH; real-time session intent re-ranks
        this cached list at request time ("precompute heavy, decide light").
    """
    validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["merchant_id", "customer_id", "items",
                         "model_version", "generated_at"],
            "properties": {
                "merchant_id":   {"bsonType": "string"},
                "customer_id":   {"bsonType": "string"},
                "strategy": {
                    "bsonType": "string",
                    "enum": ["collaborative", "content_based", "hybrid",
                             "popularity_fallback"],
                    "description": "popularity_fallback = cold-start path"
                },
                "items": {
                    "bsonType": "array",
                    "description": "ranked candidate list",
                    "items": {
                        "bsonType": "object",
                        "required": ["product_id", "score"],
                        "properties": {
                            "product_id": {"bsonType": "string"},
                            "score":      {"bsonType": "double",
                                           "minimum": 0, "maximum": 1},
                            "reason":     {"bsonType": "string"},
                        }
                    }
                },
                "next_best_action": {"bsonType": ["string", "null"],
                    "description": "upgrade prompt / retention offer / none"},
                "churn_risk":    {"bsonType": ["double", "null"],
                                  "minimum": 0, "maximum": 1},
                "model_version": {"bsonType": "string"},
                "generated_at":  {"bsonType": "date"},
                "is_erased":     {"bsonType": "bool",
                                  "description": "GDPR art.17 tombstone"},
            }
        }
    }

    try:
        db.create_collection("recommendations", validator=validator)
        print("✅ Collection recommendations created")
    except CollectionInvalid:
        db.command("collMod", "recommendations", validator=validator)
        print("⚠️  Collection recommendations already exists — validator updated")

    col = db["recommendations"]
    col.create_index([("merchant_id", ASCENDING), ("customer_id", ASCENDING)],
                     unique=True)                    # one cached list per customer
    col.create_index([("churn_risk", DESCENDING)], sparse=True)  # retention targeting
    # TTL 30 days on generated_at: a recommendation older than a month is stale;
    # the batch job refreshes active customers well within that window. This one
    # index also serves staleness-monitoring queries on generated_at, so we do
    # NOT create a second plain index on the same field (MongoDB would reject a
    # duplicate key pattern).
    col.create_index([("generated_at", ASCENDING)],
                     expireAfterSeconds=2_592_000, name="ttl_reco_30d")
    print("✅ Indexes created on recommendations (incl. 30d TTL)")


if __name__ == "__main__":
    print(f"\n🔌 Connected to {MONGO_DB} @ {MONGO_URI}\n")
    create_fraud_events()
    create_user_sessions()
    create_app_logs()
    create_customer_feedback()
    create_dispute_documents()
    create_recommendations()
    print("\n✅ All 6 collections, indexes and the GridFS bucket are ready.\n")
