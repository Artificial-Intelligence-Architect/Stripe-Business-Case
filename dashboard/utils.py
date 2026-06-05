import pandas as pd
import psycopg2
from pymongo import MongoClient
import os

def get_postgres_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB", "stripe"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "your_password_here")
    )

def get_mongo_client():
    return MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"))

def fetch_transactions(limit=100):
    conn = get_postgres_connection()
    query = f"""
        SELECT transaction_id, customer_id, merchant_id, amount_usd, created_at, status
        FROM transactions
        ORDER BY created_at DESC
        LIMIT {limit}
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def fetch_fraud_events(limit=100):
    client = get_mongo_client()
    db = client[os.getenv("MONGO_DB", "stripe_nosql")]
    cursor = db.fraud_events.find().sort("timestamp", -1).limit(limit)
    df = pd.DataFrame(list(cursor))
    client.close()
    return df
