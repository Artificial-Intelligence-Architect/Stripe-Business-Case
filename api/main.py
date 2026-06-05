from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import psycopg2
from fraud_scorer import predict_score
import os

app = FastAPI(title="Stripe Fraud Detection API", version="1.0")

class TransactionFeatures(BaseModel):
    amount_usd: float
    txn_count_24h: int
    avg_txn_amount_30d: float
    amount_ratio_30d: float
    distinct_countries_30d: int
    geo_mismatch: int
    device_fingerprint_match: int
    merchant_fraud_rate_7d: float

class PredictionResponse(BaseModel):
    fraud_score: float
    decision: str  # "fraud" if score > threshold, otherwise "legit"

# Prediction endpoint
@app.post("/predict", response_model=PredictionResponse)
async def predict(features: TransactionFeatures):
    """Calculate fraud score and return decision based on threshold."""
    score = predict_score(features.dict())
    threshold = float(os.getenv("FRAUD_THRESHOLD", "0.5"))
    decision = "fraud" if score > threshold else "legit"
    return PredictionResponse(fraud_score=score, decision=decision)

# Endpoint to fetch recent transactions
@app.get("/transactions")
async def get_transactions(limit: int = 100):
    """Retrieve the most recent transactions from PostgreSQL."""
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        database=os.getenv("POSTGRES_DB", "stripe"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "your_password_here")
    )
    cur = conn.cursor()
    cur.execute(f"""
        SELECT transaction_id, customer_id, amount_usd, created_at, status
        FROM transactions
        ORDER BY created_at DESC
        LIMIT {limit}
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [{"transaction_id": r[0], "customer_id": r[1], "amount_usd": r[2], "created_at": str(r[3]), "status": r[4]} for r in rows]