# api/models.py
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

class TransactionFeatures(BaseModel):
    """Input features for fraud prediction."""
    amount_usd: float = Field(..., gt=0, description="Transaction amount in USD")
    txn_count_24h: int = Field(..., ge=0, description="Number of customer transactions in the last 24 hours")
    avg_txn_amount_30d: float = Field(..., ge=0, description="Average customer transaction amount over 30 days")
    amount_ratio_30d: float = Field(..., ge=0, description="Ratio amount_usd / avg_txn_amount_30d")
    distinct_countries_30d: int = Field(..., ge=0, description="Number of distinct countries in the last 30 days")
    geo_mismatch: int = Field(..., ge=0, le=1, description="1 if IP ≠ card country, otherwise 0")
    device_fingerprint_match: int = Field(..., ge=0, le=1, description="1 if fingerprint matches history, otherwise 0")
    merchant_fraud_rate_7d: float = Field(..., ge=0, le=1, description="Merchant's fraud rate over the last 7 days")

    class Config:
        json_schema_extra = {
            "example": {
                "amount_usd": 150.0,
                "txn_count_24h": 2,
                "avg_txn_amount_30d": 100.0,
                "amount_ratio_30d": 1.5,
                "distinct_countries_30d": 1,
                "geo_mismatch": 0,
                "device_fingerprint_match": 1,
                "merchant_fraud_rate_7d": 0.05
            }
        }

class PredictionResponse(BaseModel):
    """Response from the prediction endpoint."""
    fraud_score: float = Field(..., ge=0, le=1, description="Fraud score between 0 and 1")
    decision: str = Field(..., description="'fraud' if score > threshold, otherwise 'legit'")

class TransactionResponse(BaseModel):
    """Model for a transaction returned by the API."""
    transaction_id: str
    customer_id: str
    amount_usd: float
    created_at: datetime
    status: str

class FraudEventResponse(BaseModel):
    """Model for a fraud event returned by the API."""
    transaction_id: str
    fraud_score: float
    decision: str
    timestamp: datetime
    model_version: Optional[str] = None