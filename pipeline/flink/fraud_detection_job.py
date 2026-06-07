"""
Stripe Fraud Detection — Faust Stream Processing Job
Remplace Apache Flink/Java par Faust (Python natif, Kafka-native).

Architecture :
    Kafka topic: pg.transactions  →  Faust Agent  →  fraud_score  →  Kafka topic: stripe.fraud_events
                                                    ↓
                                              MongoDB (fraud_events)
                                              PostgreSQL (fraud_score update)

Fenêtrage : Tumbling window de 5 minutes pour calcul de vélocité.
Latence cible : < 100 ms p99.
"""

import logging
from datetime import datetime, timezone

import faust
from faust import Record
from faust.types import StreamT

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schémas Avro-compatibles via Faust Records
# ---------------------------------------------------------------------------

class TransactionEvent(Record, serializer="json"):
    transaction_id: str
    merchant_id: str
    customer_id: str
    amount_usd: float
    currency: str
    payment_method: str
    status: str
    device_type: str
    ip_country: str
    created_at: str


class FraudEvent(Record, serializer="json"):
    transaction_id: str
    merchant_id: str
    customer_id: str
    timestamp: str
    fraud_score: float
    velocity_flag: bool
    geo_anomaly: bool
    decision: str          # "approved" | "flagged" | "blocked"
    model_version: str


# ---------------------------------------------------------------------------
# Application Faust
# ---------------------------------------------------------------------------

app = faust.App(
    "stripe-fraud-detection",
    broker="kafka://localhost:9092",
    value_serializer="json",
    topic_partitions=4,
    # Rétention des tables de state (RocksDB par défaut)
    table_cleanup_interval=3600.0,
)

# Topics Kafka
transactions_topic = app.topic(
    "pg.transactions",
    value_type=TransactionEvent,
)

fraud_events_topic = app.topic(
    "stripe.fraud_events",
    value_type=FraudEvent,
)

# ---------------------------------------------------------------------------
# Tables de state — vélocité par client (tumbling window 5 min)
# ---------------------------------------------------------------------------

# Compteur de transactions par customer sur fenêtre glissante
txn_count_table = app.Table(
    "txn_count_per_customer",
    default=int,
    help="Nombre de transactions par customer_id dans la fenêtre active.",
).tumbling(300, expires=600)  # 5 min window, expire après 10 min

# Dernier pays connu par customer
last_country_table = app.Table(
    "last_country_per_customer",
    default=str,
    help="Dernier ip_country observé par customer_id.",
)


# ---------------------------------------------------------------------------
# Règles de scoring — heuristiques métier
# ---------------------------------------------------------------------------

def compute_fraud_score(event: TransactionEvent, txn_count: int, last_country: str) -> tuple[float, bool, bool]:
    """
    Calcule un fraud_score [0, 1] à partir de règles heuristiques.
    En production : remplacer par appel FastAPI → modèle XGBoost (< 50 ms).

    Returns:
        (fraud_score, velocity_flag, geo_anomaly)
    """
    score = 0.0
    velocity_flag = False
    geo_anomaly = False

    # Règle 1 : Vélocité — plus de 10 transactions en 5 min
    if txn_count > 10:
        score += 0.45
        velocity_flag = True
        logger.warning(
            "Velocity alert: customer=%s txn_count=%d", event.customer_id, txn_count
        )

    # Règle 2 : Anomalie géographique — changement de pays
    if last_country and last_country != event.ip_country:
        score += 0.35
        geo_anomaly = True
        logger.warning(
            "Geo anomaly: customer=%s %s→%s",
            event.customer_id, last_country, event.ip_country,
        )

    # Règle 3 : Montant élevé (> 1000 USD)
    if event.amount_usd > 1000.0:
        score += 0.15

    # Règle 4 : Méthode de paiement à risque
    if event.payment_method in ("prepaid_card", "crypto"):
        score += 0.10

    return min(score, 1.0), velocity_flag, geo_anomaly


def decide(fraud_score: float) -> str:
    """Convertit un score en décision métier."""
    if fraud_score >= 0.80:
        return "blocked"
    if fraud_score >= 0.50:
        return "flagged"
    return "approved"


# ---------------------------------------------------------------------------
# Agent principal — traitement de chaque transaction
# ---------------------------------------------------------------------------

@app.agent(transactions_topic, sink=[fraud_events_topic])
async def detect_fraud(stream: StreamT[TransactionEvent]):
    """
    Agent Faust — consomme pg.transactions, produit stripe.fraud_events.

    Fenêtrage :
        - txn_count_table : tumbling window 5 min → vélocité
        - last_country_table : state simple → détection geo-anomalie

    SLA : < 100 ms par événement (hors appel ML externe).
    """
    async for event in stream:
        try:
            # Mise à jour du compteur de vélocité dans la fenêtre courante
            txn_count_table[event.customer_id] += 1
            current_count: int = txn_count_table[event.customer_id].current()

            # Lecture du dernier pays connu
            last_country: str = last_country_table[event.customer_id]

            # Calcul du score
            fraud_score, velocity_flag, geo_anomaly = compute_fraud_score(
                event, current_count, last_country
            )
            decision = decide(fraud_score)

            # Mise à jour du dernier pays
            last_country_table[event.customer_id] = event.ip_country

            # Émission de l'événement fraude vers Kafka
            fraud_event = FraudEvent(
                transaction_id=event.transaction_id,
                merchant_id=event.merchant_id,
                customer_id=event.customer_id,
                timestamp=datetime.now(timezone.utc).isoformat(),
                fraud_score=round(fraud_score, 4),
                velocity_flag=velocity_flag,
                geo_anomaly=geo_anomaly,
                decision=decision,
                model_version="heuristic-v1.0",  # → "xgb-v2.3" en production
            )

            if decision in ("flagged", "blocked"):
                logger.info(
                    "FRAUD %s | tx=%s customer=%s score=%.2f",
                    decision.upper(), event.transaction_id, event.customer_id, fraud_score,
                )

            yield fraud_event

        except Exception as exc:  # noqa: BLE001
            logger.error("Error processing transaction %s: %s", getattr(event, "transaction_id", "?"), exc)
            # Ne pas re-raise : éviter le blocage du consumer group


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.main()