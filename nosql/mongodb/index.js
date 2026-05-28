// ============================================================
// NoSQL/MongoDB: Création des index
// ============================================================

// Connexion à la base cible (à adapter)
// use stripe_nosql;

// ---------- COLLECTION fraud_events ----------

// 1. Index composite merchant_id + timestamp
// Utilisé pour les requêtes de monitoring par marchand.
db.fraud_events.createIndex(
  { merchant_id: 1, timestamp: -1 },
  { name: "idx_merchant_time" }
);

// 2. Index partiel sur les scores de fraude élevés
// Optimise la détection temps réel sans indexer tout le champ.
db.fraud_events.createIndex(
  { "fraud_signals.score": 1 },
  {
    name: "idx_fraud_score_high",
    partialFilterExpression: { "fraud_signals.score": { $gte: 0.7 } }
  }
);

// 3. Index pour les décisions en attente (flagged non revues)
// Accélère la file de traitement manuel.
db.fraud_events.createIndex(
  { decision: 1, timestamp: -1 },
  {
    name: "idx_decision_pending",
    partialFilterExpression: { decision: "flagged", reviewed_by: null }
  }
);

// ---------- COLLECTION user_sessions ----------

// 4. Index TTL (expiration automatique après 90 jours)
// started_at + expireAfterSeconds en secondes : 90 jours = 7776000
db.user_sessions.createIndex(
  { started_at: 1 },
  { expireAfterSeconds: 7776000, name: "idx_ttl_sessions" }
);

// 5. Index sur customer_id (pour lookup ou recherches)
// (pas encore créé dans le schéma de base, mais utile pour les jointures)
db.user_sessions.createIndex(
  { customer_id: 1 },
  { name: "idx_customer_id" }
);

// ---------- COLLECTION app_logs ----------

// 6. Index TTL pour les logs (30 jours)
db.app_logs.createIndex(
  { timestamp: 1 },
  { expireAfterSeconds: 2592000, name: "idx_ttl_logs" }
);

// 7. Index composite pour le filtrage par service/level/temps
db.app_logs.createIndex(
  { service: 1, level: 1, timestamp: -1 },
  { name: "idx_service_level_time" }
);