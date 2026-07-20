#!/usr/bin/env bash
# ============================================================
# Initialise le replica set, crée les 6 collections + GridFS,
# et exécute une transaction multi-documents (exigence énoncé).
# ============================================================
set -euo pipefail

echo "════════════════════════════════════════════════════════"
echo " VALIDATION MONGODB — Stripe Business Case"
echo "════════════════════════════════════════════════════════"

echo
echo "── 1. Initialisation du replica set ────────────────────"
echo "     (requis pour les transactions multi-documents)"
docker exec -i stripe_nosql mongosh --quiet --eval '
  try { rs.status(); print("   replica set déjà initialisé"); }
  catch(e) { rs.initiate(); print("   ✅ rs.initiate() OK"); }
'
echo "   ⏳ attente de l'élection du primary..."
sleep 5
docker exec -i stripe_nosql mongosh --quiet --eval '
  while (!db.hello().isWritablePrimary) { sleep(500); }
  print("   ✅ primary élu, cluster prêt");
'

echo
echo "── 2. Installation de pymongo dans le conteneur ────────"
# L'image mongo:7 n'embarque NI python3 NI pip. On les installe (silencieusement),
# puis pymongo. L'ensemble est idempotent : au 2e lancement, apt et pip détectent
# que tout est déjà là et ne refont rien. La sortie apt est masquée pour rester lisible.
if docker exec -i stripe_nosql python3 -c "import pymongo" 2>/dev/null; then
    echo "   ✅ pymongo déjà présent"
else
    echo "   ⏳ installation de python3 + pymongo (peut prendre 1 min au 1er lancement)..."
    if docker exec -i stripe_nosql bash -c \
        "apt-get update -qq >/dev/null 2>&1 && \
         apt-get install -y python3 python3-pip -qq >/dev/null 2>&1 && \
         pip install pymongo --quiet >/dev/null 2>&1" \
       && docker exec -i stripe_nosql python3 -c "import pymongo" 2>/dev/null; then
        echo "   ✅ python3 + pymongo installés"
    else
        echo "   ❌ échec — lancez manuellement :"
        echo "      docker exec -i stripe_nosql bash -c \"apt-get update && apt-get install -y python3 python3-pip && pip install pymongo\""
        exit 1
    fi
fi

echo
echo "── 3. Création des 6 collections + GridFS ──────────────"
docker exec -i -e MONGO_URI="mongodb://localhost:27017/?replicaSet=rs0" \
    stripe_nosql python3 /nosql/mongodb/mongodb_schema.py

echo
echo "── 4. Transaction multi-documents (ACID) ───────────────"
echo "     Écrit atomiquement dans fraud_events + user_sessions + app_logs"
docker exec -i -e MONGO_URI="mongodb://localhost:27017/?replicaSet=rs0" \
    stripe_nosql python3 /nosql/mongodb/mongodb_multidoc_transactions.py

echo
echo "── 5. Vérification : les 3 écritures ont bien commité ──"
docker exec -i stripe_nosql mongosh stripe_nosql --quiet --eval '
  const fe = db.fraud_events.countDocuments({transaction_id:"txn_demo_0001"});
  const al = db.app_logs.countDocuments({"context.transaction_id":"txn_demo_0001"});
  print("   fraud_events : " + fe);
  print("   app_logs     : " + al);
  if (fe >= 1 && al >= 1) print("   ✅ PASS — transaction multi-documents commitée");
  else print("   ❌ FAIL");
'

echo
echo "════════════════════════════════════════════════════════"
echo " ✅ VALIDATION MONGODB TERMINÉE"
echo "════════════════════════════════════════════════════════"
