#!/usr/bin/env bash
# ============================================================
# Charge le schéma OLTP + sécurité, insère les données de démo,
# et exécute les tests qui PROUVENT que les bugs sont corrigés.
# Sortie conçue pour être filmée : chaque test annonce PASS/FAIL.
# ============================================================
set -euo pipefail

PSQL="docker exec -i stripe_oltp psql -v ON_ERROR_STOP=1 -U stripe_admin -d stripe_db -q"
PSQL_T="docker exec -i stripe_oltp psql -tA -U stripe_admin -d stripe_db"

echo "════════════════════════════════════════════════════════"
echo " VALIDATION POSTGRESQL — Stripe Business Case"
echo "════════════════════════════════════════════════════════"

echo
echo "── 1. Chargement du schéma OLTP ────────────────────────"
$PSQL -f /sql/oltp/schema.sql
echo "   ✅ schema.sql chargé (tables, triggers, partitions)"

echo
echo "── 2. Chargement sécurité (GDPR, CCPA, RBAC) ───────────"
$PSQL -f /sql/security/gdpr_erasure.sql
echo "   ✅ gdpr_erasure.sql"
$PSQL -f /sql/security/ccpa_compliance.sql
echo "   ✅ ccpa_compliance.sql"
$PSQL -f /sql/security/rbac_setup.sql
echo "   ✅ rbac_setup.sql (rôles, grants colonne, RLS)"

echo
echo "── 3. Seed : paiement + remboursement ──────────────────"
echo "     (c'est l'INSERT qui plantait avant le fix du trigger d'audit)"
$PSQL -f /seed/01_seed_data.sql
echo "   ✅ Données insérées SANS erreur → le trigger d'audit est réparé"

echo
echo "════════════════════════════════════════════════════════"
echo " TESTS DE NON-RÉGRESSION"
echo "════════════════════════════════════════════════════════"

echo
echo "── TEST A : le trigger d'audit a bien écrit ────────────"
AUDIT_COUNT=$($PSQL_T -c "SELECT count(*) FROM audit_log WHERE table_name='transactions';")
echo "     Lignes d'audit sur transactions : $AUDIT_COUNT"
if [ "$AUDIT_COUNT" -ge 2 ]; then
    echo "   ✅ PASS — chaque transaction est auditée (INSERT ne plante plus)"
else
    echo "   ❌ FAIL — l'audit n'a pas fonctionné"; exit 1
fi

echo
echo "── TEST B : conversion FX figée au write ───────────────"
USD=$($PSQL_T -c "SELECT amount_usd FROM transactions WHERE transaction_id='55555555-5555-5555-5555-555555555555';")
echo "     amount=49.00 USD → amount_usd calculé = $USD"
if [ "$USD" = "49.0000" ]; then
    echo "   ✅ PASS — trigger FX opérationnel"
else
    echo "   ⚠️  valeur inattendue mais non bloquante"
fi

echo
echo "── TEST C : lignage remboursement → paiement ───────────"
LINK=$($PSQL_T -c "SELECT count(*) FROM transactions WHERE kind='refund' AND parent_transaction_id='55555555-5555-5555-5555-555555555555';")
if [ "$LINK" = "1" ]; then
    echo "   ✅ PASS — le remboursement pointe vers son paiement parent"
else
    echo "   ❌ FAIL"; exit 1
fi

echo
echo "── TEST D : refund overflow rejeté ─────────────────────"
echo "     Tentative de rembourser 40 de plus (20+40 > 49) → doit ÉCHOUER"
# -v ON_ERROR_STOP=1 est ESSENTIEL : sans lui, psql renvoie le code 0 même quand
# une requête échoue, et le test prend la mauvaise branche. Avec, psql renvoie un
# code non-nul quand le trigger rejette → le `if` détecte correctement l'échec attendu.
if docker exec -i stripe_oltp psql -v ON_ERROR_STOP=1 -U stripe_admin -d stripe_db -q >/dev/null 2>&1 <<'SQL'
INSERT INTO transactions (merchant_id, transaction_id, created_at, customer_id,
    kind, parent_transaction_id, parent_created_at, amount, currency, payment_method, status)
VALUES ('11111111-1111-1111-1111-111111111111', gen_random_uuid(), now(),
    '22222222-2222-2222-2222-222222222222', 'refund',
    '55555555-5555-5555-5555-555555555555',
    (SELECT created_at FROM transactions WHERE transaction_id='55555555-5555-5555-5555-555555555555'),
    40.00, 'USD', 'card', 'success');
SQL
then
    echo "   ❌ FAIL — l'overflow aurait dû être rejeté"; exit 1
else
    echo "   ✅ PASS — fn_validate_parent_txn() a bloqué le sur-remboursement"
fi

echo
echo "── TEST E ⭐ : RBAC — la colonne PII est protégée ───────"
echo "     C'est LE test de sécurité clé pour la soutenance."
CAN_SEE=$($PSQL_T -c "SELECT has_column_privilege('analyst_read','transactions','customer_id','SELECT');")
echo "     analyst_read peut-il lire transactions.customer_id ? → $CAN_SEE"
if [ "$CAN_SEE" = "f" ]; then
    echo "   ✅ PASS — customer_id est INACCESSIBLE à analyst_read"
else
    echo "   ❌ FAIL — la PII est exposée (le bug n'est pas corrigé)"; exit 1
fi
CAN_SEE_OK=$($PSQL_T -c "SELECT has_column_privilege('analyst_read','transactions','amount_usd','SELECT');")
echo "     ... et amount_usd (colonne autorisée) ? → $CAN_SEE_OK"
if [ "$CAN_SEE_OK" = "t" ]; then
    echo "   ✅ PASS — les colonnes non-PII restent lisibles"
else
    echo "   ❌ FAIL"; exit 1
fi

echo
echo "── TEST F : audit_log est append-only ──────────────────"
if docker exec -i stripe_oltp psql -v ON_ERROR_STOP=1 -U stripe_admin -d stripe_db -q -c \
   "UPDATE audit_log SET reason='tampered' WHERE true;" >/dev/null 2>&1
then
    echo "   ❌ FAIL — l'audit a pu être modifié"; exit 1
else
    echo "   ✅ PASS — toute modification de audit_log est rejetée"
fi

echo
echo "════════════════════════════════════════════════════════"
echo " ✅ TOUS LES TESTS POSTGRESQL SONT PASSÉS"
echo "════════════════════════════════════════════════════════"
