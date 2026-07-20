# Kit de validation runtime

Prouve, sur de vraies bases, que le schéma se charge et que les contrôles
de sécurité fonctionnent. À exécuter **avant d'enregistrer la vidéo** : un
screenshot de la sortie « TOUS LES TESTS PASSÉS » vaut mieux que dix slides.

## Prérequis
- Docker + Docker Compose

## Marche à suivre

```bash
cd validation
docker compose up -d           # démarre PostgreSQL + MongoDB

# vérifier que les 2 conteneurs sont "healthy" (10-15 s) :
docker compose ps

# puis :
./run_postgres_validation.sh   # charge le schéma + 6 tests de non-régression
./run_mongo_validation.sh      # 6 collections + transaction multi-documents

docker compose down -v         # nettoyage complet
```

**Deux points à connaître :**

- **Ports** : le kit expose PostgreSQL sur `5433` et MongoDB sur `27018` côté hôte.
  Si l'un est déjà pris (message `port is already allocated`), changez-le dans
  `docker-compose.yml` (ex. `5433` → `5455`) — les scripts passent par le réseau
  Docker interne, ils ne sont donc pas affectés par ce changement.
- **Première exécution Mongo plus longue** : l'image `mongo:7` n'embarque pas
  Python, que les scripts pymongo requièrent. Le script l'installe silencieusement
  au premier lancement (~1 min). Les lancements suivants sont instantanés.
  Si le replica set n'a pas fini d'élire son primary, une erreur de connexion peut
  apparaître au tout début — relancez simplement le script.


## Ce qui est prouvé côté PostgreSQL

| Test | Ce qu'il démontre | Bug d'origine couvert |
|------|-------------------|------------------------|
| A | Le trigger d'audit s'exécute et logge `transactions` | INSERT plantait ; puis nom de partition au lieu du nom logique |
| B | Conversion FX figée au write | — |
| C | Lignage remboursement → paiement parent | refunds sans lien vers le paiement |
| D | Sur-remboursement rejeté (20+40 > 49) | absence de contrôle d'intégrité |
| E | `analyst_read` ne voit PAS `customer_id`, voit `amount_usd` | REVOKE colonne sans effet (PII exposée) |
| F | `audit_log` est append-only | audit falsifiable |
| G/H | Effacement GDPR anonymise ET le rapport le voit | procédure écrivait 'D', rapport cherchait 'U' → toujours 0 |

## Deux bugs trouvés à l'exécution (invisibles au parsing)

Ces deux-là ne se voient qu'en chargeant réellement le SQL — d'où ce kit :

1. **`sla_due_at` en colonne générée** : PostgreSQL refuse une expression
   `GENERATED ALWAYS AS (timestamptz + interval)` car elle n'est pas immutable.
   Corrigé en `DEFAULT`.
2. **Trigger d'audit sur table partitionnée** : `TG_TABLE_NAME` renvoie le nom
   de la partition physique (`transactions_default`), pas de la table parente.
   Le nom logique est désormais passé en argument du trigger.

## Note Citus

Le schéma utilise la syntaxe compatible Citus, mais les appels
`create_distributed_table()` sont commentés. Il tourne donc sur un PostgreSQL
standard — idéal pour une démo. Les PK composites, triggers, RLS et grants
colonne se comportent exactement pareil, avec ou sans Citus.

## Si un port est déjà pris

Les ports hôte sont 5433 (Postgres) et 27018 (Mongo) justement pour éviter les
conflits avec une instance locale. Sinon, modifiez-les dans `docker-compose.yml`.
