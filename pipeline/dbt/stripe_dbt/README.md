# Projet dbt pour Stripe OLAP

Ce projet dbt transforme les données brutes chargées dans Snowflake (schéma `staging`) en un schéma en étoile (`analytics`).

## Utilisation

- Installer dbt-snowflake : `pip install dbt-snowflake`
- Configurer le profil `profiles.yml` avec les identifiants Snowflake
- Lancer : `dbt run --target prod`
- Tester : `dbt test`