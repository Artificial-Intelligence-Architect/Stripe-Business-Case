# dbt Project for Stripe OLAP

This dbt project transforms raw data loaded into Snowflake (`staging` schema) into a star schema (`analytics`).

## Usage

- Install dbt-snowflake: `pip install dbt-snowflake`
- Configure the `profiles.yml` profile with Snowflake credentials
- Run: `dbt run --target prod`
- Test: `dbt test`