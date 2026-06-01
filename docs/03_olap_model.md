# 03 — OLAP Data Model

## Objective

The OLAP layer supports analytical queries, reporting, customer segmentation, fraud analysis and compliance dashboards.

## Technology Choice

**Snowflake** is selected as the target analytical platform because it provides:

- separation of storage and compute
- scalable analytical workloads
- Time Travel for recovery and auditability
- native support for dbt transformations
- efficient handling of large joins and aggregations

## Modelling Approach

The OLAP model uses a **star schema**.

## Fact Table

| Table               | Purpose                                                                            |
|---------------------|------------------------------------------------------------------------------------|
| `fact_transactions` | Central transactional fact table used for revenue, fraud and performance analytics |

## Dimensions

| Dimension             | Purpose                                           |
|-----------------------|---------------------------------------------------|
| `dim_customer`        | Customer segmentation and historical attributes   |
| `dim_merchant`        | Merchant category, region and risk level          |
| `dim_currency`        | Currency conversion and exchange rates            |
| `dim_payment_method`  | Payment channel analysis                          |
| `dim_date`            | Time-series analysis                              |

## Slowly Changing Dimensions

`dim_customer` and `dim_merchant` follow **SCD Type 2** principles.

This allows the platform to preserve historical changes such as:

- customer segment changes
- merchant category updates
- merchant risk level changes
- regional reclassification

## Aggregation Strategy

Common reporting workloads are optimised through:

- materialised views
- dbt marts
- pre-aggregated revenue tables
- clustering on date and merchant keys

## Example Analytical Questions

The OLAP layer supports:

- revenue by region
- fraud rate by merchant category
- customer RFM segmentation
- monthly transaction trends
- chargeback monitoring
- compliance reporting

## Trade-offs

| Choice        | Benefit                       | Trade-off                     |
|---------------|-------------------------------|-------------------------------|
| Star schema   | simple and performant queries  | controlled denormalisation   |
| Snowflake     | scalable analytics             | cost governance required     |
| SCD Type 2    | full history                   | larger dimension tables      |

## Alignment with Project Requirements

This model satisfies the OLAP requirements by providing:

- star schema design
- complex aggregations
- time-series analysis
- large-scale joins
- performance optimisation through marts and materialised views