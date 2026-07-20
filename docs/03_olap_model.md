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

## Fact Tables

| Table                      | Grain                     | Purpose                                                              |
|----------------------------|---------------------------|----------------------------------------------------------------------|
| `fact_transactions`        | one row per transaction   | Revenue, fraud, refunds/chargebacks (linked via `parent_transaction_id`) |
| `fact_subscription_events` | one row per status change | MRR / churn / expansion, impossible from a status column alone       |

`fact_transactions` carries `kind` (payment/refund/chargeback) as a degenerate dimension; refunds are counted as NEGATIVE revenue, not summed as gross.

## Dimensions

| Dimension             | Purpose                                           |
|-----------------------|---------------------------------------------------|
| `dim_customer`        | Customer segmentation and historical attributes   |
| `dim_merchant`        | Merchant category, region and risk level          |
| `dim_currency`        | Currency conversion and exchange rates            |
| `dim_payment_method`  | Payment channel analysis                          |
| `dim_product`         | Product catalog, Product Performance Metrics      |
| `dim_subscription`    | Subscription state, subscription management       |
| `dim_date`            | Time-series analysis                              |

`dim_product` and `dim_subscription` were added to cover brief requirements (product performance, subscription management) that the earlier model omitted. `dim_currency` is SCD2 so a report re-run next year reuses the historical FX rate rather than the current one.

## Slowly Changing Dimensions

`dim_customer`, `dim_merchant`, `dim_product`, `dim_subscription` and `dim_currency` follow **SCD Type 2**, preserving history: customer segment changes, merchant category/risk updates, regional reclassification, price changes, FX rate history.

**The join rule that makes SCD2 correct** (and the #1 bug fixed in this project):

- Join a fact to a dimension by **surrogate key** gives point-in-time truth, with **no** `is_current` filter. The SK already pins the version current at load time. Filtering `is_current = true` here silently erases the history of every entity that ever changed, and last year's revenue would move.
- Join by **natural key** + `is_current = true` gives current-state reporting (e.g. a risk officer acting on a merchant's tier as it stands today).

Both are valid; they answer different questions. Every analytical query in `queries_analytics.sql` declares which one it uses.

## Aggregation Strategy

Common reporting workloads are optimised through:

- **Snowflake Dynamic Tables** (not materialised views; Snowflake MVs forbid joins and GROUP BY, and every aggregate here needs both). Declarative `TARGET_LAG` drives incremental refresh: `mv_daily_revenue` 1 h, `mv_customer_monthly` 1 d, `mv_subscription_mrr` 1 h.
- dbt marts (staging to intermediate to marts), SCD2 via dbt snapshots
- **Clustering** on `(date_sk, merchant_sk)`. Snowflake has no user-defined partitions; it uses micro-partitions plus a clustering key. There is no partition by transaction_date.

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