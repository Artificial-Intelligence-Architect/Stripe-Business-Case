# Stripe Data Architecture — Executive Summary

## Context

Stripe processes **billions of transactions each year** for millions of merchants worldwide. The growth in transaction volumes, the diversity of data sources (payments, sessions, logs, fraud signals), and regulatory requirements (PCI DSS, GDPR, CCPA) demand a modern, scalable, and secure data architecture.

This project proposes a unified architecture that addresses three fundamental challenges:

* **Reliability**: every transaction must be recorded without loss, even in the event of a system failure.
* **Speed**: detect fraudulent activity before settlement, in under 100 milliseconds.
* **Intelligence**: analyse millions of transactions to understand customer behaviour and anticipate risks.

---

## The Three Pillars of the Architecture

### 1. Transactional System (OLTP) — *The Cash Register*

All transactions are recorded in real time within a highly available PostgreSQL database. This system ensures that no payment is lost, even during failures, and that every operation is atomic: it either completes successfully in its entirety or does not occur at all.

**Key figures**: 10,000 transactions per second, 99.99% availability (less than 52 minutes of downtime per year).

### 2. Analytical System (OLAP) — *The Strategic Dashboard*

Transactional data is transformed and consolidated within a cloud data warehouse (Snowflake) to support complex analysis, including revenue by region, customer segmentation, and trend detection. These insights inform both product and commercial decision-making.

**Key figures**: analytical queries executed in under one second across billions of rows.

### 3. Non-Relational System (NoSQL) — *The Intelligent Notebook*

Semi-structured data—including fraud events, user sessions, application logs, and machine learning feature sets—is stored in MongoDB. This system is designed for flexibility and high-speed access by ML models.

**Key figures**: fraud detection in under 50 milliseconds, with 100% of transactions analysed before settlement.

---

## Business Impact

| Area               | Expected Outcome                      | Estimated Value                                       |
| ------------------ | ------------------------------------- | ----------------------------------------------------- |
| Fraud Reduction    | −40% fraudulent transactions          | **+$15M per year**                                    |
| Customer Retention | +12% through churn prediction         | **+$25M per year**                                    |
| Availability       | 99.99% (contractual SLA achieved)     | Reputational risk avoided                             |
| Compliance         | Automated GDPR and PCI DSS compliance | Regulatory fines avoided (up to 4% of annual revenue) |

**Estimated return on investment: $40M per year**, for a cloud infrastructure cost of approximately $2–3M annually.

---

## What This Project Demonstrates

This architecture is not a prototype. It is designed for production deployment with:

* Automated, tested, and monitored data pipelines.
* End-to-end security (encryption, access control, and auditing).
* Artificial intelligence models embedded at the core of the platform rather than added as an afterthought.

> For a detailed technical presentation, see the Architecture Guide (`02_INTERMEDIATE_README.md`).
