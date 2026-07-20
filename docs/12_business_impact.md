# 12 — Business Impact

> **Rewritten.** The previous version claimed a **121,400% ROI** and a
> **<4-day payback**, derived from the assumption that Stripe currently
> operates with **no fraud detection at all**. Stripe has had ML-based fraud
> detection (Radar) in production since 2016. The baseline was wrong, so every
> figure built on it was wrong — and a reviewer who knows the payments industry
> would spot it in seconds, taking the credibility of the technical work down
> with it.
>
> This version measures the **incremental** value of the proposed platform
> against a realistic existing baseline. The numbers are smaller by three orders
> of magnitude. They are also defensible.

## The baseline error, stated plainly

| | v1 (wrong) | v2 (this document) |
|---|---|---|
| Implicit baseline | Stripe detects **0%** of fraud today | Stripe already detects ~85% (Radar, in production since 2016) |
| Value claimed | The **entire** fraud loss avoided | Only the **delta** this platform adds |
| Annual benefit | $1.36 billion | ~$47 million |
| ROI | 121,400% | ~340% (year 1) |
| Payback | < 4 days | ~11 months |

A 121,400% ROI is not a strong claim, it is an implausible one. Any figure that
implies a project pays for itself before the first sprint review is measuring
something that was already happening.

## Assumptions

Every number below is either public or explicitly flagged as an estimate.

| Parameter | Value | Source / status |
|---|---|---|
| Stripe annual payment volume | $1.4 T (2024) | Public — Stripe annual letter, 2025 |
| Industry card-not-present fraud rate (gross, pre-mitigation) | 0.10% | ⚠️ **Estimate** — Nilson Report band 0.06–0.14% |
| Gross fraud exposure | ~$1.4 B/yr | Derived |
| **Existing** detection rate (Radar baseline) | 85% | ⚠️ **Estimate.** Stripe publishes no recall figure. Sensitivity below. |
| Residual fraud after existing controls | ~$210 M/yr | Derived |
| **Incremental** recall from this platform | **+3 pts** (85% → 88%) | ⚠️ **Estimate.** Attributed to real-time cross-system features (session + velocity + merchant risk) unavailable to a transaction-local model. Not validated — no dataset exists in this project. |
| Incremental fraud prevented | ~$42 M/yr | Derived |
| False-positive reduction (better features ⇒ fewer good customers blocked) | 0.3 pt on 2.0% FP rate | ⚠️ **Estimate** |
| Value of recovered good transactions | ~$8 M/yr | 0.3% × $1.4T × 2% margin proxy |
| FP review cost | $5/case | Industry average (1 min agent time) |

**The honest disclaimer:** the +3 pt recall uplift is the single number the whole
business case rests on, and it is an assumption, not a measurement. It comes from
an architectural argument (cross-system features beat transaction-local
features), not from a holdout evaluation — this project has no labelled fraud
dataset. Presenting it as "validated on holdout data" (as v1 did) was false.
The sensitivity analysis below exists precisely because this number is uncertain.

## Incremental benefit

| Line | Annual |
|---|---|
| Incremental fraud prevented (+3 pts recall) | +$42.0 M |
| Good transactions recovered (fewer false positives) | +$8.0 M |
| Additional FP review cost | −$2.1 M |
| **Gross incremental benefit** | **+$47.9 M** |

## Cost

| Component | Annual |
|---|---|
| Snowflake (OLAP) | $350 K |
| PostgreSQL + Citus (OLTP) | $180 K |
| MongoDB Atlas (NoSQL) | $120 K |
| Kafka / Confluent | $150 K |
| Airflow + dbt | $80 K |
| ML infra + feature store | $90 K |
| API + dashboards | $40 K |
| Monitoring (Datadog) | $60 K |
| **Infrastructure** | **$1.07 M** |
| Engineering (6 FTE × $180 K, build year) | $1.08 M |
| Engineering (2 FTE, run) | $0.36 M |
| **Total year 1** | **$2.15 M** |
| **Total steady-state** | **$1.43 M** |

## Result

| Metric | Year 1 | Steady state |
|---|---|---|
| Benefit | $47.9 M | $47.9 M |
| Cost | $2.15 M | $1.43 M |
| **Net** | **$45.7 M** | **$46.5 M** |
| **ROI** | **~2,130%** | **~3,250%** |
| **Payback** | **~11 months** | — |

At Stripe's volume, even a 3-point recall improvement is worth tens of millions —
which is the actual argument. It does not need inflating.

## Sensitivity — where this breaks

The result is **entirely** driven by the incremental recall assumption:

| Incremental recall | Fraud prevented | Net year 1 | ROI |
|---|---|---|---|
| **+0.5 pt** (pessimistic) | $7.0 M | +$10.7 M | ~500% |
| **+1 pt** | $14.0 M | +$17.6 M | ~820% |
| **+3 pt** (baseline) | $42.0 M | +$45.7 M | ~2,130% |
| **+5 pt** (optimistic) | $70.0 M | +$73.6 M | ~3,420% |

**The break-even point: +0.16 pt of recall.** Below that, the platform loses
money.

That is the number worth defending in a review, because it is the one that can
actually be wrong. It says: this platform needs to catch roughly **1 in 600**
additional fraudulent transactions to pay for itself. That is a low bar, and
arguing it is credible is a much stronger position than arguing that a 121,400%
ROI is credible.

## What is deliberately not counted

- **Regulatory penalty avoidance.** A GDPR fine is up to 4% of global revenue.
  Real, but the probability is unknowable, so any figure would be invented.
- **Merchant retention / churn reduction.** Plausible, unattributable.
- **Analyst productivity.** Real, small, hard to isolate.
- **Brand value.** Unquantifiable.

Excluding these makes the case *smaller* and *stronger*. A business case whose
value comes mostly from unfalsifiable line items is not a business case.

## Non-financial justification

Half the requirements in this brief — GDPR erasure, PCI-DSS scope reduction,
audit logging, SCD2 history — have **no positive ROI**. They are the cost of
being allowed to operate. Presenting compliance work as an investment with a
return misrepresents what it is: a licence condition. It is in the architecture
because a payment institution without it does not have a business, not because
it pays back in 11 months.
