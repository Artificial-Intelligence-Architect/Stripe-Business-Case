# 15 — Dependency Security Strategy

> **Why this document exists.** A project that claims security and compliance as
> first-class concerns must apply the same rigour to its own software supply
> chain. GitHub Dependabot flagged dependency vulnerabilities on this repository.
> This document is not an apology for them — it is the strategy for triaging and
> remediating them, which is the actual engineering competency a reviewer looks
> for. Zero alerts is not a realistic goal on a real project; a documented,
> prioritised response is.

## 1. The honest picture

Dependabot alerts on this repository come from a small number of packages:

| Package | Severity | Nature |
|---|---|---|
| `apache-airflow` + providers | up to critical | Large framework, pulls hundreds of transitive deps; historically a heavy Dependabot generator |
| `pyspark` | low | Weak encryption strength advisory |
| `pytest` | moderate | tmpdir handling — test tooling only, never ships to production |
| `scikit-learn` | moderate | Sensitive-data leakage (CVE-2024-5206) |

Key insight: most alerts come from a single dependency (Airflow). This is not a
set of independent problems — it is a handful of packages to move, plus the
critical fixes that matter more than all the moderate/low noise combined.

## 2. Triage — not all alerts are equal

Alerts are prioritised by exploitability in this project's context, not by count.

**Priority 1 — Critical, and genuinely relevant.** The
`apache-airflow-providers-snowflake` injection in
`CopyFromExternalStageToSnowflakeOperator`: this project uses Airflow to load into
Snowflake, so this operator is on our real code path. Remediation: raise the
provider floor above the patched version. First and non-negotiable.

**Priority 2 — Airflow core (RCE in an example DAG, deserialization, is_safe_url).**
Most concern the Airflow web UI and multi-tenant auth — surfaces that sit behind a
private network and Okta SSO in this architecture (see 06_security_compliance.md),
which reduces exposure. Still remediated by moving Airflow to a patched minor.

**Priority 3 — scikit-learn and pyspark.** Real but lower exploitability here;
floors raised to the patched minors.

**Priority 4 — pytest (noise for this project).** pytest is a development/test
dependency. It never runs in production and never touches customer data. Tracked,
patched opportunistically, but not a production risk — the kind of alert that
inflates a counter without representing real exposure.

## 3. Remediation approach

The requirements files pin version ranges. Dependabot flags the floor of each
range because the floor is the vulnerable version. The fix is to raise the floor
above the patched version, keeping the upper bound so nothing breaks structurally.
The authoritative versions come from each Dependabot alert's "patched version"
field — we do not guess, we read that field and pin at or above it.

Where Dependabot cannot open an automated fix (no lockfile), floors are raised
manually in requirements.txt and ml/requirements.txt, then the test suite is run
before merge.

## 4. Ongoing policy

- Automated scanning: Dependabot enabled on the default branch; alerts triaged by
  severity and context, not by count.
- Version pinning with ranges: floors above known-vulnerable versions, upper bounds
  to avoid surprise majors.
- Separation of concerns: requirements-dev.txt (test tooling) is distinct from
  requirements.txt (runtime), so a dev-only CVE is never confused with a production
  risk.
- CI gate (target): pip-audit in CI to fail a build on a new high/critical.
- Defence in depth: the architecture does not rely on dependency patching alone —
  Airflow's UI sits behind private networking and SSO, secrets are in Vault, so a
  UI-surface CVE is not directly internet-exposed.

## 5. Honest limitations

- Version numbers are illustrative; the source of truth is each Dependabot alert's
  "patched version" field. This document describes the method, not a frozen list.
- No pip-audit CI gate is wired yet — described as target state, the same honesty
  applied to PCI-DSS conformance (06) and accessibility (14).
- Airflow will keep generating alerts: it is a large, fast-moving framework. The
  goal is not zero alerts, it is zero unaddressed critical/high on our code path.
