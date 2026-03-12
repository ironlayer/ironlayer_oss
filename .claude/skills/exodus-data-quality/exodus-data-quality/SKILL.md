---
name: exodus-data-quality
description: >
  Generate data quality tests and validation rules for Exodus Foundation. Covers dbt tests,
  DLT expectations, source freshness checks, anomaly detection config, and custom SQL validations.
  Use when adding tests to models, building quality frameworks, or debugging data issues.
triggers:
  - "add data quality tests"
  - "add dbt tests"
  - "data quality for"
  - "freshness check"
  - "anomaly detection"
  - "data validation"
outputs:
  - YAML test blocks for dbt model files
  - DLT expectation definitions
  - Anomaly detection config entries for client.yml
  - Custom singular test SQL files
---

# Data Quality Framework

Generate comprehensive data quality tests for Foundation — covering dbt, DLT, freshness, and anomaly detection.

> **Framework note:** This skill covers dbt Core data quality. For SQLMesh projects, data quality is enforced via `audits` in the `MODEL()` DDL block and unit tests in `tests/*.yaml`. See the `exodus-sqlmesh` skill for SQLMesh-specific quality patterns.

---

## Before You Start

```bash
# Check what tests already exist
ls dbt/tests/
ls dbt/tests/generic/

# Review existing model YAML for test patterns
cat dbt/models/marts/dim_customer.yml 2>/dev/null || ls dbt/models/

# Run current tests to establish baseline
cd dbt && dbt test --target dev 2>&1 | tail -20
```

---

## Test Tier Reference

| Tier | Tool | When to Use |
|------|------|-------------|
| Schema tests | dbt YAML | Always — every model with a PK |
| Freshness tests | dbt source freshness | Every source table |
| Singular SQL tests | `dbt/tests/*.sql` | Business rule validation |
| DLT expectations | Python `@expect*` decorators | Streaming pipelines |
| Anomaly detection | `config/client.yml golden_metrics` | Production metric monitoring |

---

## Tier 1 — dbt YAML Tests (Add to Every Model)

```yaml
models:
  - name: fct_daily_revenue
    description: "Daily revenue fact — one row per order per day"
    columns:
      - name: order_key
        description: "Surrogate key"
        tests:
          - unique
          - not_null

      - name: customer_key
        description: "FK to dim_customer"
        tests:
          - not_null
          - relationships:
              to: ref('dim_customer')
              field: customer_key

      - name: revenue_usd
        description: "Net revenue in USD"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"

      - name: order_date_key
        description: "FK to dim_date"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: "> 0"

      - name: status
        description: "Order status"
        tests:
          - accepted_values:
              values: ['completed', 'pending', 'refunded', 'cancelled']

  - name: dim_customer
    columns:
      - name: customer_key
        tests: [unique, not_null]
      - name: email
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: "like '%@%.%'"
```

---

## Tier 2 — Source Freshness

```yaml
# dbt/models/staging/{source}/__sources.yml
sources:
  - name: {source}
    freshness:
      warn_after: {count: 24, period: hour}
      error_after: {count: 48, period: hour}
    loaded_at_field: _loaded_at
    tables:
      - name: {table}
        freshness:
          warn_after: {count: 25, period: hour}   # override per table if needed
```

Run: `cd dbt && dbt source freshness`

---

## Tier 3 — Singular SQL Tests

Use for business logic that YAML tests can't express:

```sql
-- dbt/tests/assert_no_negative_revenue.sql
-- All revenue must be >= 0. Zero is valid (free tier orders).
SELECT
    order_key,
    revenue_usd
FROM {{ ref('fct_daily_revenue') }}
WHERE revenue_usd < 0
```

```sql
-- dbt/tests/assert_referential_integrity_fct_to_dim.sql
-- Every fct_daily_revenue row must have a matching dim_customer row.
SELECT f.order_key
FROM {{ ref('fct_daily_revenue') }} f
LEFT JOIN {{ ref('dim_customer') }} d USING (customer_key)
WHERE d.customer_key IS NULL
```

```sql
-- dbt/tests/assert_metric_coverage_complete.sql
-- No gaps in daily revenue metric — every calendar day must have data.
WITH
date_spine AS (
    SELECT calendar_date
    FROM {{ ref('dim_date') }}
    WHERE calendar_date BETWEEN
        (SELECT MIN(order_date) FROM {{ ref('fct_daily_revenue') }})
        AND CURRENT_DATE()
),
actual AS (
    SELECT DISTINCT order_date FROM {{ ref('fct_daily_revenue') }}
)
SELECT date_spine.calendar_date
FROM date_spine
LEFT JOIN actual ON date_spine.calendar_date = actual.order_date
WHERE actual.order_date IS NULL
```

---

## Tier 4 — DLT Expectations (Streaming Pipelines)

```python
# foundation/dlt/pipelines/{source}_pipeline.py
import dlt

@dlt.table(name="bronze_{source}_{table}")
@dlt.expect_or_drop("valid_pk", "{pk_column} IS NOT NULL")
@dlt.expect_or_drop("valid_amount", "amount >= 0")
@dlt.expect("recent_data", "_loaded_at > current_timestamp() - INTERVAL 48 HOURS")
def bronze_raw():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load(f"{landing_path}/{source}/{table}/")
    )
```

**Expectation levels:**
- `@dlt.expect` — track but don't drop (WARNING)
- `@dlt.expect_or_drop` — drop violating rows (QUARANTINE)
- `@dlt.expect_or_fail` — halt pipeline on any violation (BLOCK)

---

## Tier 5 — Anomaly Detection Config

```yaml
# config/client.yml — golden_metrics section
golden_metrics:
  fct_daily_revenue:
    - column: revenue_usd
      threshold: 2.0         # z-score standard deviations for WARN
      block_threshold: 3.0   # z-score for BLOCK (prevents deployment)
    - column: order_count
      threshold: 1.5
      block_threshold: 2.5

  dim_customer:
    - column: record_count   # total dimension size
      threshold: 0.1         # 10% change triggers WARN
      block_threshold: 0.3   # 30% change blocks deployment
```

Autopilot `DataStewardAgent` reads these metrics and flags anomalies via MCP `autopilot_agent_status`.

---

## Quality Scorecard

Produce this table after adding tests:

```markdown
## Quality Scorecard — {Model/Domain}

| Check | Status |
|-------|--------|
| PK unique + not_null | ✅ |
| FK referential integrity | ✅ |
| Source freshness < 24h | ✅ |
| Revenue >= 0 | ✅ |
| Metric coverage complete | ✅ |
| DLT expectations active | ✅ |
| Anomaly detection configured | ✅ |
| Coverage target (80%) | ✅ |
```

Run final check:
```bash
cd dbt
dbt test --target dev --store-failures
dbt source freshness
```

---

## SQLMesh Data Quality (Reference)

For SQLMesh clients, quality is enforced via MODEL-level audits (not YAML test files):

### Built-in Audits (in MODEL DDL)
```sql
MODEL (
  name prod.gold.fct_orders,
  kind INCREMENTAL_BY_TIME_RANGE (...),
  grain (order_id),
  audits (
    UNIQUE_VALUES(columns=(order_id)),          -- equivalent to dbt unique test
    NOT_NULL(columns=(order_id, event_date)),   -- equivalent to dbt not_null test
    ACCEPTED_VALUES(column=status, is_in=('active', 'cancelled', 'completed')),
    NUMBER_OF_ROWS(threshold := 1000),          -- minimum row count guard
    AT_LEAST_ONE(column=amount_usd)             -- at least one non-null value
  ),
);
```

### Unit Tests (tests/*.yaml)
```yaml
model: prod.gold.fct_orders
inputs:
  raw.staging.stg_orders:
    rows:
      - order_id: "ord-1"
        amount_usd: 100.00
        event_date: "2025-01-15"
outputs:
  query:
    rows:
      - order_id: "ord-1"
        amount_usd: 100.0000
```

Run SQLMesh quality checks:
```bash
sqlmesh test           # unit tests
sqlmesh run            # triggers audits automatically
# Or via MCP: sqlmesh_test
```
