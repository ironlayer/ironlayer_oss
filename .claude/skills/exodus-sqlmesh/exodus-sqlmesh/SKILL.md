---
name: exodus-sqlmesh
description: >
  Generate SQLMesh models, run plan/apply workflows, convert between dbt and SQLMesh formats,
  and use SQLMesh table diff for CI data validation. Use when creating SQLMesh models for clients
  who have chosen SQLMesh as their transformation framework, converting existing dbt projects to
  SQLMesh, or setting up SQLMesh CI/CD with IronLayer governance.
triggers:
  - "create a sqlmesh model"
  - "convert dbt model to sqlmesh"
  - "run sqlmesh plan"
  - "sqlmesh table diff"
  - "add sqlmesh blueprint"
  - "sqlmesh audit"
  - "client uses sqlmesh"
  - "sqlmesh virtual environment"
outputs:
  - "models/{layer}/*.sql"
  - "models/{layer}/*.py"
  - "tests/*.yaml"
  - "macros/*.py"
  - "config.yaml"
---

# SQLMesh Model Generator — Exodus

> **This skill is for SQLMesh projects** — clients or projects using SQLMesh as their transformation framework.
> For dbt Core projects (Foundation, ironlayer_infra), use `exodus-dbt-model`.
> For dbt Cloud Enterprise client work, use `exodus-dbt-cloud`.

---

## Before You Start — Understand the Project

```bash
# 1. Check the SQLMesh project structure
ls -la
cat config.yaml          # Gateway, connections, project name

# 2. Check existing models
ls models/
ls models/staging/
ls models/intermediate/
ls models/marts/

# 3. Review existing conventions
ls macros/               # Custom macros
ls audits/               # Custom audit definitions
ls tests/                # Unit tests

# 4. Check IronLayer framework detection
ironlayer plan --dry-run  # Should say: "Detected framework: sqlmesh"
```

---

## Step 1: Framework Detection

Confirm you are in a SQLMesh project before generating any code:

**SQLMesh indicators:**
- `config.yaml` or `config.py` at project root
- `.sql` files contain `MODEL (` blocks
- `.py` model files use `@model` decorator

**IronLayer check:**
```bash
ironlayer plan --framework auto .
# Expected: "Framework: SQLMesh | Models: N found"
```

If the project has both `dbt_project.yml` and `config.yaml`, ask the client which framework they intend to use before proceeding.

---

## Step 2: Model Generation

### Template: Staging Model (1:1 source)

```sql
-- models/staging/stg_{source}__{table}.sql
MODEL (
  name {client}_raw.staging.stg_{source}__{table},
  kind VIEW,
  owner '{owner_email}',
  tags ({source}, staging),
  audits (NOT_NULL(columns=(id))),
  -- No cron needed for VIEW kind — always computed on read
);

WITH source AS (
  SELECT * FROM {client}_raw.bronze.{table}
),

cleaned AS (
  SELECT
    id::BIGINT                    AS {entity}_id,
    lower(trim(email))::TEXT      AS email,
    -- Type-cast every column, never SELECT *
    created_at::TIMESTAMP         AS created_at,
    current_timestamp()           AS _loaded_at
  FROM source
  WHERE id IS NOT NULL  -- Only hard filter in staging
)

SELECT * FROM cleaned
```

### Template: Incremental Fact Model (time-based)

```sql
-- models/marts/fct_{event}.sql
MODEL (
  name {client}_prod.gold.fct_{event},
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (event_date, '%Y-%m-%d')
  ),
  owner '{owner_email}',
  cron '@daily',
  partitioned_by (event_date),
  grain ({event_id}),
  tags (finance, incremental),
  audits (
    UNIQUE_VALUES(columns=({event_id})),
    NOT_NULL(columns=({event_id}, event_date))
  ),
);

SELECT
  {event_id}::TEXT                    AS {event}_id,
  user_id::BIGINT                     AS user_id,
  amount_usd::DECIMAL(18, 4)          AS amount_usd,
  event_date::DATE                    AS event_date,
  current_timestamp()                 AS _loaded_at
FROM {client}_raw.staging.stg_{source}__{event_table}
WHERE
  event_date BETWEEN @start_ds AND @end_ds  -- Required for INCREMENTAL_BY_TIME_RANGE
  AND amount_usd > 0
```

### Template: SCD Type 2 Dimension

```sql
-- models/marts/dim_{entity}.sql
MODEL (
  name {client}_prod.gold.dim_{entity},
  kind SCD_TYPE_2 (
    unique_key = {entity}_id,
    valid_from_name = valid_from,
    valid_to_name   = valid_to,
    updated_at_name = updated_at
  ),
  owner '{owner_email}',
  cron '@daily',
  grain ({entity}_id),
  audits (
    UNIQUE_VALUES(columns=({entity}_id, valid_from)),
    NOT_NULL(columns=({entity}_id, status))
  ),
);

SELECT
  {entity}_id::TEXT        AS {entity}_id,
  name::TEXT               AS name,
  status::TEXT             AS status,
  updated_at::TIMESTAMP    AS updated_at,
  current_timestamp()      AS _loaded_at
FROM {client}_raw.staging.stg_{source}__{entity_table}
```

### Template: Python Model (DataFrame transform)

```python
# models/marts/py_{model_name}.py
import typing as t
import pandas as pd
from sqlmesh import ExecutionContext, model

@model(
    "prod_catalog.gold.py_{model_name}",
    kind="FULL",
    owner="{owner_email}",
    columns={
        "id": "bigint",
        "result": "double",
        "processed_at": "timestamp",
    },
)
def execute(context: ExecutionContext, **kwargs: t.Any) -> pd.DataFrame:
    """Python model for complex transformations not expressible in SQL."""
    df = context.fetchdf("SELECT id, value FROM prod_catalog.staging.stg_source__table")
    df["result"] = df["value"].apply(complex_business_logic)
    df["processed_at"] = pd.Timestamp.utcnow()
    return df
```

---

## Step 3: Blueprint Pattern (Multi-Tenant / Multi-Region)

When one logical model needs to be instantiated for multiple clients or regions:

```sql
MODEL (
  name @{client}_prod.gold.fct_revenue,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (event_date, '%Y-%m-%d')
  ),
  owner '{owner_email}',
  cron '@daily',
  partitioned_by (event_date),
  grain (transaction_id),
  blueprints (
    (client := client_alpha),
    (client := client_beta),
    (client := client_gamma)
  ),
);

SELECT
  transaction_id::TEXT                AS transaction_id,
  amount_usd::DECIMAL(18, 4)          AS amount_usd,
  event_date::DATE                    AS event_date,
  '@{client}'::TEXT                   AS client_id
FROM @{client}_raw.staging.stg_payments__transactions
WHERE event_date BETWEEN @start_ds AND @end_ds
```

This generates three separate models at plan time: `client_alpha_prod.gold.fct_revenue`, `client_beta_prod.gold.fct_revenue`, `client_gamma_prod.gold.fct_revenue`.

---

## Step 4: Custom Macros

Place custom macros in `macros/`:

```python
# macros/shared.py
from sqlmesh import macro

@macro()
def safe_divide(evaluator, numerator, denominator):
    """Returns NULL instead of raising on division by zero."""
    return f"CASE WHEN {denominator} = 0 THEN NULL ELSE {numerator} / NULLIF({denominator}, 0) END"

@macro()
def pii_mask(evaluator, column, pii_type):
    """Mask PII columns for non-prod environments."""
    if pii_type == "email":
        return f"CASE WHEN @is_dev THEN CONCAT('masked_', {column}) ELSE {column} END"
    elif pii_type == "phone":
        return f"CASE WHEN @is_dev THEN '***-***-****' ELSE {column} END"
    return column
```

Usage in SQL models:
```sql
SELECT
  @safe_divide(revenue, quantity)          AS unit_price,
  @pii_mask(email, 'email')               AS email
FROM source
```

---

## Step 5: Unit Tests

Create unit test files in `tests/`:

```yaml
# tests/test_{model_name}.yaml
model: {client}_prod.gold.{model_name}

description: "Test {model_name} transforms {source} correctly"

inputs:
  {client}_raw.staging.stg_{source}__{table}:
    rows:
      - {id_col}: "row-1"
        amount: 100.00
        event_date: "2025-01-15"
      - {id_col}: "row-2"
        amount: 0.00
        event_date: "2025-01-15"  # Zero amount — should be filtered

outputs:
  query:
    rows:
      - {id_col}: "row-1"     # Only row-1 passes amount > 0 filter
        amount_usd: 100.0000

overrides:
  start: "2025-01-15"
  end: "2025-01-15"
```

Run with: `sqlmesh test` or `sqlmesh test {model_name}` or `sqlmesh_test` MCP tool.

---

## Step 6: dbt → SQLMesh Conversion

When converting an existing dbt project to SQLMesh:

### Model File Conversion

**dbt staging model:**
```sql
-- dbt: models/staging/stg_payments__transactions.sql
{{ config(materialized='view') }}

with source as (
    select * from {{ source('payments', 'transactions') }}
),
cleaned as (
    select
        id as transaction_id,
        amount,
        created_at::timestamp as event_date
    from source
    where amount > 0
)
select * from cleaned
```

**Equivalent SQLMesh model:**
```sql
-- SQLMesh: models/staging/stg_payments__transactions.sql
MODEL (
  name {client}_raw.staging.stg_payments__transactions,
  kind VIEW,
  owner '{owner_email}',
  audits (NOT_NULL(columns=(transaction_id))),
);

WITH source AS (
  SELECT * FROM {client}_raw.bronze.transactions
),

cleaned AS (
  SELECT
    id::TEXT                AS transaction_id,
    amount::DECIMAL(18, 4)  AS amount,
    created_at::TIMESTAMP   AS event_date
  FROM source
  WHERE amount > 0
)

SELECT * FROM cleaned
```

### Conversion Checklist
- [ ] `{{ config(...) }}` → `MODEL (kind ..., ...)` DDL block
- [ ] `{{ source('schema', 'table') }}` → direct catalog reference `{client}_raw.bronze.table`
- [ ] `{{ ref('model_name') }}` → direct model name reference `schema.model_name`
- [ ] `{{ dbt_utils.surrogate_key([...]) }}` → `hash({col1}, {col2})` or IronLayer macro
- [ ] `{{ dbt_utils.safe_divide(...) }}` → `@safe_divide(...)` macro (create in `macros/`)
- [ ] `{{ config(incremental_strategy='merge', unique_key='id') }}` → `kind INCREMENTAL_BY_UNIQUE_KEY, grain (id)`
- [ ] `{{ config(incremental_strategy='insert_overwrite', partition_by='date') }}` → `kind INCREMENTAL_BY_TIME_RANGE, time_column (date, '%Y-%m-%d')`
- [ ] `{% if is_incremental() %}` blocks → SQLMesh handles this automatically via `@start_ds` / `@end_ds`
- [ ] `dbt test` → `audits` in MODEL DDL + unit tests in `tests/`
- [ ] `dbt source freshness` → SQLMesh `cron` + `CRON_FRESHNESS` audit

---

## Step 7: Plan / Apply Workflow

### Development
```bash
# 1. Make model changes
# 2. Dry-run plan — see what would change
sqlmesh plan dev --no-prompts

# 3. Apply — create Virtual Data Environment
sqlmesh run --environment dev

# 4. Validate
sqlmesh test                  # Unit tests
sqlmesh table_diff prod:dev model_name  # Compare with prod
```

### CI (per PR)
```bash
# Called by GitHub Actions via IronLayer CI/CD
PR_ENV="ci_pr_${PR_NUMBER}"
sqlmesh plan "${PR_ENV}" --no-prompts
sqlmesh run --environment "${PR_ENV}"
sqlmesh test
sqlmesh table_diff "prod:${PR_ENV}" "${CHANGED_MODEL}"

# IronLayer posts PR comment with plan output + table diff
```

### Production Promotion
```bash
sqlmesh plan prod --no-prompts   # Plan pointer swap
sqlmesh run --environment prod   # Apply (near-instant for VDE changes)
```

---

## Step 8: Table Diff for CI Validation

SQLMesh `table_diff` compares data between environments:

```bash
# Compare prod vs CI environment
sqlmesh table_diff prod:ci_pr_42 fct_orders --limit 100

# Output includes:
# - Row count delta
# - Schema diff (added/removed/changed columns)
# - Sample row differences (first 100 changed rows)
# - Aggregate metrics (mean, min, max for numeric columns)
```

**Via MCP tool:**
- Tool: `sqlmesh_table_diff`
- Args: `{ "source_env": "prod", "target_env": "ci_pr_42", "model": "fct_orders", "limit": 100 }`

IronLayer formats the output and posts it to the PR comment alongside the BLOCK/WARN/NOTE code review findings.

---

## Step 9: Framework Recommendation

When a client asks "should I use dbt or SQLMesh?", use this decision matrix:

```
Recommend SQLMesh when:
  ✓ Greenfield project (no existing dbt investment)
  ✓ Multiple target databases or dialects
  ✓ Want free Virtual Data Environments (no Databricks schema costs)
  ✓ Need Python models (DataFrame transforms)
  ✓ Want built-in table diff without additional tooling
  ✓ Want native data contracts and audits
  ✓ Team is open to learning a new framework

Recommend dbt when:
  ✓ Existing dbt project / large dbt investment
  ✓ Using dbt Cloud Enterprise (managed CI/CD, Semantic Layer, dbt Agents)
  ✓ Team already has dbt expertise
  ✓ Need dbt packages ecosystem (dbt_utils, elementary, etc.)
  ✓ Client requires dbt certification or support contracts

Recommend IronLayer-native (raw SQL) when:
  ✓ Simple pipeline (< 20 models)
  ✓ Team prefers full SQL control
  ✓ Migrating from legacy ETL
```

---

## Exodus Project Config Template

When setting up a new SQLMesh project for a client:

```yaml
# config.yaml
gateways:
  databricks:
    connection:
      type: databricks
      server_hostname: "${DATABRICKS_HOST}"
      http_path: "${DATABRICKS_HTTP_PATH}"
      access_token: "${DATABRICKS_TOKEN}"
      catalog: "${CLIENT_ID}_prod"

default_gateway: databricks

model_defaults:
  dialect: databricks
  owner: "data-team@{client}.com"

variables:
  client: "{client_id}"
  environment: dev

format:
  normalize: true
  indent: 2
  max_line_length: 120
```

---

## Key Anti-Patterns (BLOCK)

| Anti-Pattern | Why It's Blocked | Fix |
|-------------|-----------------|-----|
| `SELECT *` in marts | Breaks column lineage | Explicit column list |
| INCREMENTAL_BY_UNIQUE_KEY without `grain` | SQLMesh can't determine merge key | Add `grain (id)` |
| INCREMENTAL_BY_TIME_RANGE without `time_column` | Can't partition incremental loads | Add `time_column (date, '%Y-%m-%d')` |
| Hardcoded connection strings in MODEL DDL | Credentials in code | Use gateway config + env vars |
| No `audits` on INCREMENTAL models | Silent data quality failures | Add `UNIQUE_VALUES` + `NOT_NULL` on grain |
| Python model without type annotations in `columns` | Runtime schema errors | Add `columns={"col": "type"}` to `@model` |
