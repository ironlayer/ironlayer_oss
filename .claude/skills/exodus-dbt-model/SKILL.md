---
name: exodus-dbt-model
description: >
  Generate dbt Core models following Foundation medallion architecture (staging/intermediate/marts),
  Kimball conventions, and the project's naming patterns. Use when creating new dbt models for
  Exodus-internal projects (Foundation, ironlayer_infra, client templates).
  For dbt Cloud Enterprise client work, use the exodus-dbt-cloud skill instead.
triggers:
  - "create a dbt model for"
  - "add staging model for"
  - "build a dimension for"
  - "create a fact table for"
  - "add intermediate model"
  - "transform {source} data"
outputs:
  - "dbt/models/{layer}/{domain}/*.sql"
  - "dbt/models/{layer}/{domain}/_*.yml"
---

# dbt Core Model Generator

> **This skill is for dbt Core projects** — Exodus Foundation (`exodus-foundation/dbt`),
> ironlayer_infra (`dbt-framework/`), and client templates (`exodus-client-template/dbt-framework/`).
>
> **For dbt Cloud Enterprise client work** (Remote MCP, Semantic Layer, Discovery API, job triggers),
> use the `exodus-dbt-cloud` skill instead.
>
> **For SQLMesh projects** (clients who have chosen SQLMesh as their transformation framework),
> use the `exodus-sqlmesh` skill instead. SQLMesh uses `MODEL()` DDL blocks, not `{{ config() }}`.

---

## Shortcut: Use dbt MCP Codegen First

Before writing models from scratch, try the dbt MCP codegen tools (available in Cursor via the `dbt` MCP server):

```
# From an existing raw table:
1. generate_source     → scaffolds sources.yml with table schema from the warehouse
2. generate_staging_model → scaffolds stg_{source}__{table}.sql from source definition
3. generate_model_yaml → scaffolds YAML companion with all columns pre-populated

# Then manually:
4. Add business logic, type casts, and rename columns in the staging SQL
5. Add test assertions (unique, not_null, accepted_values) to the YAML
6. Write the intermediate/mart model building on the staging layer
```

Only fall back to the full manual workflow below when codegen tools are unavailable
(e.g., no warehouse connection, or working on a model with complex business logic).

---

---

## Before You Start — Read Existing Patterns

```bash
# 1. Project structure
ls dbt/models/staging/
ls dbt/models/marts/
cat dbt/dbt_project.yml

# 2. Existing models in the same domain
ls dbt/models/staging/{closest_domain}/
cat dbt/models/staging/{closest_domain}/_*.yml

# 3. Macros available
ls dbt/macros/
```

---

## Layer Naming and Materialization

| Layer | Schema | Materialization | Pattern |
|-------|--------|----------------|---------|
| Staging | silver (views on bronze) | **view** | `stg_{source}__{table}.sql` |
| Intermediate | silver | **table** | `int_{domain}__{description}.sql` |
| Dimension | gold | **incremental** (merge) | `dim_{entity}.sql` |
| Fact | gold | **incremental** (merge) | `fct_{event}.sql` |

---

## Step 1 — Staging Model (`dbt/models/staging/{source}/stg_{source}__{table}.sql`)

```sql
{{
    config(
        materialized='view',
        schema='silver',
        tags=['silver', '{source}']
    )
}}

with

source as (
    select * from {{ source('{source}', '{raw_table}') }}
),

renamed as (
    select
        -- primary key
        {pk_column}                              as {entity}_id,

        -- dimensions
        lower(trim({email_col}))                 as email,

        -- measures
        cast({amount_col} as decimal(18,6))      as amount_usd,

        -- audit
        {created_at_col}                         as created_at,
        {{ audit_columns() }}

    from source
    where {pk_column} is not null
)

select * from renamed
```

**Staging checklist — BLOCK if any missing:**
- [ ] `materialized='view'` — never table at staging
- [ ] Uses `{{ source() }}` — never raw table reference
- [ ] Has `WHERE {pk} IS NOT NULL` filter
- [ ] `{{ audit_columns() }}` macro included
- [ ] No joins — staging is 1:1 with source
- [ ] No business logic — only type casts, renames, nullability

---

## Step 2 — Source YAML (`dbt/models/staging/{source}/__sources.yml`)

```yaml
version: 2

sources:
  - name: {source}
    database: "{{ env_var('FOUNDATION_RAW_CATALOG', 'foundation_raw') }}"
    schema: {source}
    freshness:
      warn_after: {count: 24, period: hour}
      error_after: {count: 48, period: hour}
    loaded_at_field: _loaded_at
    tables:
      - name: {raw_table}
        description: "Raw {source} {table} data loaded by the {source} extractor"
```

---

## Step 3 — Model YAML (`dbt/models/staging/{source}/_stg_{source}__models.yml`)

```yaml
version: 2

models:
  - name: stg_{source}__{table}
    description: "One row per {entity} from {source}. Cleaned, renamed, typed."
    columns:
      - name: {entity}_id
        description: "Business key — {source} native ID"
        tests:
          - unique
          - not_null
      - name: email
        description: "Normalized (lowercase, trimmed) email"
        tests:
          - not_null
      - name: amount_usd
        description: "Transaction amount in USD"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
```

---

## Step 4 — Intermediate Model (`dbt/models/marts/intermediate/int_{domain}__{desc}.sql`)

```sql
{{
    config(
        materialized='table',
        schema='silver',
        tags=['silver', '{domain}']
    )
}}

{# Grain: one row per {entity} per {period} #}

with

orders as (
    select * from {{ ref('stg_{source}__orders') }}
),

customers as (
    select * from {{ ref('stg_{source}__customers') }}
),

joined as (
    select
        {{ surrogate_key(['o.order_id', 'o.order_date']) }}  as order_key,
        o.order_id,
        c.customer_id,
        c.email,
        o.amount_usd,
        o.created_at
    from orders o
    left join customers c using (customer_id)
)

select * from joined
```

---

## Step 5 — Mart Models (Dimension / Fact)

### Dimension

```sql
{{
    config(
        materialized='incremental',
        unique_key='customer_key',
        incremental_strategy='merge',
        schema='gold',
        tags=['gold', '{domain}']
    )
}}

with

source as (
    select * from {{ ref('int_{domain}__customers') }}
    {% if is_incremental() %}
        where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

final as (
    select
        {{ surrogate_key(['customer_id']) }}  as customer_key,
        customer_id,
        email,
        created_at,
        updated_at
    from source
)

select * from final
```

### Fact

```sql
{{
    config(
        materialized='incremental',
        unique_key='order_key',
        incremental_strategy='merge',
        schema='gold',
        tags=['gold', '{domain}']
    )
}}

{# Grain: one row per order #}

with

orders as (
    select * from {{ ref('int_{domain}__orders') }}
    {% if is_incremental() %}
        where order_created_at > (select max(order_created_at) from {{ this }})
    {% endif %}
),

dim_date as (
    select * from {{ ref('dim_date') }}
),

final as (
    select
        {{ surrogate_key(['order_id']) }}    as order_key,
        coalesce(d.date_key, -1)             as order_date_key,
        order_id,
        customer_id,
        {{ safe_divide('amount_usd', 'exchange_rate') }} as amount_base_currency,
        o.order_created_at
    from orders o
    left join dim_date d on d.calendar_date = date(o.order_created_at)
)

select * from final
```

---

## Step 6 — Bundle Toggle (Foundation-Specific)

If this model belongs to a bundle, wrap with enabled flag:

```yaml
# in the model YAML
models:
  - name: stg_coinbase__candles
    config:
      enabled: "{{ var('bundle_crypto', false) }}"
```

---

## Step 7 — Verify

```bash
cd dbt
dbt compile --select stg_{source}__{table}+
dbt test --select stg_{source}__{table}+
dbt run --select stg_{source}__{table}+ --target dev
```

All tests must pass before opening a PR.
