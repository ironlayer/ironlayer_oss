---
name: exodus-kimball-design
description: >
  Design dimensional models (star schemas) following Kimball methodology for the Exodus
  Foundation data warehouse. Use when designing new data domains, creating fact and
  dimension tables, planning grain decisions, or evaluating slowly changing dimensions.
triggers:
  - "design a star schema"
  - "dimensional model for"
  - "kimball design"
  - "fact table for"
  - "dimension table for"
  - "grain decision"
  - "SCD strategy"
outputs:
  - Star schema design document
  - Grain statement and dimensional rationale
  - SCD strategy per dimension
  - dbt model skeleton list
---

# Kimball Dimensional Design

Design dimensional models for Foundation data warehouse domains, producing a star schema
spec that maps directly to dbt mart models.

---

## Step 1 — Business Question First

Before designing, answer:

1. **What business question does this mart answer?**
   - "Revenue by customer segment over time"
   - "Pipeline conversion rates by rep and product"
2. **Who are the primary consumers?**
   - Analysts (SQL), dashboards (Tableau/PowerBI), Foundation Studio (NL→SQL)
3. **What is the reporting cadence?**
   - Daily incremental is the standard for Foundation marts

---

## Step 2 — Grain Statement

The most critical design decision. Write it as one sentence:

```
The grain of {fct_table_name} is one row per {business event} per {time period}.
```

**Examples:**
- "One row per order per day"
- "One row per user session per session"
- "One row per candle per product per 1-minute interval"

**Common mistake:** Mixing grains. Each fact table must have exactly one grain.

---

## Step 3 — Star Schema Design

Produce this design document:

```markdown
## {Domain} Star Schema

### Grain
One row per {event} per {period}.

### Fact Table: fct_{event}

| Column | Type | Description | Source |
|--------|------|-------------|--------|
| {event}_key | STRING | Surrogate key (surrogate_key macro) | derived |
| customer_key | STRING | FK to dim_customer | stg_orders |
| date_key | INTEGER | FK to dim_date | derived |
| {measure_1} | DECIMAL(18,6) | {description} | stg_orders |
| {measure_2} | INTEGER | {description} | stg_orders |
| event_timestamp | TIMESTAMP | When the event occurred | stg_orders |

### Dimensions

#### dim_customer
| Column | Type | SCD Type |
|--------|------|---------|
| customer_key | STRING | SCD2 |
| customer_id | STRING | N/A (business key) |
| email | STRING | SCD2 (changes tracked) |
| segment | STRING | SCD2 |
| valid_from | TIMESTAMP | SCD2 |
| valid_to | TIMESTAMP | SCD2 |
| is_current | BOOLEAN | SCD2 |

#### dim_date (shared, pre-built)
Already exists as `dim_date`. Join using `date(timestamp_column)`.

### Measures
| Measure | Aggregation | Business Definition |
|---------|------------|---------------------|
| revenue_usd | SUM | Net revenue after refunds, in USD |
| order_count | COUNT_DISTINCT | Distinct completed orders |
| avg_order_value | AVG | Mean order value |

### Additive vs Semi-Additive vs Non-Additive
- **Fully additive** (sum across all dims): revenue_usd, order_count
- **Semi-additive** (sum across some dims): customer_balance (not additive over time)
- **Non-additive** (never sum): avg_order_value, conversion_rate (calculate from components)
```

---

## Step 4 — SCD Strategy by Dimension

| SCD Type | Use When | dbt Implementation |
|----------|----------|-------------------|
| **SCD1** (overwrite) | Value changes but history unimportant (email, phone) | `unique_key` merge |
| **SCD2** (track history) | History matters (customer segment, plan) | `valid_from / valid_to / is_current` |
| **SCD3** (previous value only) | Only need current + previous | Add `prev_{col}` column |
| **SCD4** (history table) | Very frequent changes (status, scores) | Separate `{dim}_history` table |

**Foundation standard:** Use SCD1 by default. Escalate to SCD2 when business asks "What segment was this customer in when they bought X?"

---

## Step 5 — dbt Model List

Output the list of models needed:

```
Staging (new):
  stg_{source}__orders.sql
  stg_{source}__customers.sql

Intermediate:
  int_{domain}__orders_enriched.sql   (join orders + customers)
  int_{domain}__daily_revenue.sql     (aggregate to daily grain)

Marts:
  dim_customer.sql                    (SCD2 — if history required, else SCD1)
  fct_{domain}_revenue.sql            (daily revenue fact)
```

---

## Step 6 — Anti-Patterns to Flag

| Anti-Pattern | Impact | Fix |
|-------------|--------|-----|
| Multiple grains in one fact | Wrong aggregations | Split into separate fact tables |
| Calculated measures (AVG, %) in fact | Can't re-aggregate correctly | Store components (numerator + denominator) |
| Snowflake dimensions (normalized dims) | Complex joins for users | Denormalize into star schema |
| Date stored as STRING | No date arithmetic | Cast to DATE at staging |
| NULL foreign keys without default | Broken aggregations | `COALESCE(date_key, -1)` |
| No `is_current` on SCD2 | Incorrect filter pattern | Always add when tracking history |

---

## SQLMesh Model Kind Mapping (Reference)

When a client uses SQLMesh instead of dbt, map the Kimball model types to SQLMesh model kinds:

| Kimball Model Type | dbt Approach | SQLMesh Model Kind |
|-------------------|-------------|-------------------|
| Staging view | `materialized='view'` | `kind VIEW` |
| Intermediate table | `materialized='table'` | `kind FULL` |
| SCD Type 1 dimension | incremental merge, `unique_key` | `kind INCREMENTAL_BY_UNIQUE_KEY, grain (id)` |
| SCD Type 2 dimension | custom SCD2 with `valid_from/valid_to` | `kind SCD_TYPE_2 (unique_key=id, valid_from_name=valid_from, valid_to_name=valid_to)` |
| Daily fact table | incremental insert_overwrite, `partition_by` | `kind INCREMENTAL_BY_TIME_RANGE (time_column (event_date, '%Y-%m-%d'))` |
| Merge/upsert fact | incremental merge, `unique_key` | `kind INCREMENTAL_BY_UNIQUE_KEY, grain (fact_id)` |
| Seed / lookup | `materialized='seed'` | `kind SEED` |

SQLMesh native SCD2 tracks history automatically — no manual `valid_from/valid_to/is_current` logic needed.
Use `exodus-sqlmesh` skill when generating the actual SQLMesh model files.
