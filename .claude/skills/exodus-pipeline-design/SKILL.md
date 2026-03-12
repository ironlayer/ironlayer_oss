---
name: exodus-pipeline-design
description: >
  Design end-to-end data pipelines for Exodus Foundation. Use when planning data flows
  from source to mart, choosing between pipeline approaches (DLT vs batch), or designing
  orchestration with IronLayer integration.
triggers:
  - "design a pipeline"
  - "data pipeline for"
  - "end-to-end data flow"
  - "ingestion to mart"
  - "pipeline architecture"
outputs:
  - Pipeline design document
  - Technology stack decision
  - Orchestration spec
  - Monitoring plan
---

# Pipeline Designer

Design complete data pipelines from source to analytics-ready models.

---

## Step 1 — Source Classification

Classify the data source before choosing a stack:

| Source Type | Characteristics | Recommended Approach |
|-------------|-----------------|---------------------|
| REST API (polling) | Pull-based, rate-limited | `BaseExtractor` → Auto Loader |
| Webhook / event stream | Push-based, real-time | Event Hub/Kinesis → DLT streaming |
| Database (CDC) | JDBC/ODBC, change detection | DLT APPLY CHANGES / Debezium |
| File drop (S3/ADLS/GCS) | Batch, irregular | Auto Loader → DLT batch |
| SaaS platform (Salesforce, HubSpot) | Meltano Singer taps | Meltano → Auto Loader |

---

## Step 2 — Pipeline Stack Decision

### Stack A — Foundation Native (Preferred)

```
API Source → BaseExtractor (Python) → foundation_raw landing volume
         → Databricks Auto Loader  → Bronze Delta table (streaming)
         → Delta Live Tables       → Silver (DLT views)
         → dbt (batch, triggered)  → Gold (incremental marts)
         → IronLayer plan          → Validate before apply
```

**Use when:** API-based source, new data domain, need CI/CD gate with IronLayer.

### Stack B — Meltano + dbt

```
SaaS Source → Meltano (Singer tap) → foundation_raw landing
            → dbt staging           → Silver views
            → dbt marts             → Gold incremental
```

**Use when:** Source has a well-maintained Singer tap, team familiar with Meltano, no streaming requirement.

### Stack C — DLT Streaming

```
Event Hub / Kinesis / Kafka → DLT Streaming table
                            → DLT Silver aggregation
                            → DLT Gold micro-batch
```

**Use when:** Latency < 1 hour required, event-driven source, streaming semantics needed.

### Stack D — Foundation Native + SQLMesh (Alternative to Stack A)

```
API Source → BaseExtractor (Python) → {client}_raw landing
         → Databricks Auto Loader  → Bronze Delta table
         → SQLMesh staging (VIEW)   → Silver (Virtual Data Environment)
         → SQLMesh marts (INCR.)    → Gold (incremental with built-in state)
         → IronLayer plan           → Validate before apply
         → sqlmesh plan prod        → Pointer swap (near-instant promotion)
```

**Use when:** Greenfield project, client prefers SQLMesh over dbt, or client needs multi-dialect support (e.g., migrating from Redshift to Databricks). SQLMesh Virtual Data Environments eliminate CI schema cloning costs.

---

## Step 3 — Pipeline Design Document

Produce this spec:

```markdown
## {Source Name} Pipeline Design

### Overview
- **Source:** {source system and type}
- **Target:** foundation_raw → foundation_prod.gold.{table}
- **Latency SLA:** {daily / hourly / near-real-time}
- **Bundle:** {crypto | igaming | crm | finance | marketing}
- **Stack:** {A — Foundation Native | B — Meltano | C — DLT Streaming}

### Data Flow Diagram
```
{source_api}
  ↓ BaseExtractor (scheduled daily by Databricks job)
foundation_raw.{source}/{table}/*.json  (Auto Loader landing)
  ↓ Databricks Auto Loader
foundation_raw.{source}.{table}  (Bronze Delta — append-only)
  ↓ dbt staging
foundation_prod.silver.stg_{source}__{table}  (view — rename, type, clean)
  ↓ dbt intermediate
foundation_prod.silver.int_{domain}__{desc}  (table — business joins)
  ↓ dbt mart
foundation_prod.gold.fct_{event}  (incremental merge — Kimball grain)
  ↓ IronLayer plan gate (in CI)
foundation_prod.gold.fct_{event}  (production — post apply)
```

### Orchestration
| Step | Owner | Schedule | SLA |
|------|-------|----------|-----|
| Extract | Databricks Job (`extract_{source}`) | 06:00 UTC daily | < 30 min |
| Auto Loader ingest | Databricks Job (triggered by new files) | Continuous / triggered | < 5 min |
| dbt build | Databricks Job (`dbt_build_silver`) | 07:00 UTC daily | < 20 min |
| dbt mart build | Databricks Job (`dbt_build_gold`) | 07:30 UTC daily | < 30 min |

### Monitoring
- Freshness check: `dbt source freshness --select {source}`
- Row count assertion: `dbt test --select fct_{event} --store-failures`
- Anomaly detection: Foundation golden metrics (`config/client.yml golden_metrics`)
- Autopilot DataStewardAgent: daily scan via MCP `foundation_pipeline_status` tool

### IronLayer Integration
Run before dbt apply in production CI:
```bash
ironlayer plan ./dbt HEAD~1 HEAD --output plan.json
# Human/automated gate reviews plan.json
ironlayer apply plan.json --auto-approve  # only after gate
```
```

---

## Step 4 — Error Handling and Recovery

| Failure Mode | Detection | Recovery |
|-------------|-----------|----------|
| API rate limit | HTTP 429 / budget exceeded | Exponential backoff, retry next window |
| Partial extraction | Missing files in landing | Re-run extractor for date range |
| dbt model failure | `dbt run` non-zero exit | Alert, skip downstream, preserve prod data |
| Schema drift | IronLayer diff detects BREAKING | BLOCK deployment, alert data engineer |
| Databricks job failure | Job run status API | Autopilot ReactiveLoop auto-retries transient |

---

## Step 5 — Databricks Asset Bundle Spec

```yaml
# bundle/resources/{source}_pipeline.yml
resources:
  jobs:
    extract_{source}:
      name: "Foundation Extract — {Source}"
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"
        timezone_id: UTC
      tasks:
        - task_key: extract
          new_cluster:
            num_workers: 2
            spark_version: "15.4.x-scala2.12"
            node_type_id: i3.xlarge
          python_wheel_task:
            package_name: foundation
            entry_point: extract
            parameters: ["--source", "{name}"]
```
