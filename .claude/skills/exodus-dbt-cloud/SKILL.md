---
name: exodus-dbt-cloud
description: >
  Manage, review, and integrate with a client's dbt Cloud Enterprise environment via Exodus consulting.
  Covers Remote MCP setup, Semantic Layer queries, Discovery API, job management, and CI/CD integration.
  Use when working with a client on dbt Cloud, not for Exodus-internal dbt Core projects.
triggers:
  - "connect to dbt Cloud"
  - "query client metrics"
  - "check client dbt jobs"
  - "review client dbt models"
  - "client dbt Cloud environment"
  - "trigger dbt Cloud job"
  - "dbt Semantic Layer"
  - "dbt Discovery API"
outputs:
  - "Client environment audit report"
  - "Metric definitions and health"
  - "Model lineage and health dashboard"
  - "CI/CD integration configuration"
---

# dbt Cloud Enterprise — Client Consulting Skill

> **This skill is for client dbt Cloud Enterprise environments.**
> For Exodus-internal dbt Core projects (Foundation, ironlayer_infra), use `exodus-dbt-model`.
> For clients using SQLMesh (not dbt Cloud) as their transformation framework, use `exodus-sqlmesh` instead.

---

## Context: What Is Different About dbt Cloud Engagements

| Dimension | Exodus Internal (dbt Core) | Client Consulting (dbt Cloud Enterprise) |
|-----------|---------------------------|------------------------------------------|
| Execution | Local CLI (`dbt run`, `dbt build`) | dbt Cloud Jobs (no local CLI) |
| Metadata | Local manifest.json | Discovery API (Remote MCP) |
| Metrics | Not implemented yet | dbt Semantic Layer |
| Lineage | Local DAG | Discovery API lineage graph |
| CI | GitHub Actions + Databricks job compute | dbt Cloud CI Jobs (Slim CI) |
| Review | Exodus PR engine + local compile | Exodus PR engine + Remote MCP metadata |
| AI agents | Cursor subagents | dbt Analyst/Discovery Agents + Cursor |

---

## Before You Start — Connect to the Client's Remote MCP

Verify the `dbt-cloud` MCP server is configured with the client's credentials:

```bash
# Check env vars are set
echo $DBT_CLOUD_HOST
echo $DBT_CLOUD_ACCOUNT_ID
echo $DBT_CLOUD_PROD_ENV_ID

# Test connection via MCP tool (in Cursor)
# Use: get_all_models (via dbt-cloud MCP server)
# Expected: list of all production models with schema and test status
```

If vars are missing, add them to `~/.exodus/env.local`:
```bash
export DBT_CLOUD_HOST="https://<account>.us1.dbt.com"
export DBT_CLOUD_TOKEN="<service-token>"
export DBT_CLOUD_ACCOUNT_ID="<account-id>"
export DBT_CLOUD_PROD_ENV_ID="<prod-environment-id>"
export DBT_CLOUD_DEV_ENV_ID="<dev-environment-id>"
```

---

## Step 1 — Environment Audit (First Engagement)

Run a full environment audit on first engagement with a new client:

```
Use dbt-cloud MCP tools in this order:

1. get_all_models
   → Count total models by layer (staging, intermediate, marts)
   → Flag models with no tests
   → Flag models with failed tests

2. get_all_sources
   → Check source freshness status
   → Flag sources with warn/error freshness

3. list_metrics
   → Inventory all Semantic Layer metrics
   → Note metrics missing descriptions or filters

4. list_jobs
   → Identify production job(s) and CI job(s)
   → Note schedule, last run status, and duration

5. get_model_health (for top 10 most-referenced models)
   → Test pass/fail rates
   → Last successful run timestamp
```

**Output format:**
```markdown
## Client dbt Cloud Environment Audit — {client_name}

### Environment
- Account: {account_id}
- Host: {host}
- Production environment: {env_id}

### Model Inventory
| Layer | Count | Tested | Failing |
|-------|-------|--------|---------|
| Staging | N | N | N |
| Intermediate | N | N | N |
| Marts | N | N | N |

### Source Freshness
| Source | Status | Last Load | Configured SLA |
|--------|--------|-----------|----------------|

### Metric Inventory
- Total metrics: N
- With descriptions: N
- With filters: N

### Jobs
| Job Name | Schedule | Last Status | Avg Duration |
|----------|----------|-------------|-------------|

### Top Risks
- [BLOCK/WARN] {finding}
```

---

## Step 2 — PR Review in Cloud Mode

When reviewing a client's PR, use Discovery API — never local dbt CLI:

```bash
# DO NOT run these against client environment:
# dbt compile, dbt run, dbt test (requires local profile/credentials)

# INSTEAD, use dbt-cloud MCP tools:
# get_model_details — get schema and columns for any changed model
# get_lineage — trace downstream impact of changes
# get_model_health — check current test pass/fail before/after
```

**PR Review workflow:**
1. Read the PR diff (via GitHub MCP)
2. For each changed `.sql` or `.yml` file, call `get_model_details` (not compile)
3. Call `get_lineage` on changed models to identify downstream impact
4. Apply review checklist (see below)
5. If structural changes, call `trigger_job_run` on the CI job
6. Poll `get_job_run_details` until complete
7. Call `get_model_health` to compare before/after test results
8. Post structured findings as GitHub Review

**Review checklist (Cloud mode):**
- [ ] Naming follows client conventions (check `clients/{client}/client-conventions.mdc`)
- [ ] Changed models have YAML companions with descriptions and tests
- [ ] Metric changes: call `list_metrics` to verify metric is still valid after SQL change
- [ ] Lineage: check `get_lineage` for downstream dashboards or exposures
- [ ] No hardcoded `database.schema.table` references (use `ref()` and `source()`)
- [ ] No `limit` clauses in production models
- [ ] Incremental models use `merge` strategy with a stable `unique_key`

---

## Step 3 — Semantic Layer Queries

Use the Semantic Layer when a client asks a business question about their data:

```
# Example: "What is our ARR by country for the last 6 months?"

1. list_metrics → find "arr" or "revenue" metric
2. get_dimensions → find "customer__country" dimension
3. query_metrics:
   - metric: arr
   - grain: month
   - dimensions: [customer__country]
   - where: metric_time >= '2025-09-01'
```

**When to use Semantic Layer vs direct SQL:**
- Use Semantic Layer: for any business KPI (revenue, retention, churn, LTV)
- Use execute_sql: for exploratory data queries, schema inspection, debugging
- Never use execute_sql for KPIs that have metric definitions — always go through Semantic Layer to ensure consistency

---

## Step 4 — CI/CD Integration (Job Trigger Pattern)

When Exodus needs to trigger a dbt Cloud job as part of PR review:

```python
# Pattern for exodus-autopilot/agents/dbt_cloud_runner.py

async def trigger_ci_run(client_id: str, pr_number: int) -> dict:
    """Trigger a dbt Cloud Slim CI job for a PR and return run details."""
    job_name = f"{client_id}_slim_ci"

    # 1. Find the CI job
    jobs = await mcp_dbt_cloud("list_jobs")
    ci_job = next(j for j in jobs if j["name"] == job_name)

    # 2. Trigger the run
    run = await mcp_dbt_cloud("trigger_job_run", {
        "job_id": ci_job["id"],
        "cause": f"Exodus PR Review — PR #{pr_number}"
    })

    # 3. Poll until complete (max 10 min)
    for _ in range(60):  # 60 x 10s = 10 min
        details = await mcp_dbt_cloud("get_job_run_details", {"run_id": run["id"]})
        if details["status"] in ("Success", "Error", "Cancelled"):
            return details
        await asyncio.sleep(10)

    return {"status": "Timeout", "run_id": run["id"]}
```

---

## Step 5 — Client Handoff Documentation

After an engagement, create a client handoff document:

```markdown
# dbt Cloud Environment — {client_name} Handoff

## Access
- dbt Cloud URL: https://{account}.us1.dbt.com
- Account ID: {account_id}
- Service token type: {scope} (expires: {date})
- Responsible contact at client: {name}

## Environment IDs
- Production: {prod_env_id}
- Development/CI: {dev_env_id}

## Jobs
| Name | Schedule | Purpose |
|------|----------|---------|
| {name} | {schedule} | Production daily build |
| {name} | PR trigger | Slim CI for PR review |

## Metrics (Top 5 by usage)
| Metric | Definition | Owner |
|--------|-----------|-------|
| {metric} | {definition} | {owner} |

## Known Issues / Tech Debt
- {issue}

## Exodus Integration Status
- [ ] Remote MCP connected
- [ ] PR review engine configured
- [ ] CI job integration active
- [ ] Semantic Layer queries tested
- [ ] Data diff integration (Epic 23.5)
```

---

## Key Differences Cheat Sheet

| Action | dbt Core (Internal) | dbt Cloud (Client) |
|--------|--------------------|--------------------|
| Compile a model | `dbt_compile --select model` | `get_model_details` |
| Check test results | `dbt_test --select model` | `get_model_health` |
| View lineage | `get_lineage` (local manifest) | `get_lineage` (Discovery API) |
| Query data | `dbt_show --select model` | `execute_sql` |
| Run a build | `dbt_build --select model` | `trigger_job_run` |
| Check source freshness | `dbt_source_freshness` | `get_all_sources` |
| Generate model YAML | `generate_model_yaml` (codegen) | Not available (no local project) |
