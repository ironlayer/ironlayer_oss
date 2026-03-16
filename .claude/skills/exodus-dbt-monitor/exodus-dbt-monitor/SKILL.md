---
name: exodus-dbt-monitor
description: >
  Monitor dbt Cloud jobs, check run status, detect failures, and surface actionable diagnostics.
  Use when checking job health, source freshness, model health, or generating a status report.
triggers:
  - "monitor dbt jobs"
  - "check dbt Cloud failures"
  - "source freshness status"
  - "model health"
  - "dbt job status report"
  - "what failed in dbt"
  - "stale sources"
outputs:
  - "Summary report (failed runs, stale sources, unhealthy models)"
  - "Remediation recommendations"
---

# dbt Monitor — Job Status, Failures, Health

> Monitor dbt Cloud jobs, detect failures, check source freshness, and surface model health.
> For dbt Core projects, use `dbt_source_freshness` and local logs; for dbt Cloud, use the dbt-cloud MCP.

---

## MCP Tools Used

| Tool | Server | Purpose |
|------|--------|---------|
| `list_jobs` | dbt-cloud | All jobs in account |
| `list_jobs_runs` | dbt-cloud | Recent runs per job |
| `get_job_run_details` | dbt-cloud | Run status, error logs, failed models |
| `get_model_health` | dbt-cloud | Test pass/fail rates, last run |
| `get_all_sources` | dbt-cloud | Source definitions |
| `dbt_source_freshness` | dbt | Freshness (dbt Core only) |

---

## Step 1 — List Jobs and Recent Runs

```
1. Call dbt-cloud MCP: list_jobs
   → Get job IDs and names

2. For each production/CI job, call: list_jobs_runs
   → Filter for status: failed, error, cancelled
   → Capture run_id for failed runs
```

---

## Step 2 — Get Failure Details

For each failed run:

```
Call dbt-cloud MCP: get_job_run_details
Arguments: run_id

Extract:
- Error message
- Failed model names
- Test failures (if any)
- Compilation errors (if any)
```

---

## Step 3 — Source Freshness

**dbt Cloud:**
```
Call dbt-cloud MCP: get_all_sources
→ Check freshness status from Discovery API
```

**dbt Core:**
```
Call dbt MCP: dbt_source_freshness
→ Parse output for warn/error status
```

---

## Step 4 — Model Health

```
Call dbt-cloud MCP: get_model_health
→ For critical models, check test pass rate and last successful run
```

---

## Step 5 — Generate Report

Output format:

```markdown
## dbt Monitor Report — {date}

### Failed Runs
| Job | Run ID | Status | Error |
|-----|--------|--------|-------|
| prod_full_refresh | 12345 | error | Compilation error in stg_orders |

### Stale Sources
| Source | Table | Status | Last Load |
|--------|-------|--------|-----------|

### Unhealthy Models
| Model | Test Pass Rate | Last Success |
|-------|----------------|--------------|

### Recommendations
- [Actionable remediation steps based on error patterns]
```

---

## Error Pattern → Remediation

| Pattern | Recommendation |
|---------|----------------|
| Compilation error | Check refs, schema YAML; use exodus-dbt-debug |
| Test failure | Identify test, check data; adjust threshold or fix model |
| Runtime (Spark/Databricks) | Parse error code; check SQL, incremental config |
| Source freshness warn | Check extractor; adjust freshness threshold |
| Timeout | Increase job timeout; optimize long-running models |
