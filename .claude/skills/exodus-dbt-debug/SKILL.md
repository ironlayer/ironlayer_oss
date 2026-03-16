---
name: exodus-dbt-debug
description: >
  Diagnose and fix dbt errors — compilation failures, test failures, runtime errors on Databricks.
  Use when a dbt run failed and you need to identify root cause and suggest fixes.
triggers:
  - "fix dbt error"
  - "diagnose dbt failure"
  - "dbt compilation error"
  - "dbt test failed"
  - "why did dbt fail"
  - "debug dbt run"
outputs:
  - "Root cause analysis"
  - "Suggested fixes (SQL, schema, config)"
---

# dbt Debug — Diagnose and Fix Errors

> Parse dbt error output, look up failing models, check lineage, and suggest fixes.
> Works for both dbt Core (local logs) and dbt Cloud (run details via API).

---

## MCP Tools Used

| Tool | Server | Purpose |
|------|--------|---------|
| `get_job_run_details` | dbt-cloud | Error logs, failed model (dbt Cloud) |
| `dbt_compile` | dbt | Reproduce compilation (dbt Core) |
| `dbt_test` | dbt | Reproduce test failure (dbt Core) |
| `get_model_details` | dbt | Failing model SQL and config |
| `get_lineage` | dbt | Upstream dependencies |
| `ironlayer_diff` | exodus-core | Semantic SQL diff |
| `ironlayer_validate` | exodus-core | Schema contract validation |

---

## Step 1 — Get Error Context

**dbt Cloud:**
```
Call dbt-cloud MCP: get_job_run_details
Arguments: run_id (from failed run)

Extract:
- Error type (compilation, runtime, test)
- Failing model name
- Full error message
- Stack trace (if available)
```

**dbt Core:**
```
Read local dbt logs or run:
  dbt compile --select {failing_model}
  dbt test --select {failing_model}
Parse error output for model name and message.
```

---

## Step 2 — Inspect Failing Model

```
Call dbt MCP: get_model_details
Arguments: model_name (or model unique_id)

Review:
- SQL
- Schema YAML (columns, tests)
- Config (materialization, incremental key)
```

---

## Step 3 — Check Upstream

```
Call dbt MCP: get_lineage
Arguments: model_name

Identify:
- Upstream models (refs)
- Source tables
- Missing or broken refs
```

---

## Step 4 — Cross-Reference Error Patterns

| Error Type | Common Causes | Fix |
|------------|---------------|-----|
| Compilation: ref not found | Typo, wrong schema | Fix ref() or source() |
| Compilation: column not found | Schema drift | Update model or source YAML |
| Runtime: division by zero | Raw division | Use {{ safe_divide() }} |
| Runtime: duplicate key | Incremental merge key wrong | Fix unique_key |
| Test: not_null failed | Data quality issue | Fix model or adjust test |
| Test: relationship failed | FK violation | Fix join or test |

---

## Step 5 — Validate Schema (IronLayer)

```
Call exodus-core MCP: ironlayer_validate
Arguments: project_path

Check for:
- Contract violations
- Schema drift vs expected
```

---

## Step 6 — Suggest Fix

Output format:

```markdown
## dbt Debug — {model_name}

### Error
{error_message}

### Root Cause
{explanation}

### Suggested Fix
1. [Specific change to SQL or YAML]
2. [If applicable, upstream fix]

### Verification
- Run: dbt compile --select {model}
- Run: dbt run --select {model}
```
