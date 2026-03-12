---
name: exodus-pr-review
description: >
  Review pull requests against Exodus coding standards, architecture patterns, and data
  warehouse conventions. Produces structured BLOCK/WARN/NOTE output compatible
  with the PRHealingAgent loop. Use when reviewing PRs locally, checking agent-created
  code, or validating changes before merge.
triggers:
  - "review PR #"
  - "review this PR"
  - "check the agent's PR"
  - "is this PR ready to merge"
  - "review before merge"
  - "exodus review"
outputs:
  - Structured review comment with BLOCK/WARN/NOTE findings
  - Confidence score (0-100)
  - TL;DR summary
  - Machine-readable data block for PRHealingAgent
---

# PR Reviewer

> **Core skill.** This review is the quality gate before human review.
> The PRHealingAgent will attempt to auto-fix BLOCK and WARN findings.
> Every finding must be specific and actionable enough for an AI to apply the fix.

---

## Before You Start — Read the PR

```bash
gh pr view <PR_NUMBER>
gh pr diff <PR_NUMBER>
gh pr diff <PR_NUMBER> --name-only
gh pr view <PR_NUMBER> --json body -q '.body'
```

---

## Stage 1 — Triage

1. **Is this an agent PR?** (branch starts with `agent/`)
   - Verify stated confidence and patterns
   - Check PR body for backlog item ID from `implementation_plan/BACKLOG.md`
2. **Which file types changed?** (SQL, Python, YAML, Terraform, DAB)
3. **How large is the PR?** — > 500 lines: WARN (should be split)
4. **Are CI checks failing?** — BLOCK regardless of code content

---

## Stage 2 — Load Standards

```bash
# Always read
cat AGENTS.md

# If SQL/dbt changed (dbt projects)
cat dbt/dbt_project.yml
ls dbt/models/

# If SQLMesh model changed (SQLMesh projects — look for MODEL() DDL blocks)
cat config.yaml 2>/dev/null || true
ls models/ 2>/dev/null || true

# If Python agents changed
head -80 autopilot/agents/base.py

# If Foundation extractors changed
head -80 foundation/extractors/base.py

# If Terraform changed
ls terraform/modules/
cat terraform/modules/_shared/tags.tf 2>/dev/null || true

# If IronLayer changed
head -80 core_engine/models/__init__.py 2>/dev/null || true
```

---

## Stage 3 — File-Type Review Checklist

> **Framework detection:** If changed `.sql` files contain `MODEL (` DDL blocks, the project uses SQLMesh.
> Apply the SQLMesh checklist below instead of the dbt checklists.

### SQL / SQLMesh Models (`MODEL()` DDL)

| Check | Tier |
|-------|------|
| `MODEL()` block missing | BLOCK |
| `name` not fully qualified (`catalog.schema.model`) | BLOCK |
| INCREMENTAL_BY_UNIQUE_KEY without `grain` | BLOCK |
| INCREMENTAL_BY_TIME_RANGE without `time_column` | BLOCK |
| No `audits` on incremental model (min: UNIQUE_VALUES + NOT_NULL on grain) | BLOCK |
| `SELECT *` in a mart or fact model | BLOCK |
| Hardcoded connection string in MODEL DDL | BLOCK |
| `owner` missing from MODEL block | WARN |
| No unit test in `tests/` for model with non-trivial business logic | WARN |
| INCREMENTAL_BY_UNIQUE_KEY with large table and no `partitioned_by` | WARN |
| Python model missing `columns` type annotations in `@model` | WARN |
| Blueprint template variables not consistent with other blueprints | NOTE |

---

### SQL / dbt Staging (`stg_*.sql`)

| Check | Tier |
|-------|------|
| `SELECT *` present | BLOCK |
| Missing `{{ audit_columns() }}` | BLOCK |
| Hardcoded catalog/schema name | BLOCK |
| `materialized` not `view` | BLOCK |
| `WHERE pk IS NOT NULL` missing | BLOCK |
| Division without `{{ safe_divide() }}` | BLOCK |
| `MD5()` instead of `{{ surrogate_key() }}` | BLOCK |
| JOIN in staging model | BLOCK |
| PII column not masked with `{{ pii_mask() }}` | BLOCK |
| Missing schema YAML entry | WARN |
| PK missing `unique` + `not_null` tests | WARN |
| SQL keywords lowercase | WARN |
| String column not trimmed | WARN |
| Column description missing | NOTE |

### SQL / dbt Intermediate (`int_*.sql`)

| Check | Tier |
|-------|------|
| `materialized` not `table` | BLOCK |
| Raw table reference (not `{{ ref() }}`) | BLOCK |
| Division without `{{ safe_divide() }}` | BLOCK |
| Missing grain comment | WARN |

### SQL / dbt Fact (`fct_*.sql`)

| Check | Tier |
|-------|------|
| Not `incremental` with `merge` | BLOCK |
| `MD5()` instead of `{{ surrogate_key() }}` | BLOCK |
| Missing `{% if is_incremental() %}` filter | WARN |
| Missing dim_date join | WARN |

### Python — Autopilot Agents (`autopilot/agents/`)

| Check | Tier |
|-------|------|
| Hardcoded API key/token/password | BLOCK |
| Hard-coded model ID (e.g. `claude-sonnet-4-20250514`) | BLOCK |
| Bare `except:` or `except Exception: pass` | BLOCK |
| `print()` instead of `self.console.print()` | BLOCK |
| LLM call inside `execute()` method | BLOCK |
| Missing type hints on public methods | WARN |
| Missing docstring on class | WARN |
| No unit test for new code | WARN |
| `DEFAULT_MODEL` uses version ID not tier alias | WARN |

### Python — Foundation Extractors (`foundation/extractors/`)

| Check | Tier |
|-------|------|
| Hardcoded API key/token | BLOCK |
| `requests.get()` instead of `self._get()` | BLOCK |
| Missing `self._check_budget()` before API loop | BLOCK |
| Hardcoded catalog/schema name | BLOCK |
| Missing type hints | WARN |
| `print()` instead of `self.console.print()` | WARN |

### Terraform

| Check | Tier |
|-------|------|
| Hardcoded account ID or region | BLOCK |
| `Action: "*"` + `Resource: "*"` in IAM | BLOCK |
| `sensitive = true` missing on sensitive outputs | BLOCK |
| `0.0.0.0/0` ingress without explicit comment | BLOCK |
| `count` instead of `for_each` for named resources | WARN |
| Missing `common_tags` | WARN |
| Module missing README.md | NOTE |

### IronLayer Core Engine (`core_engine/`)

| Check | Tier |
|-------|------|
| LLM call in Layer A (deterministic code) | BLOCK |
| Non-deterministic operation (timestamps, random) in planner | BLOCK |
| Plan output missing content hash | WARN |
| Missing test for new planner logic | WARN |

---

## Stage 4 — Security Audit (Always Run)

```bash
gh pr diff <PR_NUMBER> | grep -iE "(api.?key|secret|password|token)\s*=\s*['\"][^'\"]{8,}"
gh pr diff <PR_NUMBER> | grep -E "AKIA[A-Z0-9]{16}"
gh pr diff <PR_NUMBER> | grep "0\.0\.0\.0/0"
gh pr diff <PR_NUMBER> | grep -E "claude-[a-z]+-[0-9]+-[0-9]+"  # hard-coded model IDs
```

Any match = BLOCK regardless of other findings.

---

## Stage 5 — Agent PR Validation (`agent/*` branches only)

1. Read the backlog item ID from the PR body
2. Check `implementation_plan/BACKLOG.md` for that task
3. Do files changed match the task's scope?
4. Are acceptance criteria genuinely met?
5. Did the agent reference paths that actually exist?
6. Did the agent use macros/functions defined in this project?

---

## Stage 6 — Produce Review

**Required format** — PRHealingAgent parses this exactly:

```markdown
## AI Code Review

> **{DECISION}** · Confidence: {N}/100 · ~{M} min human review

**TL;DR:** {One sentence: what changed, overall quality, main concern.}

---

### 🔴 BLOCK ({N} findings — must fix before merge)

#### `{file/path.sql}`

- 🔴 **BLOCK** [{location}] **{Short title}**
  {What is wrong and why it matters.}
  _Fix: {Exact instruction — specific enough for an AI to apply without ambiguity.}_

### 🟡 WARN ({N} findings — should fix)

#### `{file/path.py}`

- 🟡 **WARN** [{location}] **{Short title}**
  {Description.}
  _Fix: {Instruction.}_

### 🟢 NOTE ({N} — suggestions)

- 🟢 **NOTE** [{location}] **{Short title}**
  {Suggestion.}

---

### Verdict

{APPROVE | REQUEST_CHANGES | COMMENT}

{1-2 sentences explaining the verdict.}

<!-- review-data: {"decision":"{DECISION}","confidence":{N},"findings":[...]} -->
```

**The `<!-- review-data: ... -->` comment is machine-readable and required.**

---

## Confidence Score

| Score | Meaning |
|-------|---------|
| 90-100 | Very confident |
| 70-89 | Confident — some ambiguity |
| 50-69 | Moderate — domain context needed |
| < 50 | Low — recommend thorough human review |

---

## Verdict Decision Tree

```
Any BLOCK?             → REQUEST_CHANGES (decision: BLOCK)
Any WARN?              → REQUEST_CHANGES (decision: WARN)
Only NOTE, conf ≥ 85?  → APPROVE
Only NOTE, conf < 85?  → COMMENT
No findings, conf ≥ 75? → APPROVE
No findings, conf < 75? → COMMENT
```

---

## Guardrails

- Never approve automatically — always produce a review for human decision
- Always run security audit regardless of PR size
- Fix instructions must be unambiguous — PRHealingAgent applies them literally
- One finding per bullet — never combine issues
- Group by file — PRHealingAgent uses this structure
