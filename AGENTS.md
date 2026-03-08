# AGENTS.md — Exodus / Iron Layer Platform

This file provides AI coding agents (Cursor, GitHub Copilot, Claude Code, etc.) with workspace-level context.
Individual repos have extended AGENTS.md files with repo-specific details.

---

## Work Tracking

**All work — including AI-agent work — must be tracked before starting.**

1. Open `ironlayer_infra/BACKLOG.md` (private repo — canonical backlog).
2. Find the item matching your task. If it doesn't exist, add it first.
3. Change its status to `[IN-PROGRESS]` with today's date before writing any code.
4. Only then begin implementation.

See `ironlayer_infra/CLAUDE.md` for the full rule set (dependency verification,
no-stubs rule, verification suite requirements, lessons learned update policy).

OSS repo work (`ironlayer_OSS`) is tracked in the **same** private backlog —
no changes to the public repo without a corresponding `[IN-PROGRESS]` item.

---

## Project Overview

Exodus is an AI-native data platform company with three products:

| Product | Repo | Role |
|---|---|---|
| **IronLayer** | `ironlayer_oss` (public) | SQL control plane — plan, lineage, cost modeling |
| **Exodus Foundation** | `ironlayer_infra` | Turnkey Databricks platform — Terraform + dbt/SQLMesh + extractors |
| **Exodus Autopilot** | `exodus-autopilot` | Agent fleet — GitHub App, AI PR review, self-healing |
| **Shared Kernel** | `exodus-core` | auth, llm, security, memory, mcp, config |

All repos live under `~/Documents/production_code/` as siblings (not submodules).

---

## Build and Test Commands

```bash
# Activate environment (always use this)
source activate.sh && source ~/.exodus/env.local

# Run tests (any Python repo)
uv run pytest tests/ -v

# Lint Python
uv run ruff check .
uv run mypy . --ignore-missing-imports

# Lint SQL (ironlayer_infra)
sqlfluff lint dbt-framework/models/ --dialect databricks

# Lint Terraform
terraform fmt -check -recursive terraform/

# Pre-commit (all repos)
pre-commit run --all-files

# IronLayer CLI
ironlayer plan    # Validated execution plan
ironlayer diff    # Schema/data diff
ironlayer status  # System health

# Autopilot CLI
exodus review code <path>   # AI code review
exodus agent run --next     # Run highest-priority backlog item
exodus ai-usage             # Check AI spend
```

---

## Architecture Decisions

1. **Shared kernel** — `exodus-core` for all shared concerns; product repos own product logic only
2. **`for_each` over `count`** — Terraform resources use `for_each` with named maps
3. **Medallion architecture** — Bronze → Silver → Gold
4. **PEVR agent loop** — `plan()` → `execute()` → `verify()` → `replan()` (max 3 iterations)
5. **Two-pass review** — local Ollama pass 1, conditional cloud pass 2
6. **Budget-protected AI** — All calls through `exodus-core` LLM router
7. **Everything Terraform** — All repos and org settings managed via `exodus-terraform`

---

## Coding Conventions

### Python
- Type hints required, docstrings on public methods
- Agents inherit from `exodus_core.agents.base.BaseAgent`
- `rich` for CLI output, `click`/`typer` for CLI structure
- `structlog` for structured logging
- Never hardcode credentials — read from `os.environ` or `~/.exodus/env.local`
- `uv run` for all commands

### Terraform
- `for_each` with named maps — `count` only for boolean toggles
- Tag all resources with `common_tags`
- S3 backend + DynamoDB locking

### SQL / dbt / SQLMesh
- UPPERCASE keywords, CTE pattern
- **dbt:** `{{ surrogate_key([...]) }}` — never MD5; every model needs YAML schema file
- **SQLMesh:** `MODEL()` DDL block; grain + audits on incremental models; see `sqlmesh.mdc` rule
- Clients choose dbt Core, dbt Cloud, or SQLMesh — IronLayer is framework-agnostic

### Git
```
type(scope): description
Types: feat, fix, refactor, test, docs, ci, chore, perf
Scopes: core, llm, plan, lineage, terraform, dbt, sqlmesh, agents, github-app, cicd, ai, standards
```

---

## Credentials

Stored in `~/.exodus/env.local`. Template at `iron-layer-dev-workspace/tooling/env/env.template`.

```bash
ANTHROPIC_API_KEY, DATABRICKS_HOST, DATABRICKS_TOKEN, GITHUB_TOKEN
GITHUB_APP_ID, GITHUB_APP_PRIVATE_KEY, GITHUB_WEBHOOK_SECRET
STRIPE_SECRET_KEY, EXODUS_BUDGET_LIMIT, OLLAMA_BASE_URL
```

---

## AI Budget Rules

- Worth spending on: complex reviews, architecture decisions, initial code gen, Terraform risk analysis
- Use local first: `EXODUS_USE_LOCAL_LLMS=true` routes economy/standard tier to Mac Studio Ollama
- Track spend: `exodus ai-usage`
- Budget gate: `EXODUS_BUDGET_LIMIT` env var (default $15/session)
