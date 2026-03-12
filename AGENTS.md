# AGENTS.md — Exodus / IronLayer Platform

> **Synced from** `iron-layer-dev-workspace/ai/shared/AGENTS.md` — do not edit in repos.

This file provides AI coding agents (Cursor, GitHub Copilot, Claude Code, etc.) with workspace-level context.
Individual repos have extended AGENTS.md files with repo-specific details.

---

## Project Overview

Exodus is an AI-native data platform company with three products:

| Product | Repo | Role |
|---|---|---|
| **IronLayer** | `ironlayer_oss` (public) | SQL control plane — plan, lineage, cost modeling |
| **Exodus Foundation** | `ironlayer_infra` | Turnkey Databricks platform — Terraform + dbt/SQLMesh + extractors |
| **Exodus Autopilot** | `exodus-autopilot` | Agent fleet — GitHub App, AI PR review, self-healing |
| **Shared Kernel** | `exodus-core` | auth, llm, security, memory, mcp, config |

All repos live as siblings in a shared parent directory (not submodules). `activate.sh` sets `WORKSPACE_ROOT`; paths resolve relative to it.

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
5. **Two-pass review** — local vLLM pass 1, conditional cloud pass 2
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
Types: feat, fix, docs, style, refactor, test, chore, ci, perf
Scopes: core, llm, auth, security, plan, lineage, terraform, dbt, sqlmesh, agents, github-app, cicd, ai, standards, tooling, workspace, config, bundle, monitoring, memory, ironlayer, extractors, cli, docs
```

---

## Credentials

Stored in `~/.exodus/env.local`. Template at `iron-layer-dev-workspace/tooling/env/env.template`.

```bash
ANTHROPIC_API_KEY, DATABRICKS_HOST, DATABRICKS_TOKEN, GITHUB_TOKEN
GITHUB_APP_ID, GITHUB_APP_PRIVATE_KEY, GITHUB_WEBHOOK_SECRET
STRIPE_SECRET_KEY, EXODUS_BUDGET_LIMIT, VLLM_BASE_URL
```

---

## AI Budget Rules

- Worth spending on: complex reviews, architecture decisions, initial code gen, Terraform risk analysis
- Use local first: `EXODUS_USE_LOCAL_LLMS=true` routes economy/standard tier to Mac Studio vLLM
- Track spend: `exodus ai-usage`
- Budget gate: `EXODUS_BUDGET_LIMIT` env var (default $15/session)
