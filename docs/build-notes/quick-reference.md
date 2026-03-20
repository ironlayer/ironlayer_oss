# Quick Reference — ironlayer_oss

## Project Overview

IronLayer is an open-source SQL control plane for Databricks (Apache 2.0). It generates deterministic execution plans from git diffs, manages incremental state, and layers AI advisory analysis (cost predictions, risk scoring, SQL optimization) on top. Framework-agnostic — works with dbt Core, SQLMesh, and raw SQL.

> **Note:** Repo rename from `ironlayer_oss` to `ironlayer` tracked in I-38.

---

## Locked Technical Decisions

| # | Decision | Key Detail |
|---|----------|------------|
| 1 | **SQLGlot pinned 25.34.1** | Last MIT version. Do NOT upgrade without legal review. |
| 2 | **uv workspace** | Single lockfile. Always `uv run --package <pkg>`, never bare `python`/`pip`. |
| 3 | **Ruff line-length=120** | Root `pyproject.toml`. No per-package overrides. E501 ignored. |
| 4 | **mypy strict mode** | Workspace-level target; per-package relaxations during migration. |
| 5 | **SQLAlchemy 2.0 async** | asyncpg (prod) / aiosqlite (dev). PostgreSQL 16 prod, SQLite local. |
| 6 | **Pydantic v2** | Settings via `pydantic-settings`. No v1 compat shims. |
| 7 | **Hatchling** build backend | All four Python packages. No setuptools. |
| 8 | **PyO3 abi3-py311** | Single Rust wheel for Python 3.11+. Rayon parallel execution. |
| 9 | **React 18 + Vite + Tailwind 3** | SPA, no Next.js. ReactFlow for lineage. |
| 10 | **FastAPI 0.115.x** pinned | `api` and `ai_engine` pin together. Upgrade in lockstep. |
| 11 | **Redis** for caching/queues | Host port 6380 → container 6379. |
| 12 | **Prometheus + Grafana** | Pushgateway for check_engine, postgres_exporter, custom `/metrics`. |
| 13 | **Apache 2.0 license** | Each package has its own `LICENSE` file. |

---

## Current State

**Built and functional:**
- 4 Python packages: `core_engine` v0.2.0, `ai_engine` v0.1.0, `api` v0.1.0, `cli` v0.2.0
- Rust `check_engine` v0.3.0 (90 rules, 12 categories, PyO3 bindings)
- React frontend with auth, dashboard, plan detail, model catalog, billing, admin
- Docker Compose full-stack (API, AI, frontend, Postgres, Redis, Prometheus, Grafana)
- CI workflows, pre-commit hooks, security hardening pass complete

**Recent activity (from git log):**
- Phase 0 doc standardization (CLAUDE.md, PR template, docs structure)
- Workspace rename standardization for Iron Tools / TheAiGroup
- Security review and hardening (auth, body limits, webhook replay)
- OSS cleanup (remove proprietary content, fix CI)

**Backlog:** `docs/backlog-execution.md` exists but has no active items.

---

## File Structure Overview

```
ironlayer_oss/
├── core_engine/core_engine/   # Deterministic SQL plan engine (SQLGlot, NetworkX, DuckDB)
│   ├── models/                #   Domain models: Plan, ModelDefinition, DiffResult, Snapshot
│   ├── parser/                #   SQL parsing (SQLGlot Databricks dialect)
│   ├── graph/                 #   DAG operations (NetworkX)
│   ├── planner/               #   Deterministic plan generation
│   ├── executor/              #   Plan execution against Databricks
│   ├── loader/                #   Model file discovery and loading
│   ├── state/                 #   Async persistence (SQLAlchemy 2.0)
│   └── config.py              #   pydantic-settings, PLATFORM_ prefix
├── ai_engine/ai_engine/       # AI advisory (scikit-learn, optional Anthropic LLM)
│   ├── engines/               #   Cost, risk, optimization engines
│   ├── ml/                    #   scikit-learn models
│   ├── routers/               #   FastAPI advisory endpoints
│   └── config.py              #   pydantic-settings, AI_ENGINE_ prefix
├── api/api/                   # FastAPI control-plane REST API
│   ├── routers/               #   REST endpoints
│   ├── services/              #   Business logic
│   ├── middleware/             #   Auth, CORS, rate limiting
│   └── security.py            #   JWT, credential encryption
├── cli/cli/                   # Typer CLI
│   ├── commands/              #   plan, apply, check, backfill, models, lineage, auth, etc.
│   ├── mcp/                   #   Optional MCP server
│   └── app.py                 #   Typer app + command registration
├── check_engine/src/          # Rust/PyO3 validation engine
│   ├── lib.rs                 #   PyO3 module entry (ironlayer_check_engine)
│   ├── engine.rs              #   Orchestrator: discovery → caching → checking
│   ├── checkers/              #   90 validation rules across 12 categories
│   └── pyo3_bindings.rs       #   Python-facing API
├── frontend/src/              # React 18 + TypeScript + Tailwind
│   ├── pages/                 #   Dashboard, PlanDetail, ModelCatalog, Billing, Admin...
│   ├── components/            #   Layout, ErrorBoundary, ProtectedRoute
│   └── contexts/              #   AuthContext
├── infra/                     # Docker, Terraform (Azure), Helm, CI/CD
├── tests/                     # Cross-package integration tests
├── scripts/                   # dev_setup.sh, e2e_smoke_test.sh
├── pyproject.toml             # uv workspace root
├── Cargo.toml                 # Rust workspace root
└── docker-compose.yml         # Full local stack
```

**Dependency graph:** `cli` → `core_engine` ← `api` → `ai_engine`. `check_engine` (Rust) is a PyO3 extension imported by `core_engine`.

---

## Key Code Patterns

| Pattern | Convention | Example |
|---------|-----------|---------|
| **Python imports** | `from __future__ import annotations` at top of every file | All config.py, models, app.py |
| **Config classes** | `pydantic_settings.BaseSettings` with `SettingsConfigDict` | `core_engine/config.py` → `PLATFORM_` prefix; `ai_engine/config.py` → `AI_ENGINE_` prefix |
| **Domain models** | `pydantic.BaseModel` with `Field()` defaults | `core_engine/models/plan.py` — Plan, PlanStep, PlanSummary |
| **Enums** | `class Foo(str, Enum)` pattern | `PlatformEnv`, `ClusterSize`, `Materialization`, `ChangeType` |
| **Secrets** | `pydantic.SecretStr` for tokens/keys | `databricks_token: SecretStr \| None` |
| **Deterministic IDs** | SHA-256 from content, not random UUIDs | `compute_deterministic_id(*parts)` in plan.py |
| **CLI commands** | `typer.Typer()` + `app.command(name=...)` | `cli/app.py` registers all commands |
| **CLI output** | `rich.console.Console` to stderr; JSON to stdout | Never `print()` |
| **Rust modules** | `pub mod` declarations in `lib.rs`; `//!` doc comments | `check_engine/src/lib.rs` |
| **Rust safety** | `catch_unwind` per-file; 2-min timeout (`MAX_CHECK_DURATION`) | `engine.rs` |
| **Frontend routes** | Lazy-loaded pages with per-route `ErrorBoundary` (BL-114) | `App.tsx` |
| **Backlog refs** | `BL-NNN` in code comments links to backlog items | `BL-077` (LLM allowlist), `BL-114` (error boundaries), `BL-118` (timeout) |
| **B008 ignored** | FastAPI `Depends()` in default args is intentional | Ruff config |

---

## Environment Variables

| Variable | Prefix | Required | Default |
|----------|--------|----------|---------|
| `PLATFORM_ENV` | `PLATFORM_` | no | `dev` |
| `PLATFORM_DATABASE_URL` | `PLATFORM_` | prod | SQLite locally |
| `AI_ENGINE_PORT` | `AI_ENGINE_` | no | `8001` |
| `AI_ENGINE_LLM_ENABLED` | `AI_ENGINE_` | no | `false` |
| `AI_ENGINE_LLM_API_KEY` | `AI_ENGINE_` | if LLM | — |
| `API_DATABASE_URL` | `API_` | prod | SQLite locally |
| `API_AI_ENGINE_URL` | `API_` | yes | `http://localhost:8001` |
| `API_PLATFORM_ENV` | `API_` | no | `dev` |
| `AI_ENGINE_SHARED_SECRET` | — | prod | `change-me-in-production` |
| `REDIS_URL` | — | prod | `redis://redis:6379/0` |
| `JWT_SECRET` | — | prod | dev default |
| `ANTHROPIC_API_KEY` | — | if LLM | — |
| `DATABRICKS_HOST` | — | runtime | — |
| `DATABRICKS_TOKEN` | — | runtime | — |
| `STRIPE_SECRET_KEY` | — | prod | — |

Full list in `CLAUDE.md` > Environment Variables.

---

## Common Commands

```bash
# Install
make install                     # uv sync + frontend npm install

# Lint & format
make format                      # ruff format + ruff check --fix
make lint                        # ruff check + mypy per package

# Test
make test                        # test-unit + test-integration
make test-unit                   # pytest with per-package coverage gates
make test-e2e                    # end-to-end tests

# Docker stack
make docker-up                   # Postgres, Redis, API, AI, frontend, Prometheus, Grafana
make docker-down

# Database
make migrate                     # alembic upgrade head
make migrate-create msg="..."    # new migration

# Rust check_engine
cd check_engine && cargo build --release --features extension-module
cd check_engine && cargo test && cargo bench

# Frontend
cd frontend && npm run dev       # Vite dev server
cd frontend && npm run build     # production build
cd frontend && npm run test:e2e  # Playwright

# Cleanup
make clean                       # remove __pycache__, .pytest_cache, etc.
```

---

## Related Files

| File | Purpose |
|------|---------|
| `CLAUDE.md` | Full project context — source of truth for AI agents |
| `docs/dev-journal.md` | Non-obvious discoveries and debugging breakthroughs |
| `docs/engineering-patterns.md` | Reusable patterns (async DB, Pydantic, error handling) |
| `docs/backlog-execution.md` | Current work items and status |
| `docs/architecture.md` | System architecture and data flow diagrams |
| `docs/api-reference.md` | REST API endpoints and schemas |
| `docs/cli-reference.md` | All `ironlayer` CLI commands and flags |
| `.claude/skills/` | Agent skills: building-item, resuming-build, capturing-lesson, exodus-* |
