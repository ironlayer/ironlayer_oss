# AGENTS.md — IronLayer

This file provides AI coding agents (Cursor, GitHub Copilot, Claude Code, etc.) with workspace-level context.

---

## Project Overview

IronLayer is an AI-native SQL transformation control plane for Databricks.

| Component | Path | Role |
|---|---|---|
| **API** | `api/` | FastAPI control plane (port 8000) |
| **AI Engine** | `ai_engine/` | FastAPI advisory service (port 8001) |
| **Core Engine** | `core_engine/` | Execution engine, ORM, state, DAG |
| **CLI** | `cli/` | Typer CLI (`ironlayer`) |
| **Check Engine** | `check_engine/` | Rust/PyO3 validation engine (90+ rules) |
| **Frontend** | `frontend/` | React + Vite (port 3000) |

---

## Build and Test Commands

```bash
# Run tests (from repo root, per package)
uv run --package ironlayer-api pytest api/tests/ -v
uv run --package ai-engine pytest ai_engine/tests/ -v
uv run --package ironlayer-core pytest core_engine/tests/ -v
uv run --package ironlayer pytest cli/tests/ -v

# Lint and type-check
uv run ruff check .
uv run mypy . --ignore-missing-imports

# Make targets (from repo root)
make test-unit          # all unit tests
make lint               # ruff + mypy
make format             # ruff format + ruff --fix
make migrate            # run Alembic migrations

# CLI commands
ironlayer plan ./project HEAD~1 HEAD
ironlayer diff ./project
ironlayer status
```

---

## Coding Conventions

### Python
- Type hints required, docstrings on public methods
- `rich` for CLI output, `typer` for CLI structure
- Never hardcode credentials — read from `os.environ`
- `uv run` for all commands

### Terraform
- `for_each` with named maps — `count` only for boolean toggles
- Tag all resources with `common_tags`

### SQL
- UPPERCASE keywords, CTE pattern
- Framework-agnostic — supports dbt Core, dbt Cloud, and SQLMesh

### Git Commit Convention
```
type(scope): description
Types: feat, fix, refactor, test, docs, ci, chore, perf
Scopes: core, api, ai, cli, plan, lineage, check-engine, frontend, cicd
```

---

## Architecture Quick Reference

| Concern | Implementation | Notes |
|---------|---------------|-------|
| Auth modes | dev / JWT / KMS / OIDC | Configured via `AUTH_MODE` env var |
| Multi-tenancy | PostgreSQL RLS via `app.tenant_id` | Set in `dependencies.py` |
| Rate limiting | Redis-backed (distributed) | Falls back to in-process when no Redis |
| Token revocation | 3-layer: L1 in-process → L2 Redis → L3 DB | Shared across replicas |
| AI engine role | Advisory only, never mutates | Enforced architecturally |
| Feature gates | `require_feature()` dependency | 3 tiers: community/team/enterprise |
| Credential encryption | Fernet + PBKDF2 (480k rounds) | Always-on security behaviour |
| Event bus | Transactional outbox | At-least-once delivery via `EventOutboxTable` |
| Determinism | Tested via `TestDeterminism` gate | Core invariant — never break |
