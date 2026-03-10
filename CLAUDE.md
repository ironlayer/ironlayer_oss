# CLAUDE.md — IronLayer OSS

---

## Non-Negotiable Workflow Rules

These rules apply to every AI agent (Claude, Cursor, Copilot, etc.) working in
this repo.

### 1. No Stubs, No TODOs, No Softening

Every implementation must be **fully functional and production-complete**:
- No `# TODO: implement this`
- No `pass` where logic belongs
- No `raise NotImplementedError`
- No placeholder strings like `"to be implemented"`
- No stripped-down versions "for now"

### 2. Run the Verification Suite Before Marking Done

Before considering any task complete, run and pass:

```bash
# From the repo root
uv run ruff check .                              # zero errors
uv run mypy . --ignore-missing-imports           # zero new errors
uv run --package <pkg> pytest <pkg>/tests/ -v -x # all tests pass
```

**Never mark an item done while tests are failing or new lint errors exist.**

### 3. Update Docs After Completion

After completing work:

1. If the change revealed a recurring pattern or non-obvious constraint,
   document it in `.cursor/rules/` for future agent reference.
2. If the change altered architecture (added Redis, changed auth flow, etc.),
   update the relevant documentation.

---

## Build and Test Commands

```bash
# Run tests per package
uv run --package ironlayer-api pytest api/tests/ -v -x
uv run --package ai-engine pytest ai_engine/tests/ -v -x
uv run --package ironlayer-core pytest core_engine/tests/ -v -x
uv run --package ironlayer pytest cli/tests/ -v -x

# With coverage
uv run --package ironlayer-api pytest api/tests/ --cov=api --cov-report=term-missing
uv run --package ai-engine pytest ai_engine/tests/ --cov=ai_engine --cov-report=term-missing

# Lint and type-check
uv run ruff check .
uv run ruff check . --fix          # auto-fix safe issues
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

## Repo Layout

| Work type | Path |
|-----------|------|
| API, auth, billing, webhooks, middleware | `api/` |
| AI advisory engines, ML models | `ai_engine/` |
| Execution engine, ORM, state, DAG | `core_engine/` |
| CLI commands | `cli/` |
| Rust validation rules | `check_engine/` |
| Frontend (React/Vite) | `frontend/` |
| Deployment, Terraform, monitoring | `infra/` |

## Package Reference

```
api/           package: ironlayer-api   v0.1.0   port 8000
ai_engine/     package: ai-engine       v0.1.0   port 8001
core_engine/   package: ironlayer-core  v0.3.0
cli/           package: ironlayer       v0.2.0
check_engine/  Rust + PyO3
frontend/      React + Vite             port 3000
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

---

## Known Permanent Constraints

1. **Never use `python` or `pip` directly** — use `uv run` in CI/Makefile.
2. **OSS repo is Apache 2.0 licensed** — all changes must be license-compatible.
3. **No infrastructure secrets** — never commit real API keys, tokens, or credentials.
   Use `.env.example` files for templates. Real `.env` files are gitignored.
