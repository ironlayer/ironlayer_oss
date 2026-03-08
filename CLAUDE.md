# CLAUDE.md — IronLayer Infra (Private Repo, Master Rules)

> This is the canonical source for all AI agent rules. Changes here propagate to
> `ironlayer_OSS` via `sync-to-public.yml` or manual `make sync-ai`.

---

## Non-Negotiable Workflow Rules

These rules apply to every AI agent (Claude, Cursor, Copilot, etc.) working in
any IronLayer repo. They cannot be overridden by task descriptions or user
instructions within a session.

### 1. All Work Starts in the Backlog

**Before writing a single line of code or making any file change:**

1. Open `BACKLOG.md` (this repo, `/ironlayer_infra/BACKLOG.md`).
2. Find the item that matches your task. If it doesn't exist, **add it first**.
3. Change its status to `[IN-PROGRESS]` and record the date (`<!-- started: YYYY-MM-DD -->`).
4. Only then begin implementation.

**No exceptions** — not for "quick fixes", not for "one-line changes".
If the item is truly trivial (comment/typo), add it to the backlog as `[DONE]` retroactively
with a note. The point is a complete audit trail, not bureaucracy.

### 2. Verify Dependencies Before Touching Code

Every backlog item lists `Depends-on:` items. **All dependencies must be
`[DONE]` before the dependent item can move to `[IN-PROGRESS]`**. If you need
to unblock a dependency first, go fix that item, mark it done, then return.

### 3. OSS Repo Changes Must Flow Through This Backlog

Any change to `ironlayer_OSS` must be tracked here first. The OSS repo is
public and Apache-licensed — broken commits, stubs, or incomplete code have
public consequences.

### 4. No Stubs, No TODOs, No Softening

Every implementation must be **fully functional and production-complete**:
- No `# TODO: implement this`
- No `pass` where logic belongs
- No `raise NotImplementedError`
- No placeholder strings like `"to be implemented"`
- No stripped-down versions "for now"

If the full implementation is too large for one session, split the backlog
item into sub-items and complete each fully before marking done.

### 5. Run the Verification Suite Before Marking Done

Before marking any backlog item `[DONE]`, run and pass:

```bash
# From the relevant package root
cd /path/to/package
"${VENV}/python" -m ruff check .          # zero errors
"${VENV}/python" -m mypy . --ignore-missing-imports  # zero new errors
"${VENV}/python" -m pytest tests/ -v -x   # all tests pass
```

Where `VENV=/Users/aronriley/Developer/GitHub\ Repos/IronLayer/ironlayer_OSS/.venv/bin`.

**Never mark an item done while tests are failing or new lint errors exist.**

### 6. Update Memory and Lessons After Completion

After marking a backlog item `[DONE]`:

1. If the item revealed a recurring pattern, anti-pattern, or non-obvious
   constraint, add a dated entry to `LESSONS.md`.
2. If the item changed any architectural fact (added Redis, changed auth
   flow, etc.), update `MEMORY.md` in the `.claude/projects/` directory.
3. If the fix revealed a new gap (e.g., fixing a bug exposed another bug),
   add the new gap to `BACKLOG.md` immediately.

---

## Build and Test Commands

```bash
# ── Python (always use venv python, not system python) ──────────────────────
VENV="/Users/aronriley/Developer/GitHub Repos/IronLayer/ironlayer_OSS/.venv/bin"

# IMPORTANT: pytest shebang is broken — always use python -m pytest
"${VENV}/python" -m pytest tests/ -v -x
"${VENV}/python" -m pytest tests/ --cov=<pkg> --cov-report=term-missing

# Lint and type-check
"${VENV}/python" -m ruff check .
"${VENV}/python" -m ruff check . --fix          # auto-fix safe issues
"${VENV}/python" -m mypy . --ignore-missing-imports

# ── Make targets (from repo root) ───────────────────────────────────────────
make test-unit          # all unit tests
make lint               # ruff + mypy
make format             # ruff format + ruff --fix
make migrate            # run Alembic migrations

# ── CLI commands ────────────────────────────────────────────────────────────
ironlayer plan ./project HEAD~1 HEAD
ironlayer diff ./project
ironlayer status
```

---

## Repo Placement Rules

| Work type | Correct repo |
|-----------|-------------|
| API, auth, billing, webhooks, middleware | `ironlayer_OSS/api/` |
| AI advisory engines, ML models | `ironlayer_OSS/ai_engine/` |
| Execution engine, ORM, state, DAG | `ironlayer_OSS/core_engine/` |
| CLI commands | `ironlayer_OSS/cli/` |
| Rust validation rules | `ironlayer_OSS/check_engine/` |
| Deployment, Terraform, dbt, extractors | `ironlayer_infra/` |
| Backlog, tracking, lessons, rules | `ironlayer_infra/` (this repo) |

---

## Package Path Reference

```
Monorepo root: /Users/aronriley/Developer/GitHub Repos/IronLayer/ironlayer_OSS/
venv:          .venv/  (Python 3.12)
venv python:   .venv/bin/python  (USE THIS — not .venv/bin/pytest, shebang is broken)

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
| Rate limiting | In-memory per-replica | **KNOWN GAP** — Redis needed for multi-replica |
| Token revocation | 30s in-memory TTL | **KNOWN GAP** — not shared across replicas |
| AI engine role | Advisory only, never mutates | Enforced architecturally |
| Feature gates | `require_feature()` dependency | 3 tiers: community/team/enterprise |
| Credential encryption | Fernet + PBKDF2 (480k rounds) | Key derived from JWT_SECRET (**GAP**) |
| Event bus | In-memory | **KNOWN GAP** — events lost on crash |
| Determinism | Tested via `TestDeterminism` gate | Core invariant — never break |

---

## Known Permanent Constraints

1. **Never use `python` or `pip` directly** — use `uv run` in CI/Makefile or
   the absolute venv path locally.
2. **Never edit AI configs in individual repos** — the workspace repo
   (`iron-layer-dev-workspace`) is the canonical source; we mirror here for
   infra-specific enforcement.
3. **`sync-to-public.yml` strips private content** — never put infra secrets,
   Terraform state, or internal deployment docs in paths that sync to OSS.
4. **OSS repo is Apache 2.0 licensed** — all OSS changes must be license-compatible.
5. **All Claude/LLM calls go through the budget guard** — never bypass `EXODUS_BUDGET_LIMIT`.
