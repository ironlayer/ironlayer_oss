# IronLayer Production Backlog

**Source of truth for all planned and in-progress work.**

All AI agents and developers must reference this file before starting any task.
See `CLAUDE.md` for workflow rules. See `LESSONS.md` for lessons learned.

Last full audit: **2026-03-07** | P1 sprint completed: **2026-03-07** | P2 sprint completed: **2026-03-07** | P2 corrections: BL-041 (2026-03-07) | Security audit: **2026-03-07** | Performance baseline: **2026-03-07**

---

## Status Legend

| Status | Meaning |
|--------|---------|
| `[OPEN]` | Not started |
| `[IN-PROGRESS]` | Being worked on — include date |
| `[DONE]` | Completed — include date and commit/PR ref |
| `[BLOCKED]` | Cannot start — waiting on dependency |

## Priority Legend

| Priority | Criteria |
|----------|---------|
| `P0` | Production-breaking — causes runtime crash, data loss, or security breach |
| `P1` | High — correctness bug, security gap, or multi-replica failure mode |
| `P2` | Medium — quality gap, missing coverage, degraded reliability |
| `P3` | Low — enhancement, observability, future-proofing |

## Repo Legend

| Tag | Meaning |
|-----|---------|
| `[OSS]` | Changes in `ironlayer_OSS` |
| `[INFRA]` | Changes in `ironlayer_infra` |
| `[BOTH]` | Changes in both repos (OSS + infra sync) |

## Quick Stats

| Priority | Items | Status |
|----------|-------|--------|
| P0 | BL-001 through BL-004 (4 items) | ALL DONE (2026-03-07) |
| P1 | BL-005 through BL-012, BL-017, BL-018, BL-038, BL-039, BL-040 (13 items) | ALL DONE (2026-03-07) |
| P2 | BL-013 through BL-016, BL-019 through BL-027, BL-036, BL-037, BL-041 (19 items) | ALL DONE (2026-03-07) |
| P3 | BL-028 through BL-035 (8 items) | ALL DONE (2026-03-07) |
| **Security & Performance Sprint — 2026-03-07** | | |
| P0 | BL-043 through BL-046 (4 items) | ALL DONE (2026-03-07) |
| P1 | BL-047 through BL-061 (15 items) | ALL DONE (2026-03-07) |
| P2 | BL-062 through BL-083 (22 items) | ALL DONE (2026-03-07, BL-064 skipped) |
| P3 | BL-084 through BL-096 (13 items) | ALL DONE (2026-03-07, BL-086 deferred) |
| P4 | BL-097 through BL-104 (8 items) | ALL DONE (2026-03-07) |

---

## P0 — Production-Breaking

---

### BL-001 — Fix `HTTPException` undefined name in `models.py`
**Priority:** P0 | **Status:** [DONE] <!-- started: 2026-03-07, done: 2026-03-07 --> | **Repo:** [BOTH]
**Audit source:** Ruff F821 | Production: confirmed runtime `NameError` if code path is hit

**Problem:**
`api/api/routers/models.py` uses `HTTPException` at lines 336, 361, 374, and 383
(all in the `get_column_lineage` endpoint) but the name is never imported. These
are the error paths for path-traversal validation and SQL lineage parse failures.
Any request that triggers these code paths crashes with `NameError: name 'HTTPException' is not defined`.

**Affected lines:**
```python
# line 336
raise HTTPException(status_code=400, detail="Model repository path is outside...")
# line 361
raise HTTPException(status_code=422, detail="No SQL available for model...")
# line 374
raise HTTPException(status_code=422, detail="Column lineage analysis failed.")
# line 383
raise HTTPException(status_code=404, detail=f"Column '{column}' not found...")
```

**Fix:**
Add `HTTPException` to the existing `fastapi` import at the top of the file:
```python
from fastapi import APIRouter, Depends, HTTPException, Query
```

**Also in same file:** Remove the unused bare call at the current bottom of `get_model`:
```python
await run_repo.get_by_plan("")  # empty plan_id returns nothing
```
This does nothing and should be removed.

**Depends-on:** Nothing

**Acceptance criteria:**
- [ ] `ruff check api/api/routers/models.py` reports zero F821 errors
- [ ] `python -m pytest api/tests/test_models_router.py -v` passes all tests
- [ ] GET `/models/{model_name}/column-lineage` returns proper 400/422/404 responses
      (not 500 NameError) for invalid inputs
- [ ] No new ruff or mypy errors introduced

---

### BL-002 — Fix broken pytest shebang in venv
**Priority:** P0 | **Status:** [DONE] <!-- started: 2026-03-07, done: 2026-03-07 --> | **Repo:** [BOTH]

**Problem:**
`.venv/bin/pytest` shebang bakes in the wrong Python path:
```
#!/usr/bin/env sh
exec '/Users/aronriley/Developer/GitHub Repos/ironlayer_OSS/.venv/bin/python' ...
```
The actual path is `.../IronLayer/ironlayer_OSS/.venv/bin/python` (missing `IronLayer`
parent directory). Running `pytest` directly fails with "No such file or directory".

**Scope clarification (found during BL-001 work, 2026-03-07):**
The broken path is not limited to the shebang. ALL `.pth` editable-install files
in the venv also bake in the wrong path. `_ironlayer_core.pth`, `_ironlayer_api.pth`,
and `_ironlayer.pth` all point to `/Users/aronriley/Developer/GitHub Repos/ironlayer_OSS/...`
(missing `IronLayer` parent). Python silently ignores `.pth` entries for non-existent
paths, so the workspace packages (`core_engine`, `api`, `cli`) are NOT importable
through the normal venv mechanism. Tests have been working only when PYTHONPATH is
set explicitly. **Option B (sed shebang patch) cannot fix .pth files — Option A
(full rebuild) is required.**

**Fix — Option A (required):** Rebuild the venv from the correct path:
```bash
cd "/Users/aronriley/Developer/GitHub Repos/IronLayer/ironlayer_OSS"
rm -rf .venv
uv venv --python 3.12
uv sync --all-packages
```

**Permanent fix:** Add a `make fix-venv` target or use `uv` which doesn't bake
absolute paths into scripts.

**Depends-on:** Nothing

**Acceptance criteria:**
- [ ] `.venv/bin/pytest --version` runs without error
- [ ] `cd api && ../.venv/bin/pytest tests/ -q` runs and completes
- [ ] Document in `CLAUDE.md` to always use `python -m pytest` as a permanent habit
      regardless of shebang status (defense-in-depth)

---

### BL-003 — Fix CLI tests: PYTHONPATH + `core_engine` module not found
**Priority:** P0 | **Status:** [DONE] <!-- started: 2026-03-07, done: 2026-03-07 --> | **Repo:** [BOTH]
**Audit source:** 8 collection errors in `cli/tests/`

**Problem:**
All 8 CLI test files fail to collect because `core_engine` is not on `sys.path`
when pytest runs from `cli/`. The CLI imports `from core_engine.config import PlatformEnv`
and similar, but `core_engine` lives at `../core_engine/` which is not added to
`PYTHONPATH` automatically.

The shebang issue (BL-002) compounds this but is a separate problem — even after
fixing the shebang, PYTHONPATH is still missing.

**Root cause:**
`cli/pyproject.toml` does not declare `ironlayer-core` as a dev dependency, so it
is not installed into the CLI's test environment. The workspace `uv sync` installs
everything into the root venv which has `.pth` files, but when pytest resolves
imports it doesn't benefit from those in all configurations.

**Fix:**
1. Add `ironlayer-core` as a dev dependency in `cli/pyproject.toml`:
   ```toml
   [dependency-groups]
   dev = [
       "ironlayer-core",    # Add this
       "pytest>=8.0",
       ...
   ]
   ```
2. Add `conftest.py` at `cli/tests/conftest.py` with path setup as a safety net:
   ```python
   import sys
   from pathlib import Path
   # Ensure core_engine is importable when running from cli/ directory
   _root = Path(__file__).parent.parent.parent  # ironlayer_OSS/
   for _pkg in ["core_engine", "api"]:
       _pkg_path = str(_root / _pkg)
       if _pkg_path not in sys.path:
           sys.path.insert(0, _pkg_path)
   ```
3. Verify all 8 test files collect without error.

**Depends-on:** BL-002 (venv rebuild)

**Acceptance criteria:**
- [ ] `cd cli && python -m pytest tests/ --collect-only` shows 0 collection errors
- [ ] `cd cli && python -m pytest tests/ -v -x` runs with all tests passing or xfailing
      (no errors — only legitimate test failures count)
- [ ] CI `test-cli` job passes

---

### BL-004 — Fix 2 failing AI engine tests
**Priority:** P0 | **Status:** [DONE] <!-- started: 2026-03-07, done: 2026-03-07 --> | **Repo:** [BOTH]

**Problem A — `test_pii_scrubber.py::TestContainsPII::test_token_detected`:**
`contains_pii("dapi_FAKE_TOKEN_FOR_TESTING")` returns `False`.

Root cause: `sync-to-public.yml` replaced the original Databricks PAT
(`dapi` + 32 hex chars, e.g., `dapi1a2b3c4d...`) with `dapi_FAKE_TOKEN_FOR_TESTING`.
The `contains_pii` regex matches real DAP tokens (`dapi[0-9a-f]{32}`) but
`dapi_FAKE_TOKEN_FOR_TESTING` contains underscores and uppercase letters that
don't match the hex-only pattern. The `scrub_for_llm` tests pass because they
test for context-aware scrubbing (`token=dapi_...`) which uses a broader generic
secret pattern.

**Fix for Problem A:**
Update the test to use a format-correct fake Databricks PAT:
```python
_FAKE_DAPI = "dapi" + "a" * 32  # matches real PAT format: dapi + 32 hex chars

def test_token_detected(self):
    assert contains_pii(_FAKE_DAPI) is True

def test_dapi_token_scrubbed(self):
    text = f"token={_FAKE_DAPI} for auth"
    result = scrub_for_llm(text)
    assert _FAKE_DAPI not in result

def test_dapi_in_sql_context(self):
    sql = f"-- token: {_FAKE_DAPI}\nSELECT 1"
    result = scrub_sql_for_llm(sql)
    assert _FAKE_DAPI not in result
```
Also update `sync-to-public.yml` to sanitize tokens to a format-correct fake:
Replace `dapi_FAKE_TOKEN_FOR_TESTING` → `dapi${"a" * 32}` (or equivalent sed pattern).

**Problem B — `test_suggestion_validator.py::TestValidateSyntax::test_garbage_text_rejected`:**
`SuggestionValidator._validate_syntax("THIS IS NOT SQL")` returns `True`.

Root cause: The test comment says "sqlglot >=25 rejects this" but sqlglot 26.x
running in the venv still permissively accepts `THIS IS NOT SQL` as a valid
expression (treating words as identifier column references). The validation
logic only checks for parse errors, not whether the result is actually a
recognized SQL statement type.

**Fix for Problem B:**
Update `_validate_syntax` in `suggestion_validator.py` to additionally verify
the parsed AST contains at least one recognized SQL statement node:
```python
import sqlglot
from sqlglot.expressions import (
    Select, Insert, Update, Delete, Create, Drop,
    Merge, With, Union, Except, Intersect,
)

_SQL_STATEMENT_TYPES = (
    Select, Insert, Update, Delete, Create, Drop,
    Merge, With, Union, Except, Intersect,
)

@staticmethod
def _validate_syntax(sql: str) -> bool:
    """Return True only if sql parses as a recognized SQL statement."""
    try:
        statements = sqlglot.parse(sql, dialect="databricks", error_level=sqlglot.ErrorLevel.RAISE)
        if not statements:
            return False
        return any(isinstance(stmt, _SQL_STATEMENT_TYPES) for stmt in statements if stmt is not None)
    except (sqlglot.ParseError, ValueError):
        return False
```

**Depends-on:** Nothing (BL-002 helpful but not blocking)

**Acceptance criteria:**
- [ ] `python -m pytest ai_engine/tests/test_pii_scrubber.py -v` 0 failures
- [ ] `python -m pytest ai_engine/tests/test_suggestion_validator.py -v` 0 failures
- [ ] `python -m pytest ai_engine/tests/ -q` shows 395+ passed, 0 failed
- [ ] `sync-to-public.yml` sanitizes to format-correct fake PAT pattern

---

## P1 — High Security / Correctness

---

### BL-005 — Fix `TokenManager | None` None guard in `auth.py:333`
**Priority:** P1 | **Status:** [DONE] <!-- completed: 2026-03-07 --> | **Repo:** [BOTH]
**Audit source:** mypy `union-attr` error

**Problem:**
`api/api/middleware/auth.py` line 333 accesses `.validate_token()` on
`self._token_manager` which is typed `TokenManager | None`. If token manager
initialization fails (e.g., missing JWT_SECRET in a non-dev environment, KMS
unavailable), `self._token_manager` is `None` and the `.validate_token()` call
raises `AttributeError`. This presents as a 500 server error rather than a
proper 401, which could be exploited to infer initialization state.

**Fix:**
Add an explicit None guard before the attribute access:
```python
# In AuthMiddleware.dispatch or _validate_bearer_token
if self._token_manager is None:
    logger.error("TokenManager not initialized — rejecting request")
    return JSONResponse(
        {"detail": "Authentication service unavailable"},
        status_code=503,
    )
result = self._token_manager.validate_token(token)
```

**Depends-on:** Nothing

**Acceptance criteria:**
- [ ] `mypy api/api/middleware/auth.py --ignore-missing-imports` reports zero `union-attr` errors
- [ ] When `AUTH_MODE=jwt` and `JWT_SECRET` is unset, API returns 503 (not 500 AttributeError)
- [ ] Existing auth middleware tests still pass
- [ ] New test: `test_auth_returns_503_when_token_manager_not_initialized`

---

### BL-006 — Fix `body_limit.py` `BaseHTTPMiddleware` type annotation
**Priority:** P1 | **Status:** [DONE] <!-- completed: 2026-03-07 --> | **Repo:** [BOTH]
**Audit source:** mypy `arg-type` error at `body_limit.py:26`

**Problem:**
`BodyLimitMiddleware.__init__` declares `app: object` but `BaseHTTPMiddleware`
expects an ASGI callable. While this works at runtime (the type is ignored in
practice), the mypy error indicates the type contract is broken and mypy cannot
verify that `super().__init__(app)` is called correctly. In some Starlette
versions this can cause middleware ordering issues.

**Fix:**
Import and use the correct type annotation:
```python
from starlette.types import ASGIApp

class BodyLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp, *, max_body_size: int = 1_048_576) -> None:
        super().__init__(app)
        self._max_size = max_body_size
```

**Depends-on:** Nothing

**Acceptance criteria:**
- [ ] `mypy api/api/middleware/body_limit.py --ignore-missing-imports` zero errors
- [ ] `python -m pytest api/tests/ -v -k body_limit` passes
- [ ] Body limit middleware still correctly rejects oversized requests

---

### BL-007 — Redis-backed distributed rate limiting
**Priority:** P1 | **Status:** [DONE] <!-- completed: 2026-03-07 --> | **Repo:** [BOTH]

**Problem:**
`api/api/middleware/rate_limit.py` and `login_rate_limiter.py` use
`collections.defaultdict` with in-process counters. In a multi-replica
deployment (Azure Container Apps scales to N instances), each replica maintains
independent counters. A coordinated attack distributing N-1 requests per minute
across N replicas bypasses all rate limits entirely.

**Fix:**
Replace in-process counters with Redis-backed sliding window counters using
`redis-py[asyncio]`. Use `aioredis` or the `redis.asyncio` client.

Implementation steps:
1. Add `redis[asyncio]>=5.0,<6.0` to `api/pyproject.toml` dependencies.
2. Add Redis config to `APISettings` in `api/api/config.py`:
   ```python
   redis_url: str | None = Field(default=None, description="Redis URL for rate limiting and token revocation. If None, falls back to in-process (single-replica only).")
   redis_key_prefix: str = Field(default="ironlayer:", description="Key prefix for Redis keys.")
   redis_rate_limit_window_seconds: int = Field(default=60)
   ```
3. Create `api/api/services/redis_client.py` — async Redis client singleton
   with connection pooling and graceful fallback to in-process if Redis is
   unavailable (log a warning, never fail hard on Redis unavailability).
4. Refactor `rate_limit.py` to use a `RateLimitBackend` protocol with two
   implementations: `InProcessRateLimitBackend` (existing) and
   `RedisRateLimitBackend` (new). Factory selects based on `redis_url`.
5. Use Redis INCR + EXPIRE sliding window pattern (atomic, no race condition).
6. Add `REDIS_URL=redis://redis:6379/0` to `docker-compose.yml` and `.env.example`.

**Depends-on:** Nothing (can be done independently)
**Paired with:** BL-008 (both touch Redis infrastructure — implement together)

**Acceptance criteria:**
- [ ] Rate limiting works correctly when 2 workers are started and requests
      are distributed across both (integration test with concurrent workers)
- [ ] Falls back to in-process backend gracefully when `REDIS_URL` is unset
      (with logged warning)
- [ ] `docker-compose up` includes Redis container
- [ ] `.env.example` documents `REDIS_URL` variable
- [ ] `python -m pytest api/tests/test_rate_limit.py -v` all pass
- [ ] No new ruff/mypy errors

---

### BL-008 — Redis-backed distributed token revocation cache
**Priority:** P1 | **Status:** [DONE] <!-- completed: 2026-03-07 --> | **Repo:** [BOTH]

**Problem:**
The token revocation cache in `security.py` / `auth.py` uses a 30-second
in-memory TTL dict. In a multi-replica deployment, revoking a token on one
replica does not propagate to other replicas — the revoked token remains valid
on all other replicas for up to 30 seconds. For emergency revocation (e.g.,
compromised credentials), this is unacceptable.

**Fix:**
Replace the in-memory revocation cache with Redis-backed SET membership:
1. Reuse the `RedisClient` singleton from BL-007.
2. On token revocation: `SADD ironlayer:revoked:{tenant_id} {token_jti}` with
   `EXPIREAT` set to the token's own `exp` claim (so keys auto-clean when token
   would have expired naturally).
3. On token validation: `SISMEMBER ironlayer:revoked:{tenant_id} {token_jti}`.
4. If Redis is unavailable: fail-closed (reject the token, return 401). This
   is the existing behavior for DB outage — maintain it for Redis outage too.
5. Reduce in-memory TTL cache to 5 seconds as a L1 cache in front of Redis
   (reduces Redis round-trips for hot paths).

**Depends-on:** BL-007 (Redis infrastructure and client singleton)

**Acceptance criteria:**
- [ ] Token revoked on replica A is rejected on replica B within 5 seconds
      (integration test with 2 workers + shared Redis)
- [ ] When Redis is unavailable, revocation calls return 503 (fail-closed)
- [ ] JTI expiry aligns with token's `exp` claim (no orphan Redis keys)
- [ ] `python -m pytest api/tests/test_token_revocation.py -v` all pass

---

### BL-009 — Separate credential encryption key from JWT secret
**Priority:** P1 | **Status:** [DONE] <!-- completed: 2026-03-07 --> | **Repo:** [BOTH]

**Problem:**
The Fernet encryption key used for Databricks PAT storage is derived from
`JWT_SECRET` via PBKDF2. A single `JWT_SECRET` compromise therefore gives an
attacker the ability to both forge JWT tokens AND decrypt all stored Databricks
credentials. The blast radius of a JWT secret compromise is doubled.

**Fix:**
1. Add a new env var `CREDENTIAL_ENCRYPTION_KEY` (32+ bytes, base64-encoded).
2. In `security.py` (the `CredentialEncryptor` or equivalent class), derive the
   Fernet key from `CREDENTIAL_ENCRYPTION_KEY` instead of `JWT_SECRET`.
3. In `_build_token_config` / `APISettings`, require `CREDENTIAL_ENCRYPTION_KEY`
   when `AUTH_MODE != development`, alongside the existing `JWT_SECRET` check.
4. Add migration path: on first load after upgrade, if old credentials exist but
   `CREDENTIAL_ENCRYPTION_KEY` is new, provide a migration utility that re-encrypts
   existing credentials. Document in release notes.
5. Add `CREDENTIAL_ENCRYPTION_KEY` to `.env.example` with generation instructions:
   ```
   # Generate with: python -c "import secrets,base64; print(base64.b64encode(secrets.token_bytes(32)).decode())"
   CREDENTIAL_ENCRYPTION_KEY=<base64-encoded 32-byte key>
   ```

**Depends-on:** Nothing

**Acceptance criteria:**
- [ ] `CREDENTIAL_ENCRYPTION_KEY` env var read and validated at startup
- [ ] API refuses to start in non-dev mode without `CREDENTIAL_ENCRYPTION_KEY`
- [ ] Credentials encrypted with `CREDENTIAL_ENCRYPTION_KEY`, not `JWT_SECRET`
- [ ] Credential decryption still works after key rotation (re-encryption utility)
- [ ] `.env.example` updated with instructions
- [ ] `python -m pytest api/tests/test_auth_service.py -v` all pass

---

### BL-010 — JWT secret rotation mechanism
**Priority:** P1 | **Status:** [DONE] <!-- completed: 2026-03-07 --> | **Repo:** [BOTH]

**Problem:**
There is no mechanism for rotating `JWT_SECRET`. If the secret is compromised,
the only option is to invalidate ALL active sessions simultaneously (by changing
the secret), causing a complete user logout. There is no grace period or
dual-signing window.

**Fix:**
Implement dual-secret validation with a `JWT_SECRET_PREVIOUS` env var:
1. Add `JWT_SECRET_PREVIOUS: str | None` to `TokenConfig`.
2. During validation, try `JWT_SECRET` first; if verification fails with
   `InvalidSignatureError`, try `JWT_SECRET_PREVIOUS` (if set).
3. All NEW tokens are signed with `JWT_SECRET` only.
4. Rotation procedure (documented): set `JWT_SECRET_PREVIOUS=<old_secret>`,
   set `JWT_SECRET=<new_secret>`, deploy. Old tokens valid during
   `TOKEN_TTL_SECONDS`. After TTL passes, remove `JWT_SECRET_PREVIOUS`.
5. Add `JWT_SECRET_PREVIOUS` to `.env.example` with rotation docs.

**Depends-on:** Nothing

**Acceptance criteria:**
- [ ] Token signed with `JWT_SECRET_PREVIOUS` is still valid during rotation window
- [ ] Token signed with `JWT_SECRET_PREVIOUS` is rejected after key removed
- [ ] All new tokens issued use `JWT_SECRET` (not previous)
- [ ] `python -m pytest api/tests/test_auth_service.py -v` all pass
- [ ] Rotation procedure documented in `api/.env.example`

---

### BL-011 — Raise webhook secret minimum entropy requirement
**Priority:** P1 | **Status:** [DONE] <!-- completed: 2026-03-07 --> | **Repo:** [BOTH]

**Problem:**
`EventSubscriptionCreate.secret` has `min_length=8`, allowing 8-character
secrets (~48 bits entropy). OWASP recommends 256-bit secrets for HMAC signing.
An 8-character human-chosen secret is trivially brute-forced.

**Fix:**
1. Change `min_length=8` → `min_length=32` in `EventSubscriptionCreate.secret`
   and `EventSubscriptionUpdate.secret`.
2. Add description update: "Must be at least 32 characters (256-bit entropy
   recommended). Generate with: `python -c \"import secrets; print(secrets.token_hex(32))\"`"
3. Update validation error messages to be actionable.

**Depends-on:** Nothing

**Acceptance criteria:**
- [ ] POST `/event-subscriptions` with `secret` of length < 32 returns HTTP 422
- [ ] POST `/event-subscriptions` with `secret` of length 32 returns HTTP 201
- [ ] `.env.example` shows correct generation command
- [ ] `python -m pytest api/tests/test_event_subscription_router.py -v` all pass

---

### BL-012 — Fix OIDC DNS resolution blocking event loop
**Priority:** P1 | **Status:** [DONE] <!-- completed: 2026-03-07 --> | **Repo:** [BOTH]

**Problem:**
The SSRF protection in OIDC token validation calls `socket.getaddrinfo()` (a
blocking syscall) from inside an async FastAPI route handler. Under load, a
slow or malicious DNS server can stall the asyncio event loop for the duration
of the DNS TTL, blocking all concurrent requests on that worker.

**Fix:**
Replace `socket.getaddrinfo()` with `asyncio.get_event_loop().getaddrinfo()` and
add a timeout:
```python
import asyncio

async def _is_ssrf_safe_host(hostname: str) -> bool:
    try:
        loop = asyncio.get_running_loop()
        infos = await asyncio.wait_for(
            loop.getaddrinfo(hostname, None, family=socket.AF_INET),
            timeout=2.0,   # never block longer than 2s
        )
    except (asyncio.TimeoutError, OSError):
        return False  # fail-closed on DNS failure
    for _family, _type, _proto, _canonname, sockaddr in infos:
        ip = sockaddr[0]
        if _is_private_ip(ip):
            return False
    return True
```

**Depends-on:** Nothing

**Acceptance criteria:**
- [ ] DNS resolution in OIDC SSRF check is async with 2s timeout
- [ ] Slow DNS response (>2s) results in SSRF check failing closed (host rejected)
- [ ] `python -m pytest api/tests/test_auth_router.py -v -k oidc` all pass
- [ ] No new mypy errors

---

### BL-038 — Add pre-commit hook for F821 undefined names (zero-tolerance)
**Priority:** P1 | **Status:** [DONE] <!-- completed: 2026-03-07 --> | **Repo:** [BOTH]
**Added:** 2026-03-07 self-review | Audit source: LESSONS.md lesson 2

**Problem:**
The `HTTPException` undefined name (BL-001) survived code review and CI because
`ruff F821` was not enforced as a blocking pre-commit hook. F821 errors (undefined
names) have zero legitimate false positives in Python — every F821 is a real bug.
Catching these at commit time rather than in CI (or in production) is trivially
achievable and should be non-negotiable.

**Fix:**
1. Add a dedicated F821 hook to `.pre-commit-config.yaml` in `ironlayer_OSS`
   and `ironlayer_infra`:
   ```yaml
   - repo: local
     hooks:
       - id: ruff-undefined-names
         name: ruff F821 — undefined names (zero tolerance)
         language: system
         entry: python -m ruff check --select=F821 --no-fix
         types: [python]
         pass_filenames: true
   ```
2. Add the same check to CI `ruff` step: `--select=F821 --exit-non-zero-on-fix=false`.
3. Run `ruff check --select=F821 .` now (after BL-001) to verify zero violations.

**Depends-on:** BL-001 (fix existing F821 violations before enforcing the hook)

**Acceptance criteria:**
- [ ] `pre-commit run --all-files` fails if any Python file has an undefined name
- [ ] CI ruff step includes `--select=F821`
- [ ] Zero F821 violations in codebase when hook is enabled
- [ ] Hook documented in `CLAUDE.md` onboarding section

---

### BL-039 — Add tenant isolation (RLS) integration tests
**Priority:** P1 | **Status:** [DONE] <!-- completed: 2026-03-07 --> | **Repo:** [BOTH]
**Added:** 2026-03-07 self-review

**Problem:**
Multi-tenant data isolation via PostgreSQL RLS (`app.tenant_id`) is the most
critical security property in the system. There are currently no integration
tests verifying that:

1. Tenant A cannot read, update, or delete Tenant B's resources
2. A missing or spoofed `app.tenant_id` session variable returns zero rows
   (RLS should block all rows — not return all rows — when tenant context is absent)
3. RLS is applied to INSERT, UPDATE, and DELETE (not just SELECT)
4. A new migration that accidentally drops or weakens an RLS policy is caught before deploy

A regression in `SET app.tenant_id` in `dependencies.py` would expose all
tenants' data to all users — invisible until a customer report.

**Fix:**
Add `api/tests/integration/test_tenant_isolation.py`:
```python
import pytest
from httpx import AsyncClient

@pytest.mark.integration
async def test_tenant_a_cannot_read_tenant_b_model(
    async_client_tenant_a: AsyncClient,
    async_client_tenant_b: AsyncClient,
    tenant_a_model_id: str,
):
    """Tenant B must receive 404 (not 200) for Tenant A's model."""
    response = await async_client_tenant_b.get(f"/models/{tenant_a_model_id}")
    assert response.status_code == 404  # RLS makes row invisible — not 403

@pytest.mark.integration
async def test_missing_tenant_context_returns_no_rows(db_session):
    """Raw query without app.tenant_id set must return zero rows from all RLS tables."""
    # Do NOT set app.tenant_id — verify RLS blocks everything
    result = await db_session.execute(text("SELECT COUNT(*) FROM models"))
    assert result.scalar() == 0

@pytest.mark.integration
async def test_tenant_a_cannot_update_tenant_b_model(
    async_client_tenant_b: AsyncClient,
    tenant_a_model_id: str,
):
    response = await async_client_tenant_b.patch(
        f"/models/{tenant_a_model_id}",
        json={"description": "injected"},
    )
    assert response.status_code == 404  # UPDATE affected 0 rows → 404
```

Also add a migration validation fixture: after each test migration, re-verify
that `pg_policies` contains the expected RLS policies.

**Depends-on:** BL-001 (stable API), BL-027 (migration validation CI)

**Acceptance criteria:**
- [ ] Tenant A's models return 404 (not 403/200) for Tenant B
- [ ] Raw DB queries without tenant context return 0 rows
- [ ] RLS verified on SELECT, UPDATE, DELETE operations
- [ ] Tests run in CI `test-integration` job against PostgreSQL (not SQLite)
- [ ] A migration that drops RLS policy causes these tests to fail

---

## P2 — Quality / Coverage / Reliability

---

### BL-013 — Improve `security.py` test coverage: 38% → 75%+
**Priority:** P2 | **Status:** [DONE] <!-- completed: 2026-03-07 --> | **Repo:** [BOTH]

**Problem:**
`api/api/security.py` is the most security-critical file in the codebase but
has only 38% line coverage. Untested paths include: KMS mode token exchange,
OIDC token validation, credential encryption/decryption, SSRF blocking logic,
and token revocation edge cases.

**Fix:**
Write comprehensive unit tests in `api/tests/test_security_coverage.py` covering:
- All 4 auth modes: dev, jwt, kms_exchange, oidc_onprem
- JWT expiry, invalid signature, wrong algorithm
- KMS key derivation (mocked KMS client)
- OIDC JWKS fetch, token validation, expired OIDC token
- SSRF protection: private IPs (10.x, 172.16-31.x, 192.168.x, 127.x, 169.254.x),
  IPv6 private ranges, metadata endpoint URLs
- Credential encryption round-trip (encrypt → decrypt → matches)
- Token revocation: revoke token, validate revoked token (returns False)
- `CredentialEncryptor`: encrypt, decrypt, invalid key, corrupted ciphertext
- `TokenManager` initialization failure modes

**Depends-on:** BL-005 (None guard), BL-009 (key separation), BL-010 (rotation)
— tests should cover fixed behavior

**Acceptance criteria:**
- [ ] `python -m pytest api/tests/test_security_coverage.py -v` all pass
- [ ] `security.py` coverage ≥ 75% (`--cov-report=term-missing`)
- [ ] All 4 auth modes have at least one happy-path and one failure-path test
- [ ] SSRF tests cover all private IP ranges and metadata URLs

---

### BL-014 — AI engine: add router-level HTTP tests (all 7 routers at 0%)
**Priority:** P2 | **Status:** [DONE] <!-- completed: 2026-03-07 --> | **Repo:** [BOTH]

**Problem:**
All 7 `ai_engine/routers/*.py` files are at 0% test coverage:
`cache.py`, `cost.py`, `fragility.py`, `optimize.py`, `predictions.py`,
`risk.py`, `semantic.py`. The engine logic is tested but the HTTP layer
is completely untested — wrong response codes, missing auth checks, or
schema mismatches would only be caught in production.

**Fix:**
Add `ai_engine/tests/test_routers.py` using FastAPI `TestClient`:
```python
from fastapi.testclient import TestClient
from ai_engine.main import create_app

@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("AI_ENGINE_SHARED_SECRET", "test-secret-for-ci")
    app = create_app()
    return TestClient(app)
```

Cover for each router:
- Happy-path request with valid payload → 200 + correct schema
- Missing required fields → 422 (Pydantic validation)
- Missing/invalid auth header → 401
- Oversized body → 413 (body limit middleware)
- Correct response model (all required fields present)

**Depends-on:** BL-004 (tests should pass), BL-015 (ensures main.py is tested)

**Acceptance criteria:**
- [ ] All 7 routers have at least 3 tests (happy, validation, auth)
- [ ] Router coverage ≥ 80%
- [ ] `ai_engine/main.py` coverage ≥ 70% (BL-015 may cover this jointly)
- [ ] `python -m pytest ai_engine/tests/test_routers.py -v` all pass

---

### BL-015 — AI engine: add `main.py` and `middleware.py` integration tests
**Priority:** P2 | **Status:** [DONE] <!-- completed: 2026-03-07 --> | **Repo:** [BOTH]

**Problem:**
`ai_engine/ai_engine/main.py` (0% coverage) and `middleware.py` (0% coverage)
contain the application startup, lifespan hooks, auth middleware, and CORS
configuration. None of this is exercised in any test.

**Fix:**
Add `ai_engine/tests/test_app_lifecycle.py`:
- App creation succeeds with valid env vars
- `/health` endpoint returns 200
- Shared secret middleware: valid secret header → passes through
- Shared secret middleware: missing header → 401
- Shared secret middleware: wrong secret → 401
- CORS headers present on OPTIONS requests
- Body limit middleware rejects 1MB+1 byte body with 413
- Lifespan startup/shutdown runs without error

**Depends-on:** BL-004, BL-014 (same test infrastructure)

**Acceptance criteria:**
- [ ] `main.py` coverage ≥ 70%
- [ ] `middleware.py` coverage ≥ 80%
- [ ] `python -m pytest ai_engine/tests/test_app_lifecycle.py -v` all pass

---

### BL-016 — Add circuit breaker for AI engine HTTP calls
**Priority:** P2 | **Status:** [DONE] <!-- completed: 2026-03-07 --> | **Repo:** [BOTH]

**Problem:**
`api/api/services/ai_client.py` calls the AI engine with a hardcoded 10s
timeout but no circuit breaker. If the AI engine is slow or degraded, every
API request that invokes it blocks for 10 seconds, exhausting the thread pool
and degrading the entire API — a cascading failure.

**Fix:**
Add `tenacity` (already likely in the project) or `pybreaker` for circuit
breaking. `tenacity` is preferred for consistency with existing retry patterns:

```python
from tenacity import (
    retry, stop_after_attempt, wait_exponential,
    retry_if_exception_type, CircuitBreaker,
)

# Circuit breaker: open after 5 consecutive failures, reset after 30s
_ai_circuit_breaker = CircuitBreaker(
    fail_max=5,
    reset_timeout=30,
    exclude=[httpx.HTTPStatusError],  # don't trip on 4xx
)

async def get_risk_score(self, payload: dict) -> dict | None:
    try:
        async with _ai_circuit_breaker:
            response = await self._client.post("/risk/score", json=payload, timeout=10.0)
            response.raise_for_status()
            return response.json()
    except CircuitBreakerError:
        logger.warning("AI engine circuit breaker open — returning None")
        return None
    except (httpx.TimeoutException, httpx.ConnectError):
        logger.warning("AI engine unavailable — returning None")
        return None
```

All AI engine callers must handle `None` return gracefully (advisory engine,
never blocks the core flow).

**Depends-on:** Nothing (advisory engine being None-safe is already the design)

**Acceptance criteria:**
- [ ] After 5 consecutive AI engine failures, subsequent calls return `None`
      immediately (no HTTP attempt, no 10s wait)
- [ ] Circuit resets after `reset_timeout` seconds
- [ ] AI engine being completely down does not degrade API response times
      beyond a single failed request
- [ ] `python -m pytest api/tests/ -v -k ai_client` all pass

---

### BL-017 — Persist event bus to database (replace in-memory queue)
**Priority:** P1 | **Status:** [DONE] <!-- completed: 2026-03-07 --> | **Repo:** [BOTH]
**Escalated:** Upgraded P2 → P1 during self-review (2026-03-07). Billing events and audit entries are in-scope — silent loss is a correctness/revenue bug, not just a reliability gap.

**Problem:**
`api/api/services/event_bus.py` uses an in-memory queue. Events published
between a successful DB write and the delivery are lost if the process
crashes — billing events, audit entries, and webhook notifications can
be silently dropped.

**Fix:**
Replace the in-memory queue with a PostgreSQL-backed outbox table:
1. Add `EventOutboxTable` to `core_engine/state/tables.py`:
   ```python
   class EventOutboxTable(Base):
       __tablename__ = "event_outbox"
       id: int (PK, autoincrement)
       tenant_id: str
       event_type: str
       payload: JSON
       created_at: datetime
       processed_at: datetime | None  (null = pending)
       retry_count: int (default 0)
       last_error: str | None
   ```
2. Publish: INSERT into `event_outbox` inside the same transaction as the
   business operation (transactional outbox pattern).
3. Background poller: `asyncio` task that polls `WHERE processed_at IS NULL`
   every 1 second, dispatches to webhooks/subscribers, marks `processed_at`.
4. Dead-letter: after 5 retries, mark as `processed_at = now()` and set
   `last_error` — emit a warning log and metrics counter.
5. Add Alembic migration for the new table.
6. Add `GET /admin/event-outbox/pending` endpoint (admin role only) for monitoring.

**Depends-on:** BL-007 for Redis is NOT a dependency — this is a DB-backed
approach that works without Redis.

**Acceptance criteria:**
- [ ] Events survive process crash (verified by killing worker mid-flight)
- [ ] Failed deliveries retry up to 5 times with exponential backoff
- [ ] Dead-lettered events are queryable via admin endpoint
- [ ] Alembic migration applies cleanly (`make migrate`)
- [ ] `python -m pytest api/tests/test_event_bus.py -v` all pass

---

### BL-018 — Atomic LLM budget enforcement (fix race condition)
**Priority:** P1 | **Status:** [DONE] <!-- completed: 2026-03-07 --> | **Repo:** [BOTH]
**Escalated:** Upgraded P2 → P1 during self-review (2026-03-07). Race condition allows N concurrent requests to all proceed past a tight budget, with no upper bound on overrun.

**Problem:**
`ai_engine/ai_engine/engines/budget_guard.py` reads current spend, compares
to limit, then proceeds — non-atomically. Two concurrent requests can both
read spend < limit and both proceed, causing the budget to be exceeded by
`max_concurrent_requests * max_single_request_cost`.

**Fix:**
Replace the read-compare-proceed pattern with a DB-level atomic increment:
```sql
-- PostgreSQL atomic increment with limit enforcement
UPDATE tenant_budget
SET current_spend = current_spend + :cost
WHERE tenant_id = :tenant_id
  AND current_spend + :cost <= budget_limit
RETURNING current_spend;
```
If `RETURNING` yields no rows, the budget was exceeded — reject the request.
For the in-memory case (no DB), use `asyncio.Lock()` around the check-and-increment.

**Depends-on:** Nothing

**Acceptance criteria:**
- [ ] Concurrent requests never exceed budget by more than the cost of a single
      request (test with 50 concurrent requests and tight budget)
- [ ] Lock acquisition does not block unrelated requests
- [ ] `python -m pytest ai_engine/tests/test_budget_guard.py -v` all pass

---

### BL-019 — Ruff auto-fix: remove 7 unused imports
**Priority:** P2 | **Status:** [DONE] <!-- completed: 2026-03-07 --> | **Repo:** [BOTH]

**Problem:**
7 ruff `F401` violations (auto-fixable unused imports) in:
- `api/api/dependencies.py:15` — `load_api_settings`
- `api/api/main.py:81` — `os`
- `api/api/routers/event_subscriptions.py:16` — `HTTPException` (unused)
- `api/api/routers/runs.py:9` — `HTTPException` (unused)
- `api/api/services/auth_service.py:21` — `AuthMode`, `TokenConfig`

**Fix:**
```bash
cd "/Users/aronriley/Developer/GitHub Repos/IronLayer/ironlayer_OSS"
.venv/bin/python -m ruff check api/api/dependencies.py api/api/main.py \
    api/api/routers/event_subscriptions.py api/api/routers/runs.py \
    api/api/services/auth_service.py --fix
```
Verify each removal doesn't break anything (some may be used dynamically).

**Note:** `runs.py` imports `HTTPException` but doesn't use it — confirm it's
not needed by reviewing the file. `event_subscriptions.py` has the same.

**Depends-on:** Nothing (but do AFTER BL-001 to avoid confusion)

**Acceptance criteria:**
- [ ] `ruff check api/api/` reports zero F401 errors
- [ ] All API tests still pass after import cleanup

---

### BL-020 — Fix mypy `no-any-return` in services and security layers
**Priority:** P2 | **Status:** [DONE] <!-- completed: 2026-03-07 --> | **Repo:** [BOTH]

**Problem:**
39 mypy errors in `api/`, predominantly `no-any-return` in service files
(`analytics_service.py`, `ai_feedback_service.py`, `plan_service.py`,
`auth_service.py`, `quota_service.py`, `environment_service.py`,
`reconciliation_service.py`) and in `security.py`. These indicate return type
annotations that don't match the actual returned values.

**Fix strategy:**
- For SQLAlchemy ORM results: use `cast()` or proper typed result extraction
- For `security.py`: fix `dict[str, Any]` vs `Any` return from JWT decode
- Do NOT use `# type: ignore` as a workaround — fix the underlying type

**Depends-on:** Nothing

**Acceptance criteria:**
- [ ] `mypy api/api/services/ --ignore-missing-imports` zero `no-any-return` errors
- [ ] `mypy api/api/security.py --ignore-missing-imports` zero errors
- [ ] No new test failures introduced

---

### BL-021 — Register `pytest.mark.slow` in pyproject.toml
**Priority:** P2 | **Status:** [DONE] <!-- completed: 2026-03-07 --> | **Repo:** [BOTH]

**Problem:**
`ai_engine/tests/test_gold_dataset_regression.py` uses `@pytest.mark.slow` but
the marker is not registered in `ai_engine/pyproject.toml`. This produces
`PytestUnknownMarkWarning` on every test run, polluting output and potentially
causing `--strict-markers` to fail in future CI hardening.

**Fix:**
Add to `ai_engine/pyproject.toml` and `core_engine/pyproject.toml`:
```toml
[tool.pytest.ini_options]
markers = [
    "slow: marks tests as slow (deselect with '-m \"not slow\"')",
    "integration: marks tests requiring live external services",
    "benchmark: marks performance benchmark tests",
]
```

**Depends-on:** Nothing

**Acceptance criteria:**
- [ ] `python -m pytest ai_engine/tests/ -v` zero `PytestUnknownMarkWarning`
- [ ] `python -m pytest -m slow ai_engine/tests/` runs only slow-marked tests

---

### BL-022 — Align and enforce coverage thresholds across all packages
**Priority:** P2 | **Status:** [DONE] <!-- completed: 2026-03-07 --> | **Repo:** [BOTH]

**Problem:**
Coverage thresholds are inconsistent and don't enforce meaningful minimums:
- `core_engine`: 70% in Makefile, 60% in CI
- `api`: no explicit `--cov-fail-under` in CI
- `ai_engine`: no explicit threshold in CI
- `frontend`: 20% in CI (not meaningful)
- `cli`: never runs (BL-003)

**Fix:**
After BL-003, BL-013, BL-014, BL-015 raise coverage, enforce:

| Package | Current | Target | Method |
|---------|---------|--------|--------|
| api | 74% | 75% min | `--cov-fail-under=75` in CI |
| ai_engine | 72% | 75% min | `--cov-fail-under=75` in CI |
| core_engine | 65% | 70% min | `--cov-fail-under=70` in CI |
| cli | N/A | 65% min | `--cov-fail-under=65` after BL-003 |
| frontend | 20% | 40% min | Update CI threshold |

Edit `.github/workflows/ci.yml` to add `--cov-fail-under` to each test job.

**Depends-on:** BL-003, BL-013, BL-014, BL-015 (must raise coverage first)

**Acceptance criteria:**
- [ ] CI fails if any package drops below its threshold
- [ ] Thresholds documented in this backlog and in `CLAUDE.md`
- [ ] No package is currently below its threshold when thresholds are enforced

---

### BL-023 — Add container image Trivy scan to CI
**Priority:** P2 | **Status:** [DONE] <!-- completed: 2026-03-07 -->

**Problem:**
The CI `security-scan` job runs `trivy fs .` (filesystem scan). It does NOT
scan the built Docker image layers. Vulnerable packages added during the
Docker build (e.g., in a `RUN apt-get install` step or via pip in the image
build context) would not be caught.

**Fix:**
After the `build-and-push` job, add an image scan job:
```yaml
scan-image:
  needs: [build-and-push]
  runs-on: ubuntu-latest
  steps:
    - name: Scan container image
      uses: aquasecurity/trivy-action@0.30.0
      with:
        image-ref: ${{ env.ACR_REGISTRY }}/ironlayer-api:${{ github.sha }}
        format: sarif
        output: trivy-image-results.sarif
        severity: CRITICAL,HIGH
        exit-code: 1
```

**Depends-on:** Nothing

**Acceptance criteria:**
- [ ] CI scans the built image (not just filesystem)
- [ ] CRITICAL/HIGH CVEs in image layers fail the build
- [ ] SARIF report uploaded to GitHub Security tab
- [ ] Existing `.trivyignore` suppressions still apply

---

### BL-024 — Improve API service layer test coverage
**Priority:** P2 | **Status:** [DONE] <!-- completed: 2026-03-07 --> | **Repo:** [BOTH]

**Problem:**
Several API service files have dangerously low coverage:
- `simulation_service.py`: 20%
- `ai_client.py`: 30%
- `reconciliation_service.py`: 45%
- `approvals.py` router: 38%

`approvals.py` is particularly concerning — approval gates are a security
control, and untested approval logic could allow unauthorized plan execution.

**Fix:**
Add tests covering:
- `simulation_service.py`: happy path, model-not-found, invalid time range
- `ai_client.py`: request sanitization, circuit breaker behavior (mock),
  timeout handling, malformed response handling
- `reconciliation_service.py`: reconciliation succeeds, partial failure,
  snapshot mismatch detection
- `approvals.py`: create approval, approve, reject, expired approval,
  duplicate approval attempt, unauthorized approver

**Depends-on:** BL-016 (ai_client circuit breaker — test the new behavior)

**Acceptance criteria:**
- [ ] `simulation_service.py` ≥ 70%
- [ ] `ai_client.py` ≥ 70%
- [ ] `reconciliation_service.py` ≥ 70%
- [ ] `approvals.py` ≥ 75%
- [ ] All new tests pass

---

### BL-025 — Pin base Docker images to specific digest
**Priority:** P2 | **Status:** [DONE] <!-- completed: 2026-03-07 -->

**Problem:**
Dockerfiles reference `python:3.11-slim` (floating tag). A new patch release
could introduce a regression or vulnerability into the build without any
explicit change. CI builds on different days could use different base images,
making builds non-reproducible.

**Fix:**
Pin to digest in Dockerfiles:
```dockerfile
# Before
FROM python:3.11-slim

# After (example — get current digest with: docker pull python:3.11-slim; docker inspect python:3.11-slim --format='{{index .RepoDigests 0}}')
FROM python:3.11-slim@sha256:<digest>
```
Add a comment noting the Python version and the date pinned.
Add a monthly Dependabot or GitHub Actions cron job to update the digest.

**Depends-on:** Nothing

**Acceptance criteria:**
- [ ] All Dockerfiles use digest-pinned base images
- [ ] Cron job or Dependabot config to auto-update digest monthly
- [ ] Builds are reproducible (same digest produces same layers)

---

### BL-026 — Add `HEALTHCHECK` to all Dockerfiles
**Priority:** P2 | **Status:** [DONE] <!-- completed: 2026-03-07 -->

**Problem:**
Docker and orchestrators (Azure Container Apps, ECS) use `HEALTHCHECK` to
determine whether a container is ready for traffic. Without it, a container
that starts the process but hangs before binding the port will receive traffic
and return 502s.

**Fix:**
Add to each service's Dockerfile:
```dockerfile
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1
```

**Depends-on:** Nothing

**Acceptance criteria:**
- [ ] All Dockerfiles have `HEALTHCHECK`
- [ ] `docker inspect <container>` shows `Health.Status: healthy` after startup
- [ ] Container apps route traffic only to healthy containers

---

### BL-027 — Validate Alembic migrations in CI before deploy
**Priority:** P2 | **Status:** [DONE] <!-- completed: 2026-03-07 -->

**Problem:**
The CI pipeline deploys to production without verifying that Alembic migrations
apply cleanly against the current HEAD. A broken migration would fail on first
deploy, causing downtime.

**Fix:**
Add a `validate-migrations` CI job that:
1. Spins up a PostgreSQL test container (reuse the existing `test-core` service config)
2. Runs `alembic upgrade head`
3. Runs `alembic downgrade -1` (verify rollback works)
4. Runs `alembic upgrade head` again (verify re-apply)
This job must pass before the `deploy` job can run.

**Depends-on:** Nothing

**Acceptance criteria:**
- [ ] CI has `validate-migrations` job gating `deploy`
- [ ] Broken migration fails CI before any deploy attempt
- [ ] Up + down + up migration cycle verified in CI

---

## P3 — Enhancements / Future-Proofing

---

### BL-028 — Improve `core_engine` repository coverage (29% on key files)
**Priority:** P3 | **Status:** [DONE] <!-- started: 2026-03-07, done: 2026-03-07 --> | **Repo:** [BOTH]

**Problem:**
`core_engine/state/repository.py` is ~3700 lines and a central dependency for
all other packages but has only 29% line coverage. Many repository methods are
called through API tests but only via integration paths, not unit tested directly.

**Fix:**
Add `core_engine/tests/unit/test_repository.py` with targeted unit tests for:
- CRUD operations on all repository classes (ModelRepository, RunRepository,
  WatermarkRepository, etc.)
- Filter methods (list_filtered, search)
- Error paths (not found, duplicate key, tenant isolation enforcement)
- Watermark get/set/clear operations

**Depends-on:** Nothing

**Acceptance criteria:**
- [ ] `core_engine/state/repository.py` coverage ≥ 65%
- [ ] All new tests use async fixtures with SQLite (no PostgreSQL required)

---

### BL-029 — Add OpenTelemetry distributed tracing
**Priority:** P3 | **Status:** [DONE] <!-- started: 2026-03-07, done: 2026-03-07 --> | **Repo:** [BOTH]

**Problem:**
Prometheus metrics exist but there are no distributed traces. A request that
flows API → AI engine → core_engine is impossible to follow in production.
The `trace_context` middleware sets trace IDs in headers but doesn't export
spans to any trace collector.

**Fix:**
1. Add `opentelemetry-sdk`, `opentelemetry-instrumentation-fastapi`,
   `opentelemetry-instrumentation-httpx`, `opentelemetry-exporter-otlp` deps.
2. Configure OTel in `api/api/main.py` `lifespan` with OTLP exporter
   (configurable via `OTEL_EXPORTER_OTLP_ENDPOINT` env var, no-op if unset).
3. Instrument FastAPI automatically (`FastAPIInstrumentor`).
4. Instrument `httpx` client in `ai_client.py` for AI engine spans.
5. Propagate `traceparent` header from API → AI engine → core_engine.
6. Add `OTEL_EXPORTER_OTLP_ENDPOINT` to `.env.example`.

**Depends-on:** Nothing

**Acceptance criteria:**
- [ ] When `OTEL_EXPORTER_OTLP_ENDPOINT` is set, traces appear in collector
- [ ] When unset, no error — OTel SDK no-ops silently
- [ ] `traceparent` header propagates through API → AI engine
- [ ] No performance regression > 1ms p99 on API endpoints (benchmark test)

---

### BL-030 — Add SLO definitions and alerting rules
**Priority:** P3 | **Status:** [DONE] <!-- started: 2026-03-07, done: 2026-03-07 --> | **Repo:** [INFRA]

**Problem:**
There are no SLO definitions, error budgets, or alerting rules. Monitoring is
Prometheus-based but without targets, dashboards show metrics with no context
for what constitutes "healthy".

**Fix:**
1. Define SLOs in `infra/monitoring/slos.yml`:
   - API availability: 99.9% (43.8 min/month downtime budget)
   - API p99 latency: < 500ms for plan endpoints
   - AI engine availability: 99.5% (advisory service — softer SLO)
   - Plan determinism: 100% (zero tolerance)
2. Add Prometheus alerting rules for: error rate > 1%, p99 > 500ms, 5xx spike.
3. Add Grafana dashboard JSON for the key metrics.
4. Document SLOs in README.

**Depends-on:** BL-029 (tracing makes SLO measurement more accurate)

**Acceptance criteria:**
- [ ] SLO YAML defined and reviewed by team
- [ ] Alert rules fire correctly in test environment (use `amtool` to validate)
- [ ] Grafana dashboard displays error budget burn rate

---

### BL-031 — Audit log retention and archival policy
**Priority:** P3 | **Status:** [DONE] <!-- started: 2026-03-07, completed: 2026-03-07 --> | **Repo:** [BOTH]

**Problem:**
The audit log (`audit_log` table) grows unboundedly. There is no TTL, archival
policy, or retention limit. For GDPR compliance, the right-to-erasure requires
that PII in audit logs be deleted or anonymized after a defined retention period.

**Fix:**
1. Add `retention_days: int` to tenant config (default: 365).
2. Add a background cleanup task in `api/api/services/audit_service.py` that
   runs nightly: `DELETE FROM audit_log WHERE created_at < NOW() - INTERVAL ':days days'`.
3. For GDPR right-to-erasure: anonymize (replace with `[REDACTED]`) rather than
   delete, preserving audit count integrity.
4. Add Alembic migration adding `retention_days` to tenant config table.
5. Document retention policy in a `PRIVACY.md` at repo root.

**Depends-on:** Nothing

**Acceptance criteria:**
- [x] `retention_days` column added to `TenantConfigTable` (default 365) — `tables.py`
- [x] Alembic migration `025_add_audit_retention_days.py` adds column with `server_default=365`
- [x] `AuditRepository.cleanup_old_entries(retention_days)` — deletes old entries, returns rowcount
- [x] `AuditRepository.anonymize_user_entries(user_id)` — sets actor=[REDACTED], clears metadata_json
- [x] `AuditService.cleanup_old_logs(retention_days)` — delegates to repo, commits session
- [x] `AuditService.anonymize_user_data(user_id)` — delegates to repo, commits session
- [x] `DELETE /api/v1/audit/users/{user_id}` endpoint — ADMIN role, calls anonymize_user_data
- [x] 9 new tests in `api/tests/test_audit_retention.py` — all pass (1455 total, 0 failed)
- [x] Changes synced to infra repo

---

### BL-032 — ML model registry and versioning
**Priority:** P3 | **Status:** [DONE] <!-- started: 2026-03-07, done: 2026-03-07 --> | **Repo:** [BOTH]

**Problem:**
`CostPredictor` and `FailurePredictor` load pre-trained joblib models from
disk with no versioning, drift detection, or scheduled retraining. The training
pipeline is not in the repo. An outdated model silently gives bad predictions.

**Fix:**
1. Add `models/` directory with semantic versioning: `cost_model_v1.2.0.joblib`.
2. Add `ai_engine/ai_engine/ml/model_registry.py` with:
   - `load_model(name, version=None)` — loads specific or latest version
   - `record_prediction(model_name, features, prediction, actual=None)` — for drift tracking
   - `drift_check(model_name)` — PSI/KS test on recent predictions vs training distribution
3. Add training scripts to `ai_engine/scripts/train_cost_model.py` and
   `train_failure_model.py` — these should be runnable locally and in CI.
4. Add `make train-models` target.
5. Document the retraining schedule (recommended: monthly or on drift alert).

**Depends-on:** Nothing

**Acceptance criteria:**
- [ ] Model version is logged with every prediction
- [ ] Drift detection alerts when PSI > 0.2 (significant drift)
- [ ] Training scripts run end-to-end in CI (with synthetic data)
- [ ] Model can be rolled back by changing version string in config

---

### BL-033 — Generate SBOM in CI (CycloneDX)
**Priority:** P3 | **Status:** [DONE] <!-- started: 2026-03-07, done: 2026-03-07 --> | **Repo:** [INFRA]

**Problem:**
No Software Bill of Materials (SBOM) is generated. Enterprise customers
increasingly require SBOMs for supply chain security compliance (NTIA, EO 14028).

**Fix:**
Add to CI publish job:
```yaml
- name: Generate SBOM
  run: |
    pip install cyclonedx-bom
    cyclonedx-py poetry > sbom.json
- name: Upload SBOM
  uses: actions/upload-artifact@v4
  with:
    name: sbom-${{ github.sha }}
    path: sbom.json
```
Attach SBOM to GitHub Release as an asset.

**Depends-on:** Nothing

**Acceptance criteria:**
- [ ] SBOM generated on every release tag
- [ ] SBOM attached to GitHub Release
- [ ] SBOM validates with `cyclonedx validate`

---

### BL-034 — Add canary / blue-green deploy strategy
**Priority:** P3 | **Status:** [DONE] <!-- started: 2026-03-07, done: 2026-03-07 --> | **Repo:** [INFRA]

**Problem:**
The CI deploy step does a direct production update: `az containerapp update`.
If the new image has a runtime bug that tests didn't catch, all production
traffic is immediately affected. There is no rollback automation.

**Fix:**
Implement blue-green using Azure Container Apps traffic splitting:
1. Deploy new revision with `--traffic-weight 0` (canary, no traffic).
2. Run smoke tests against the canary revision directly.
3. If smoke tests pass, shift traffic 10% → 50% → 100% over 10 minutes.
4. If smoke tests fail, `az containerapp ingress traffic set` back to old revision.
5. Add `make rollback` target for manual emergency rollback.

**Depends-on:** Nothing

**Acceptance criteria:**
- [ ] Deploy does not immediately send 100% traffic to new revision
- [ ] Failed smoke tests trigger automatic rollback within 5 minutes
- [ ] `make rollback` rolls back to previous revision
- [ ] Runbook documented in `docs/deployment.md`

---

### BL-035 — Add API versioning (`/v1/` prefix)
**Priority:** P3 | **Status:** [DONE] <!-- done: 2026-03-07, all routes already at /api/v1 prefix in main.py --> | **Repo:** [BOTH]

**Problem:**
All API routes are unprefixed (e.g., `/models`, `/plans`, `/runs`). When a
breaking change is needed (changing response shape, removing a field), it's
impossible to version without breaking all existing clients simultaneously.

**Fix:**
Add a `/v1` prefix to all routers in `main.py`:
```python
app.include_router(models_router, prefix="/v1")
```
Keep the old unversioned routes alive temporarily with a deprecation header:
`Deprecation: true`, `Link: </v1/models>; rel="successor-version"`.

**Depends-on:** Nothing (but coordinate with frontend team before rolling out)

**Acceptance criteria:**
- [ ] All routers accessible at `/v1/<existing-path>`
- [ ] Old paths redirect to `/v1/` with deprecation headers
- [ ] OpenAPI docs show `/v1/` paths
- [ ] Frontend updated to use `/v1/` paths

---

### BL-042 — P3 post-sprint quality corrections
**Priority:** P3 | **Status:** [DONE] <!-- started: 2026-03-07, completed: 2026-03-07 --> | **Repo:** [BOTH]

**Problem:**
Post-P3 code review identified 8 genuine bugs introduced during the sprint:

1. `audit_service.py`: `cleanup_old_logs`/`anonymize_user_data` reach into `self._repo._session` (private collaborator attribute) to commit — breaks encapsulation and interferes with framework transaction management.
2. `anonymize_user_entries` mutates `actor`/`metadata_json` (hashed fields) without updating `entry_hash` → `verify_chain` permanently returns `False` for tenants with GDPR anonymization applied (silent audit integrity failure).
3. No `is_anonymized` sentinel on `AuditLogTable` — `verify_chain` cannot distinguish hash-chain breaks caused by legitimate GDPR erasure from real tampering.
4. `ModelRegistry._current_version` does an unordered linear scan through dict keys — returns first cached version, not most-recently-loaded (wrong version tag on predictions when multiple versions are cached).
5. `PredictionRecord.actual` docstring says "filled in later via an update call" — no update API exists.
6. SBOM CI step uses `uv pip install cyclonedx-bom` — pollutes workspace venv so cyclonedx-bom appears in the SBOM it generates.
7. `rollback.sh` uses `AZURE_RESOURCE_GROUP` env var; `canary_deploy.sh` uses `RESOURCE_GROUP` — inconsistency makes `make rollback` not work without manual env setup.
8. `rollback.sh` JMESPath filter `[?properties.active]` is unreliable — Azure marks ALL deployed revisions as `active`; query may return more revisions than expected.
9. `APIErrorSpike` alert pages at >1% error rate, creating duplicate pages alongside `APIHighErrorRateFastBurn` (which also pages at 1.44% error rate) — double-paging for the same incident.

**Fix:**
See acceptance criteria below.

**Depends-on:** BL-031 (audit retention), BL-032 (model registry), BL-033 (SBOM), BL-034 (canary deploy), BL-030 (alerting)

**Acceptance criteria:**
- [x] `AuditService` stores `self._session` directly; `cleanup_old_logs` / `anonymize_user_data` commit via `self._session.commit()`, not `self._repo._session.commit()`
- [x] `AuditLogTable` has `is_anonymized: bool` column (default `False`, Alembic migration 026)
- [x] `anonymize_user_entries` sets `is_anonymized=True` alongside redaction
- [x] `verify_chain` skips hash recomputation for `is_anonymized=True` entries (chain link still advanced), returns `(True, checked)` plus anonymized count in log
- [x] `ModelRegistry._active_version: dict[str, str]` tracks most-recently-loaded version per model name; `_current_version` uses it instead of dict iteration
- [x] `PredictionRecord.actual` docstring corrected: removes "update call" reference
- [x] SBOM CI step uses `uv tool install cyclonedx-bom` (isolated tool env, not workspace venv)
- [x] `rollback.sh` uses `RESOURCE_GROUP` (matching `canary_deploy.sh`); `Makefile` rollback target sets it correctly
- [x] `rollback.sh` JMESPath query uses `sort_by(@, &properties.createdTime)[-2].name` (no unreliable active filter)
- [x] `APIErrorSpike` raised to >2% threshold and demoted to `severity: ticket` (no more double-paging with fast-burn)
- [x] All affected tests updated; suite remains green

---

## Process / Tooling Items

---

### BL-036 — Set up `make sync-ai` equivalent for infra→OSS rule propagation
**Priority:** P2 | **Status:** [DONE] <!-- completed: 2026-03-07 -->

**Problem:**
`CLAUDE.md`, `LESSONS.md`, and cursor rules exist in `ironlayer_infra` as the
canonical source. Changes must manually be kept in sync with `ironlayer_OSS`.
The `sync-to-public.yml` workflow only runs on push to `main` and has a delay.

**Fix:**
Add a `make sync-rules` target in the Makefile that copies non-sensitive
AI config files from `ironlayer_infra` to `ironlayer_OSS`:
```makefile
INFRA_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
OSS_DIR   := $(INFRA_DIR)/../ironlayer_OSS

sync-rules:
	cp $(INFRA_DIR)/CLAUDE.md $(OSS_DIR)/CLAUDE.md
	cp $(INFRA_DIR)/AGENTS.md $(OSS_DIR)/AGENTS.md
	@echo "Rules synced infra → OSS. Review changes before committing."
```

**Depends-on:** Nothing

**Acceptance criteria:**
- [ ] `make sync-rules` copies CLAUDE.md and AGENTS.md to OSS repo
- [ ] Sync is idempotent (running twice makes no changes)
- [ ] Sensitive infra content (BACKLOG.md, LESSONS.md) is NOT synced to OSS

---

### BL-040 — Fix AI engine async/sync impedance mismatch + wire BudgetGuard
**Priority:** P1 | **Status:** [DONE] <!-- started: 2026-03-07, done: 2026-03-07 --> | **Repo:** [OSS]
**Added:** 2026-03-07 P0-area review

**Problem (4 separate issues, all root-caused to the same design gap):**

1. **`LLMClient._call_llm()` was sync — blocks the event loop.**
   `LLMClient` used the sync `anthropic.Anthropic()` SDK and a plain sync
   `_call_llm()` method. This method is invoked from async FastAPI endpoints
   via `SemanticClassifier` and `SQLOptimizer`. A single LLM call (typically
   500ms–3s) starved the entire uvicorn event loop, stalling all concurrent
   requests for the duration. Under LLM-enabled load this is a denial-of-service
   to all concurrent tenants.

2. **`BudgetGuard` was never wired (BL-018 work was dead code).**
   `main.py` called `LLMClient(settings)` — no `budget_guard=` argument.
   The `LLMClient` constructor accepted `budget_guard` but the lifespan never
   passed one. All per-tenant LLM budget enforcement was silently no-op in
   production despite BL-018 being marked `[DONE]`.

3. **`SemanticClassifier.classify()` and `SQLOptimizer.optimize()` were sync.**
   Both engine methods were plain `def`, not `async def`. The async FastAPI
   routers called them without `await`, which worked silently (sync functions
   return values, not coroutines), but meant any I/O inside was blocking.

4. **`EvaluationHarness.run_full_evaluation()` was sync.**
   The evaluation harness called `classify()` and `optimize()` synchronously.
   After making those methods async, the harness returned coroutine objects
   (not results), causing `AttributeError: 'coroutine' object has no attribute
   'change_type'` in all regression tests.

**Additional problem: AI engine has no DB for usage tracking.**
`BudgetGuard` requires a usage repo but the AI engine is stateless with no
database access. Passing a DB-backed repo would require adding a DB connection,
which contradicts the engine's advisory-only stateless design.

**Fix:**

1. **`llm_client.py`** — Full async rewrite:
   - Use `AsyncAnthropic` (async SDK) instead of `Anthropic`
   - `classify_change()`, `suggest_optimization()`, `_call_llm()` → `async def`
   - Budget: `await self._budget_guard.check_budget()` before call
   - Usage: `await self._budget_guard.record_usage(...)` in `finally` block
   - Removed all `ThreadPoolExecutor`/`asyncio.get_event_loop()` bridge code

2. **`semantic_classifier.py`** — `classify()` and `_llm_enrich()` → `async def`;
   `await self._llm.classify_change(...)` and `await self._llm_enrich(...)`

3. **`sql_optimizer.py`** — `optimize()` and `_llm_suggestions()` → `async def`;
   `await self._llm.suggest_optimization(...)` and `await self._llm_suggestions(...)`

4. **`config.py`** — Added `llm_daily_budget_usd: float | None = None` and
   `llm_monthly_budget_usd: float | None = None`

5. **New: `engines/in_memory_usage_repo.py`** — `InMemoryLLMUsageRepo`:
   in-process list of usage records (no DB), satisfies `BudgetGuard`'s repo
   interface. Auto-prunes records older than `max_age_days` (default 31) on
   each write.

6. **`main.py`** — Create `BudgetGuard` in lifespan when budget settings are
   configured; pass `budget_guard=budget_guard` to `LLMClient(settings, ...)`.

7. **Routers** — `semantic.py` and `optimize.py`: added `await` to the
   `classify()` and `optimize()` calls.

8. **`evaluation/harness.py`** — `run_full_evaluation()` → `async def`;
   `await` both `classify()` and `optimize()` calls inside.

9. **Tests** — `test_semantic_classifier.py` (29 methods), `test_sql_optimizer.py`
   (25 methods), `test_evaluation_harness.py` (full rewrite with `AsyncMock`
   for `classify` and `optimize`), `test_gold_dataset_regression.py` (all
   `run_full_evaluation` calls awaited) — all converted to `async def`.

**Depends-on:** BL-018 (BudgetGuard implementation)

**Acceptance criteria:**
- [x] `LLMClient._call_llm` is `async def`; uses `AsyncAnthropic`
- [x] `SemanticClassifier.classify()` is `async def`
- [x] `SQLOptimizer.optimize()` is `async def`
- [x] `BudgetGuard` is instantiated in `main.py` lifespan when budget configured
- [x] `InMemoryLLMUsageRepo` satisfies `BudgetGuard`'s usage-repo interface
- [x] `EvaluationHarness.run_full_evaluation()` is `async def`
- [x] All 397 AI engine tests pass with 0 failures

---

### BL-037 — Update AGENTS.md with work-tracking section
**Priority:** P2 | **Status:** [DONE] <!-- completed: 2026-03-07 --> | **Repo:** [BOTH]
**Added:** 2026-03-07 self-review | Depends-on: BL-036 (sync-rules to propagate)

**Problem:**
`AGENTS.md` is the universal context file read by ALL AI tools (Claude Code,
Cursor, GitHub Copilot, etc.), but it contains no reference to `BACKLOG.md`
or the workflow rules. An AI agent working in these repos would start writing
code without knowing to check the backlog first — precisely the workflow failure
the governance setup is designed to prevent.

**Fix:**
Add a "Work Tracking" section near the top of `ironlayer_infra/AGENTS.md`
(it propagates to OSS via `make sync-rules` / BL-036):

```markdown
## Work Tracking

**All work — including AI-agent work — must be tracked before starting.**

1. Open `ironlayer_infra/BACKLOG.md` (private repo).
2. Find the item matching your task. If it doesn't exist, add it.
3. Change its status to `[IN-PROGRESS]` with today's date.
4. Only then write code.

See `ironlayer_infra/CLAUDE.md` for the full rule set. OSS repo work is
tracked in the same backlog — no changes to `ironlayer_OSS` without a
corresponding `[IN-PROGRESS]` backlog item.
```

**Depends-on:** BL-036 (so sync-rules can propagate the AGENTS.md change)

**Acceptance criteria:**
- [ ] `ironlayer_infra/AGENTS.md` has "Work Tracking" section (≥ 5 lines)
- [ ] `make sync-rules` propagates the section to `ironlayer_OSS/AGENTS.md`
- [ ] Claude Code, Cursor, and Copilot each display the section in their context panes
- [ ] No other content in AGENTS.md is changed (pure addition)

---

### BL-041 — Correct three P2 quality issues found in post-sprint review
**Priority:** P2 | **Status:** [DONE] <!-- started: 2026-03-07, done: 2026-03-07 --> | **Repo:** [BOTH]
**Added:** 2026-03-07 post-sprint code review

**Problem:**
Three genuine defects identified during post-P2-sprint code review:

1. **`time.sleep()` in circuit breaker tests** — `TestCircuitBreaker` uses
   `time.sleep(0.05)` in three tests to wait for a 10 ms `reset_timeout` to
   elapse.  This is fragile under CI load: the OS scheduler may not wake the
   process in time, causing spurious failures.  The correct approach is to
   mock `time.monotonic` and advance the clock without sleeping.

2. **`urlopen()` without explicit timeout in Dockerfile HEALTHCHECKs** —
   Both `Dockerfile.api` and `Dockerfile.ai` use
   `urllib.request.urlopen(url)` with no timeout argument.  Python's default
   socket timeout is `None` (blocks indefinitely).  Docker's `--timeout=5s`
   will eventually SIGKILL the process, but the Python process blocks until
   killed rather than gracefully timing out.  Fix: `urlopen(url, timeout=3)`.

3. **`_CircuitBreaker` docstring says "One probe request is allowed through"**
   but the implementation is soft half-open — no concurrency gate limits the
   number of concurrent probes.  The docstring creates a false expectation
   about behavior.

**Fix:**
- Replace `time.sleep()` in `TestCircuitBreaker` with `monkeypatch.setattr`
  on `"api.services.ai_client.time.monotonic"` and a mutable clock list.
  Use realistic `reset_timeout=30.0` (production default) instead of 10 ms.
- Add `timeout=3` to all four `urlopen()` HEALTHCHECK calls (api + ai, both
  repos).
- Correct `_CircuitBreaker` docstring to accurately describe soft half-open.

**Acceptance criteria:**
- [x] No `time.sleep()` calls remain in `TestCircuitBreaker`
- [x] All three circuit-breaker time tests pass without sleeping
- [x] Both `Dockerfile.api` and `Dockerfile.ai` HEALTHCHECK use `timeout=3`
- [x] `_CircuitBreaker.state` docstring accurately describes half-open behaviour
- [x] Full test suite passes (1446 passed, 0 failed)

---

---

## Security & Performance Sprint — 2026-03-07 Audit Findings

*Source: Full threat-model pass + performance baseline conducted 2026-03-07.*
*All items below are [OPEN] as of 2026-03-07.*

---

## P0 — Security Critical (must fix before any production traffic)

---

### BL-043 — Unsafe joblib deserialization in ModelRegistry (RCE vector)
**Priority:** P0 | **Status:** [OPEN] | **Repo:** [OSS]
**Audit source:** AI engine security audit 2026-03-07

**Problem:**
`ai_engine/ai_engine/ml/model_registry.py:197` calls `joblib.load(model_path)` with no
integrity verification. Joblib deserializes arbitrary Python objects; a crafted `.joblib`
file executes code at load time. If an attacker gains write access to the models directory
(compromised deployment pipeline, misconfigured filesystem permissions, or supply-chain
attack), placing a malicious file causes RCE with the service account's privileges on next
model load (startup or explicit reload call). `cost_model.py:145` does `isinstance(model,
LinearRegression)` after loading, but this is too late — `__reduce__` fires during
`joblib.load()`.

**Fix:**
1. Create `ai_engine/ai_engine/ml/model_manifest.py` — SHA256-signed manifest of every
   `.joblib` file (signed with a private key stored in Key Vault / env var).
2. Before every `joblib.load()`, compute `sha256(file_bytes)` and verify against manifest.
   Raise `SecurityError` on mismatch or missing entry.
3. CI model-publish job regenerates and signs the manifest.
4. Add `MODEL_SIGNING_PUBLIC_KEY` env var to `ai_engine/config.py`.

**Acceptance criteria:**
- [ ] `load_model()` raises `SecurityError` if file hash not in manifest
- [ ] `load_model()` raises `SecurityError` if manifest signature is invalid
- [ ] CI publishes a signed manifest alongside model files
- [ ] Tests: tampered file → SecurityError; correct file → loads successfully
- [ ] `python -m pytest ai_engine/tests/ -v -k model_registry` all pass

---

### BL-044 — Webhook HMAC verification skipped when `secret_encrypted` IS NULL
**Priority:** P0 | **Status:** [OPEN] | **Repo:** [OSS]
**Audit source:** Auth/secrets security audit 2026-03-07

**Problem:**
`api/api/routers/webhooks.py:154–199`: when `webhook_config.secret_encrypted is None`, the
HMAC signature check is skipped entirely with only a `logger.warning()`. Any unauthenticated
request can be injected as a valid webhook event for any tenant that has not migrated to
encrypted secrets. The code comment says "remove after migration 023" but there is no
database-level `NOT NULL` constraint, no backfill migration, and no enforced deadline. This
is an unauthenticated RCE-adjacent vector against plan-trigger and CI pipelines.

**Fix:**
1. Alembic migration 027: `ALTER TABLE webhook_configs ALTER COLUMN secret_encrypted SET NOT
   NULL` — backfill NULL rows with a placeholder encrypted value and notify tenants to
   reconfigure.
2. Remove the skip-HMAC `else` branch (lines 187–199) entirely.
3. Raise `HTTPException(403, "Webhook signature required")` immediately if secret cannot be
   decrypted or is absent.
4. Add a startup check: log CRITICAL if any `secret_encrypted IS NULL` rows exist.

**Acceptance criteria:**
- [ ] Migration 027 adds NOT NULL constraint with backfill for existing NULL rows
- [ ] `_verify_webhook_hmac` is called unconditionally (no skip branch remains)
- [ ] Unsigned webhook request → HTTP 403 (not 200 or processed)
- [ ] Startup logs CRITICAL if any NULL secret rows persist after migration
- [ ] New test: unsigned request against migrated config → 403

---

### BL-045 — Swagger/OpenAPI docs accessible without auth in all environments
**Priority:** P0 | **Status:** [OPEN] | **Repo:** [OSS]
**Audit source:** Frontend/Docker security audit 2026-03-07

**Problem:**
FastAPI auto-generates `/docs`, `/redoc`, and `/openapi.json`. All three are in
`_PUBLIC_PATHS` in `api/api/middleware/auth.py:221–237`, meaning any unauthenticated user
retrieves the complete API schema: all endpoints, request/response models, field names,
error codes, and business logic structure. The AI engine has the same exposure on its own
docs endpoints. This is a full reconnaissance gift to attackers.

**Fix:**
```python
# api/api/main.py
is_dev = settings.platform_env == PlatformEnv.DEV
app = FastAPI(
    title="IronLayer API",
    docs_url="/docs" if is_dev else None,
    redoc_url="/redoc" if is_dev else None,
    openapi_url="/openapi.json" if is_dev else None,
    lifespan=lifespan,
)
```
Remove `/docs`, `/redoc`, `/openapi.json` from `_PUBLIC_PATHS`. Apply same fix to
`ai_engine/ai_engine/main.py`.

**Acceptance criteria:**
- [ ] `GET /docs` → 404 in staging and prod environments
- [ ] `GET /openapi.json` → 404 in staging and prod
- [ ] `GET /docs` → 200 in dev environment (docs still work for development)
- [ ] Both API and AI engine apply the same conditional logic
- [ ] Test: assert 404 on docs endpoints when `platform_env != DEV`

---

### BL-046 — Budget check TOCTOU race: concurrent LLM calls can exceed budget N×
**Priority:** P0 | **Status:** [OPEN] | **Repo:** [OSS]
**Audit source:** AI engine performance audit 2026-03-07
**Note:** BL-018 fixed `BudgetGuard.check_budget()` atomically. This is a separate
unlocked pre-check in `LLMClient._call_llm()` that bypasses the guard entirely.

**Problem:**
`ai_engine/ai_engine/engines/llm_client.py:293–294` performs a budget pre-check OUTSIDE
the per-tenant lock:
```
remaining = await self._repo.get_remaining_budget(tenant_id)
if remaining <= 0: raise BudgetExceededError(...)
# --- lock NOT held between here and the LLM call ---
response = await client.messages.create(...)
```
Five concurrent requests all read `remaining > 0` before any acquires the lock. All five
proceed. Usage recorded in `finally`. Result: budget exceeded 5× with no enforcement. The
class docstring (lines 60–67) falsely claims "check → call → record are held under the
guard's lock."

**Fix:**
Remove the unlocked pre-check at lines 293–294. All callers must go through
`guard.guard_call(fn)` which already holds the lock correctly (per BL-018). Document that
`_call_llm()` must never be called outside the guard context.

**Acceptance criteria:**
- [ ] No budget pre-check outside the guard lock in `_call_llm()`
- [ ] Concurrent test: 10 requests, budget for 1 → exactly 1 succeeds, 9 get BudgetExceededError
- [ ] Docstring updated to accurately describe the lock contract
- [ ] `python -m pytest ai_engine/tests/ -v -k budget` all pass

---

## P1 — High Security (fix before paying customer traffic)

---

### BL-047 — Login rate limiter is in-process: defeated in multi-replica deployments
**Priority:** P1 | **Status:** [OPEN] | **Repo:** [OSS]
**Audit source:** Auth/secrets security audit 2026-03-07

**Problem:**
`api/api/middleware/login_rate_limiter.py` uses a module-level in-process singleton.
Each replica maintains separate failure counters. An attacker distributing brute-force
attempts across N replicas gets N × `max_attempts` total tries per lockout window. At the
default 3-replica deployment this is 15 attempts vs. the intended 5; at 10 replicas auto-
scaled, 50 attempts per window. This is distinct from BL-007 (general rate limiting, already
Redis-backed). The login-specific `LoginRateLimiter` was not migrated.

**Fix:**
Migrate `LoginRateLimiter` to Redis:
- Key: `login_fail:<sha256(email)>:<ip>` with `INCR` + `EXPIRE`
- Fail-open (not fail-closed) when Redis is unavailable — don't block logins if Redis is down
- Keep in-process fallback for dev environments without Redis configured

**Acceptance criteria:**
- [ ] Login attempts distributed across replicas share a single failure counter in Redis
- [ ] Lockout applied after `max_attempts` regardless of which replica receives each request
- [ ] Redis unavailability falls back to in-process counter (login not broken)
- [ ] `python -m pytest api/tests/ -v -k login_rate` all pass

---

### BL-048 — Credential encryption key has hardcoded default (fail at config, not startup)
**Priority:** P2 | **Status:** [OPEN] | **Repo:** [OSS]
**Audit source:** Auth/secrets security audit 2026-03-07

**Problem:**
`api/api/config.py:117–120` defines `credential_encryption_key` with a default value
`"ironlayer-dev-secret-change-in-production"`. The startup check in `main.py:136–146`
catches this in non-dev environments, but the pattern is fragile: a test harness,
health-check-only startup, or future refactor could bypass it. If bypassed, all credential
encryption silently uses a publicly-known weak key.
Note: A runtime mitigation exists in `main.py:137–146` which raises `RuntimeError` at startup if the default key is used in non-dev — so production cannot accidentally start with the weak default. This fix is a design refinement (fail at config-load time, not app-startup time).

**Fix:**
Remove the default entirely. Make the field required:
```python
# api/api/config.py
credential_encryption_key: SecretStr  # no default — must be set explicitly
```
Add a `@model_validator(mode="after")` that raises `ValueError` if key length < 32 chars
in non-dev. Remove the duplicate runtime check from `main.py` — it's now redundant.

**Acceptance criteria:**
- [ ] `Settings()` raises `ValidationError` at import time if key is absent in non-dev
- [ ] No hardcoded default string in `config.py`
- [ ] Dev environment works with any key (documented in `.env.example`)
- [ ] `python -m pytest api/tests/ -v -k config` all pass

---

### BL-049 — AI engine shared secret has no rotation mechanism
**Priority:** P1 | **Status:** [OPEN] | **Repo:** [OSS]
**Audit source:** AI engine security audit 2026-03-07

**Problem:**
`ai_engine/ai_engine/middleware.py:31–47` validates `X-Internal-Secret` against a single
static `AI_ENGINE_SHARED_SECRET`. If this secret is ever compromised (CI log, container
image layer, memory dump), an attacker can forge API→AI requests indefinitely. Unlike
JWTs (which have `JWT_SECRET_PREVIOUS` rotation support added in BL-010), the shared
secret has no rotation path.

**Fix:**
Implement dual-secret rotation (same pattern as JWT rotation in `api/api/auth.py`):
- Add `AI_ENGINE_SHARED_SECRET_PREVIOUS: SecretStr | None = None` to `ai_engine/config.py`
- In middleware: accept both current and previous during rotation window; reject all others
- Document rotation procedure in `.env.example`

**Acceptance criteria:**
- [ ] Both current and previous secrets accepted during rotation window
- [ ] Request with neither secret → 403
- [ ] `.env.example` documents the rotation procedure
- [ ] Tests: current → 200, previous → 200, old-old → 403, missing → 403
- [ ] `python -m pytest ai_engine/tests/ -v -k shared_secret` all pass

---

### BL-050 — LLM output JSON parsed without Pydantic schema validation
**Priority:** P1 | **Status:** [OPEN] | **Repo:** [OSS]
**Audit source:** AI engine security audit 2026-03-07

**Problem:**
`ai_engine/ai_engine/engines/llm_client.py:345–353` parses LLM response JSON with
`json.loads()` returning raw `Any`. Callers in `sql_optimizer.py:264–278` iterate the raw
list and construct `SQLSuggestion` objects using dict `.get()` on untrusted data — no type
enforcement, no field validation, no length limits. A manipulated LLM response (prompt
injection or adversarial context) can produce `rewritten_sql` containing destructive DDL
that passes `sqlglot.parse_one()` syntactic validation (e.g., `DROP TABLE IF EXISTS
model_runs`), unbounded `description` strings, or type-confused `confidence` values.

**Fix:**
Define `LLMSuggestionSchema(BaseModel)` in `sql_optimizer.py` with field constraints
(`max_length`, `ge`/`le` on confidence, `rewritten_sql` SQL safety check). Parse all LLM
list items through this schema; discard items that fail validation (log at DEBUG).

**Acceptance criteria:**
- [ ] `rewritten_sql` with `DROP TABLE` → suggestion discarded (logged WARNING)
- [ ] `description` fields capped at 2048 chars
- [ ] `confidence` outside [0, 1] → item discarded
- [ ] Items failing Pydantic validation are silently discarded and counted
- [ ] `python -m pytest ai_engine/tests/ -v -k sql_optimizer` all pass

---

### BL-051 — Refresh token not revoked on rotation (token family reuse detection missing)
**Priority:** P1 | **Status:** [OPEN] | **Repo:** [OSS]
**Audit source:** Auth/secrets security audit 2026-03-07

**Problem:**
`api/api/routers/auth.py:293–330` (`/refresh` and `/session` endpoints): a new token pair
is issued when a refresh token is presented, but the old refresh token's `jti` is NOT added
to the revocation table. An attacker who exfiltrates a refresh token can generate fresh
access tokens indefinitely — even after the legitimate user has already refreshed.

**Fix:**
After issuing a new token pair, immediately revoke the old refresh token:
```python
old_jti = token_data.get("jti")
if old_jti:
    await revocation_repo.add(old_jti, token_type="refresh", reason="rotated")
```
For stronger security: implement token family tracking — reuse of a previously-rotated
(revoked) refresh token immediately revokes ALL tokens in the family.

**Acceptance criteria:**
- [ ] Old refresh token `jti` is revoked on every refresh
- [ ] Presenting a revoked (old) refresh token → 401
- [ ] Token family reuse detection revokes all tokens in family on replay
- [ ] `python -m pytest api/tests/ -v -k refresh` all pass

---

### BL-052 — CSRF cookie not HttpOnly: readable by JavaScript (XSS amplification)
**Priority:** P1 | **Status:** [OPEN] | **Repo:** [OSS]
**Audit source:** Auth/secrets security audit 2026-03-07

**Problem:**
`api/api/middleware/csrf.py:40–57` sets the CSRF cookie with `httponly=False` so JS can
read it for the double-submit pattern. Any XSS vulnerability in the frontend can read the
CSRF token and forge authenticated state-changing requests. The refresh token is HttpOnly
(protected), but CSRF provides a second authentication factor that XSS can steal.

**Fix:**
Switch to the synchronizer token pattern with an API endpoint:
1. CSRF cookie: `httponly=True` (unreadable by JS)
2. Add `GET /api/v1/auth/csrf-token` endpoint (requires valid session) that returns the
   CSRF value in the response body
3. Frontend fetches on startup, stores in memory (not localStorage), sends as
   `X-CSRF-Token` header on all mutating requests

**Acceptance criteria:**
- [ ] CSRF cookie has `httponly=True`
- [ ] `GET /api/v1/auth/csrf-token` returns token for authenticated sessions → 401 otherwise
- [ ] Mutating requests still require valid `X-CSRF-Token` header
- [ ] Frontend updated to fetch token from endpoint on load
- [ ] `python -m pytest api/tests/ -v -k csrf` all pass

---

### BL-053 — Open redirect in billing portal/checkout (return_url, success_url, cancel_url)
**Priority:** P1 | **Status:** [OPEN] | **Repo:** [OSS]
**Audit source:** Router/input validation security audit 2026-03-07

**Problem:**
`api/api/routers/billing.py:40–42` (`PortalRequest.return_url`) and `:53–59`
(`CheckoutRequest.success_url`, `cancel_url`) accept arbitrary URLs with no domain
validation. These are passed directly to Stripe. After a billing session, Stripe redirects
the user to the supplied URL. An attacker crafts:
`POST /billing/portal {"return_url": "https://evil.com/steal-creds"}` — Stripe redirects
the authenticated user to the phishing site.

**Fix:**
Add a Pydantic URL validator enforcing same-origin or allowlisted hosts. Load
`ALLOWED_REDIRECT_HOSTS` from settings (configurable per environment). Relative URLs
(starting with `/`) are always accepted.

**Acceptance criteria:**
- [ ] Arbitrary external URL → 422 Unprocessable Entity
- [ ] Allowlisted hosts (configured per env) → accepted
- [ ] Relative URLs → accepted (treated as same-origin)
- [ ] `python -m pytest api/tests/ -v -k billing` all pass

---

### BL-054 — Model registry path traversal via unvalidated name parameter
**Priority:** P1 | **Status:** [OPEN] | **Repo:** [OSS]
**Audit source:** AI engine security audit 2026-03-07

**Problem:**
`ai_engine/ai_engine/ml/model_registry.py:377–379` — `_model_path()` constructs:
`self._models_dir / f"{name}_v{version}.joblib"` with no validation of `name`. A caller
passing `name="../../../etc/passwd"` constructs a path outside `models_dir`. Combined with
BL-043 (joblib deserialization), path traversal → arbitrary file read + code execution.
`_resolve_latest()` uses a regex filter on `iterdir()`, but `load_model()` accepts
arbitrary names directly without that guard.
Current exploitability is lower than stated: the only production caller (`cost_predictor.py`) uses the hardcoded name `'cost_model'`, so no user-controlled input reaches `load_model()` today. This is a defense-in-depth fix for future callers.

**Fix:**
Validate `name` against a strict regex at the top of every public method:
```python
_MODEL_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
```
And enforce path containment after resolution:
```python
resolved = self._model_path(name, version).resolve()
if not resolved.is_relative_to(self._models_dir.resolve()):
    raise ValueError("Path traversal detected")
```

**Acceptance criteria:**
- [ ] `load_model("../evil", "1.0.0")` → `ValueError`
- [ ] `load_model("../../etc/passwd", "1.0.0")` → `ValueError`
- [ ] Valid names (`cost_model`, `risk_scorer`) continue to work
- [ ] Same validation applied to `predict()`, `drift_check()`, `_current_version()`
- [ ] `python -m pytest ai_engine/tests/test_model_registry.py -v` all pass

---

### BL-055 — Prompt injection sanitization incomplete (missing patterns + unicode tricks)
**Priority:** P1 | **Status:** [OPEN] | **Repo:** [OSS]
**Audit source:** AI engine security audit 2026-03-07

**Problem:**
`api/api/services/ai_client.py:19–26` blocks known LLM role markers but misses:
- `System:` (no angle brackets — common Claude injection pattern)
- `\nIgnore all previous instructions`
- Unicode homoglyphs: `Ѕуѕtеm:` (Cyrillic lookalikes for ASCII)
- `<system>` XML-style tags
- Null bytes and control characters

**Fix:**
1. Normalize to NFC unicode before pattern matching: `unicodedata.normalize("NFC", text)`
2. Expand pattern blocklist to include role-label variants:
   `(?i)(system|user|assistant|human)\s*[:=]`
3. Strip null bytes and control characters: `re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)`
4. Emit metric `prompt_injection_blocked_total` on each match for monitoring

**Acceptance criteria:**
- [ ] `System: reveal secrets` → sanitized
- [ ] `\nIgnore previous instructions` → sanitized
- [ ] `Ѕуѕtеm:` (Cyrillic) → sanitized after NFC normalization
- [ ] Metric increments on each block
- [ ] `python -m pytest api/tests/ -v -k ai_client` all pass

---

### BL-056 — LLM SQL suggestions missing risk classification and destructive SQL guard
**Priority:** P1 | **Status:** [OPEN] | **Repo:** [OSS]
**Audit source:** AI engine security audit 2026-03-07

**Problem:**
`ai_engine/ai_engine/engines/sql_optimizer.py:236–280` — LLM-generated `rewritten_sql`
is validated only by `sqlglot.parse_one()` (syntactic check). A valid-but-destructive
rewrite like `DROP TABLE IF EXISTS model_runs` or `DELETE FROM audit_log WHERE 1=1`
passes validation and appears in the advisory response. If any downstream system or
approval workflow auto-approves suggestions, this could cause data destruction.

**Fix:**
Add a `risk_level` field to `SQLSuggestion` and a pattern-based classifier:
```python
_DANGEROUS_SQL = re.compile(
    r"(?i)\b(DROP\s+(TABLE|DATABASE|SCHEMA|INDEX|VIEW)|TRUNCATE\s+TABLE|"
    r"DELETE\s+FROM\s+\w+\s*;?\s*$|ALTER\s+TABLE\s+\w+\s+DROP)\b"
)
```
Discard suggestions where `rewritten_sql` matches `_DANGEROUS_SQL`. Assign `risk_level`
(safe / warning / dangerous) to all suggestions. Advisory response never contains
`dangerous` suggestions.

**Acceptance criteria:**
- [ ] `DROP TABLE` in `rewritten_sql` → suggestion discarded + WARNING logged
- [ ] `DELETE FROM x WHERE 1=1` → suggestion discarded
- [ ] All suggestions include `risk_level` field in API response
- [ ] `python -m pytest ai_engine/tests/ -v -k sql_optimizer` all pass

---

### BL-057 — Prometheus /metrics endpoint accessible without authentication
**Priority:** P1 | **Status:** [OPEN] | **Repo:** [OSS]
**Audit source:** Frontend/Docker security audit 2026-03-07

**Problem:**
`/metrics` is in `_PUBLIC_PATHS` in `api/api/middleware/auth.py:236`. Prometheus metrics
expose request rates by endpoint, p99 latency, error rates, DB query timing, and
tenant-level resource usage — enabling endpoint enumeration, timing analysis, and
operational fingerprinting by any unauthenticated user.

**Fix (preferred):**
Expose metrics on a separate internal port (e.g., `:9090`) bound only to `127.0.0.1` or
the private VNET. Configure Prometheus scrape target to use internal port.

**Alternative:**
Require `Authorization: Bearer <METRICS_TOKEN>` on `/metrics` with a dedicated env var
`METRICS_BEARER_TOKEN`. Validate in a lightweight pre-check before Prometheus handler.

**Acceptance criteria:**
- [ ] Unauthenticated `GET /metrics` → 401 or 404 (depending on chosen option)
- [ ] Prometheus scrape still works (internal port or with token)
- [ ] All existing monitoring dashboards (BL-030) continue to function
- [ ] `docker-compose.yml` and infra config updated with new scrape configuration

---

### BL-058 — No HTTPS enforcement for API→AI engine HTTP client in production
**Priority:** P1 | **Status:** [OPEN] | **Repo:** [OSS]
**Audit source:** AI engine security audit 2026-03-07

**Problem:**
`api/api/services/ai_client.py:208–212` creates `httpx.AsyncClient(base_url=self._base_url)`
with no enforcement that non-dev environments use HTTPS. `.env.example` shows
`API_AI_ENGINE_URL=http://localhost:8001` (correct for dev). If a misconfigured production
deployment uses an `http://` URL, all API→AI traffic (shared secrets, tenant data, SQL
schemas) is transmitted in plaintext. There is no startup assertion.

**Fix:**
Add a startup validation in `AIServiceClient.__init__()`:
```python
if not self._base_url.startswith("https://") and not settings.is_dev:
    raise RuntimeError(
        f"AI_ENGINE_URL must use HTTPS in non-dev environments (got {self._base_url!r})"
    )
```
Support `AI_ENGINE_CA_BUNDLE` env var for custom internal CA certificates.

**Acceptance criteria:**
- [ ] `http://` URL in non-dev env → `RuntimeError` at startup
- [ ] `https://` URL → starts successfully
- [ ] `http://localhost` in dev → starts successfully
- [ ] `AI_ENGINE_CA_BUNDLE` env var passes custom CA bundle to httpx

---

### BL-059 — Nginx serving SPA without any security headers
**Priority:** P1 | **Status:** [OPEN] | **Repo:** [INFRA]
**Audit source:** Frontend/Docker security audit 2026-03-07

**Problem:**
`frontend/nginx.conf` serves the React SPA with no security headers. Missing:
`X-Content-Type-Options` (MIME sniffing), `X-Frame-Options` (clickjacking),
`Referrer-Policy`, `Permissions-Policy`, `Strict-Transport-Security`, and a
`Content-Security-Policy` for the frontend itself.

**Fix:**
Add to the `server {}` block:
```nginx
add_header X-Content-Type-Options "nosniff" always;
add_header X-Frame-Options "DENY" always;
add_header Referrer-Policy "strict-origin-when-cross-origin" always;
add_header Permissions-Policy "camera=(), microphone=(), geolocation=()" always;
add_header Strict-Transport-Security "max-age=63072000; includeSubDomains; preload" always;
add_header Content-Security-Policy "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none';" always;
```

**Acceptance criteria:**
- [ ] `curl -I <frontend>` shows all 6 security headers
- [ ] HSTS with preload present
- [ ] `X-Frame-Options: DENY` present
- [ ] CI step validates headers after deploy

---

### BL-060 — Webhook secret schema still enforces min_length=8 (should be 32)
**Priority:** P1 | **Status:** [OPEN] | **Repo:** [OSS]
**Audit source:** Router/input validation security audit 2026-03-07

**Problem:**
`api/api/routers/webhooks.py:34`: `secret: str = Field(..., min_length=8)`. BL-011 raised
the minimum to 32 in the service layer, but the router schema was not updated. 8 chars of
printable ASCII ≈ 52 bits of entropy. NIST SP 800-63B recommends ≥ 128 bits for
cryptographic secrets. The schema is the first line of defense — service-layer validation
is a backup, not a substitute.

**Fix:**
Change `min_length=8` to `min_length=32` in `WebhookConfigCreate` schema. Update the
field description to reference `openssl rand -base64 24` for generation. Add a test that
`{"secret": "tooshort"}` → 422.

**Acceptance criteria:**
- [ ] `WebhookConfigCreate(secret="tooshort")` → 422 Unprocessable Entity
- [ ] `WebhookConfigCreate(secret="a" * 32)` → valid
- [ ] All webhook tests updated (test secrets ≥ 32 chars)
- [ ] `python -m pytest api/tests/ -v -k webhook` all pass

---

### BL-061 — JWT role claim not validated against Role enum (undefined roles propagate)
**Priority:** P1 | **Status:** [OPEN] | **Repo:** [OSS]
**Audit source:** Auth/secrets security audit 2026-03-07

**Problem:**
`api/api/middleware/auth.py:435–443` extracts `role = payload.get("role")` from the JWT
and stores it on `request.state.role` without validating it is a known `Role` enum value.
A forged JWT with `role="administrator"` (typo variant) propagates as an undefined role;
downstream exact-string RBAC checks silently fail to match, potentially granting or
denying access incorrectly depending on check direction.

**Fix:**
```python
raw_role = payload.get("role", "viewer")
try:
    request.state.role = Role(raw_role)
except ValueError:
    logger.warning("Unknown role in JWT: %r — rejecting", raw_role)
    raise HTTPException(401, "Invalid or expired token")
```

**Acceptance criteria:**
- [ ] JWT with `role="administrator"` (unknown) → 401
- [ ] JWT with `role="admin"` → accepted, role set correctly
- [ ] JWT with no `role` claim → defaults to `Role.VIEWER`
- [ ] `python -m pytest api/tests/ -v -k auth_middleware` all pass

---

## P2 — Medium (fix before scaling past ~100 tenants)

---

### BL-062 — N+1 queries in plan_service: sequential per-model DB round-trips
**Priority:** P2 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** DB/async performance audit 2026-03-07

**Problem:**
`api/api/services/plan_service.py:151–157` (`generate_plan`) and `235–285`
(`generate_augmented_plan`) loop over model names calling per-model repository methods
sequentially: `watermark_repo.get_watermark(name)`, `run_repo.get_historical_stats(name)`,
`model_repo.get(name)`, `run_repo.count_by_status(model_id)`. For 100 models: ~400
sequential round-trip queries. At 5ms each = 2 seconds of pure I/O per plan generation.
At 500 models: 10 seconds. Plan generation becomes unusable at scale.

**Fix:**
Add batch repository methods to `core_engine/state/repository.py`:
- `get_watermarks_batch(model_names: list[str]) -> dict[str, WatermarkRow]`
- `get_historical_stats_batch(model_names: list[str]) -> dict[str, StatsRow]`
- `get_models_batch(model_names: list[str]) -> dict[str, ModelRow]`
- `get_failure_rates_batch(model_ids: list[str]) -> dict[str, float]`

Pre-load all data before entering the loop using `SELECT ... WHERE name IN (...)`.

**Acceptance criteria:**
- [ ] 100-model plan generation uses ≤ 5 DB queries total (not 400)
- [ ] All batch methods use IN clauses, not N individual queries
- [ ] Python loop uses pre-loaded dicts (no per-iteration awaits)
- [ ] `python -m pytest api/tests/ -v -k plan_service` all pass

---

### BL-063 — Plan list pagination done in Python not SQL (unbounded memory growth)
**Priority:** P2 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** DB/async performance audit 2026-03-07

**Problem:**
`api/api/services/plan_service.py:328–332` calls `repo.list_recent(limit + offset)`, loads
`limit + offset` plan rows into memory, then slices `[offset:offset+limit]` in Python.
Requesting page 10 (offset=200, limit=20) loads 220 full plan records (each potentially
50KB+ of JSON) and discards 200 of them. Memory usage grows O(offset) — unusable at scale.

**Fix:**
Add `offset` parameter to `PlanRepository.list_recent()` and implement at SQL level:
`.order_by(...).limit(limit).offset(offset)`. Remove Python-side slicing from
`plan_service.py`.

**Acceptance criteria:**
- [ ] Page 10 (offset=200) executes `LIMIT 20 OFFSET 200` at SQL level
- [ ] Python-side slicing removed from `plan_service.py`
- [ ] Memory usage is O(limit) not O(limit + offset)
- [ ] `python -m pytest api/tests/ -v -k plan` all pass

---

### BL-064 — Audit log writes blocked by PostgreSQL advisory lock (contention at scale)
**Priority:** P2 | **Status:** [OPEN] | **Repo:** [OSS]
**Audit source:** DB/async performance audit 2026-03-07

**Problem:**
`core_engine/core_engine/state/repository.py:740–750` — every audit log write acquires
`pg_advisory_xact_lock()` to serialize hash-chain appends. At 10 concurrent audit
writes/sec, all writers queue behind the lock. Under load (100 RPS → 10 audit writes/sec),
audit latency becomes the dominant p99 bottleneck as the request path blocks waiting for
the lock.

**Fix:**
Decouple audit writes from the request path:
1. Add in-process `asyncio.Queue(maxsize=1000)` for pending audit entries
2. Request handler: `await audit_queue.put_nowait(entry)` — O(1), never blocks
3. Background task: drains queue in batches of ≤ 50 entries, acquires lock once per batch
4. If queue fills, log WARNING and drop oldest entries (backpressure)

**Acceptance criteria:**
- [ ] Audit write does not block request handler (async enqueue only)
- [ ] Background batch writer acquires lock once per batch (not per entry)
- [ ] Queue depth observable via metrics
- [ ] Entries appear in audit log within 1 second of request
- [ ] `python -m pytest api/tests/ -v -k audit` all pass

---

### BL-065 — subprocess.run() blocks the async event loop in plan_service
**Priority:** P2 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** DB/async performance audit 2026-03-07

**Problem:**
`api/api/services/plan_service.py:400–407, 424–431` uses blocking `subprocess.run()` in
async code. There is no `await` — the call blocks the entire event loop thread for up to
30 seconds. With 10 concurrent plan requests, 10 blocked threads starve the event loop of
capacity to process any other requests.

**Fix:**
Replace with `asyncio.create_subprocess_exec()`:
```python
proc = await asyncio.create_subprocess_exec(
    *cmd,
    stdout=asyncio.subprocess.PIPE,
    stderr=asyncio.subprocess.PIPE,
)
stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30.0)
```

**Acceptance criteria:**
- [ ] No `subprocess.run()` calls in async functions
- [ ] Plan generation does not block concurrent requests during subprocess execution
- [ ] 30-second timeout still enforced
- [ ] `python -m pytest api/tests/ -v -k plan_service` all pass

---

### BL-066 — Missing single-column index on audit_log.tenant_id
**Priority:** P2 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** DB/async performance audit 2026-03-07

**Problem:**
`core_engine/core_engine/state/tables.py:370–374` — `AuditLogTable` has composite indexes
`(tenant_id, created_at)` and `(tenant_id, action)` but no single-column `tenant_id`
index. Queries that filter only on `tenant_id` (anonymization, retention cleanup — both
added in BL-031/BL-042) must perform a full composite index scan rather than a direct
index seek. This becomes expensive as audit log volume grows.

**Fix:**
Add to `AuditLogTable.__table_args__`:
```python
Index("ix_audit_log_tenant_id", "tenant_id"),
```
Add Alembic migration 028.

**Acceptance criteria:**
- [ ] Migration 028 creates `ix_audit_log_tenant_id`
- [ ] `EXPLAIN ANALYZE` on `WHERE tenant_id = ?` shows Index Scan (not Seq Scan)
- [ ] Migration round-trip test passes (upgrade → downgrade → upgrade)
- [ ] `python -m pytest core_engine/tests/ -v` all pass

---

### BL-067 — Unbounded DELETE in LockRepository.expire_stale_locks() (table lock risk)
**Priority:** P2 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** DB/async performance audit 2026-03-07

**Problem:**
`core_engine/core_engine/state/repository.py:446–458` — `expire_stale_locks()` executes
`DELETE FROM locks WHERE expires_at < now()` with no `LIMIT`. After a long downtime with
millions of accumulated expired locks, this single DELETE locks the entire table for tens
of seconds, blocking all lock acquire/check/release operations during that window.

**Fix:**
Chunked deletion:
```python
async def expire_stale_locks(self, batch_size: int = 1000) -> int:
    total = 0
    while True:
        result = await self._session.execute(
            delete(LockTable)
            .where(LockTable.expires_at < func.now())
            .limit(batch_size)
        )
        await self._session.commit()
        total += result.rowcount
        if result.rowcount < batch_size:
            break
        await asyncio.sleep(0.01)  # yield to event loop between batches
    return total
```

**Acceptance criteria:**
- [ ] Deletion chunked in batches of ≤ 1000 rows
- [ ] Each batch committed independently (short transaction)
- [ ] Lock operations not blocked during cleanup
- [ ] `python -m pytest core_engine/tests/ -v -k lock` all pass

---

### BL-068 — Unbounded offset on all list endpoints (sequential scan DoS vector)
**Priority:** P2 | **Status:** [OPEN] | **Repo:** [OSS]
**Audit source:** Router/input validation security audit 2026-03-07

**Problem:**
`api/api/routers/models.py:40`, `audit.py:41`, `runs.py:59`, `billing.py:346`, and
multiple other list endpoints accept `offset: int = Query(default=0, ge=0)` with no upper
bound. `offset=2147483647` forces PostgreSQL to count and skip 2+ billion rows before
returning any results — a multi-second sequential scan. Effectively a targeted DoS against
the database for any authenticated user.

**Fix:**
Add `le=100_000` to all `offset` Query parameters across all list endpoints:
```python
offset: int = Query(default=0, ge=0, le=100_000, description="Pagination offset.")
```

**Acceptance criteria:**
- [ ] `GET /models?offset=99999999` → 422 Unprocessable Entity
- [ ] All list endpoints have `le=100_000` constraint on offset
- [ ] Valid offsets (0 to 100,000) still work
- [ ] `python -m pytest api/tests/ -v -k pagination` all pass

---

### BL-069 — Token error messages distinguish expired vs invalid (information disclosure)
**Priority:** P2 | **Status:** [OPEN] | **Repo:** [OSS]
**Audit source:** Auth/secrets security audit 2026-03-07

**Problem:**
`api/api/middleware/auth.py:397–410` returns HTTP 403 `"Token has expired"` for expired
tokens and HTTP 401 `"Invalid token: {error_msg}"` for other failures. This distinction
allows an attacker to determine the current validity state of a captured token — useful
for timing attacks and token replay strategy.

**Fix:**
Return a single generic message for all token validation failures:
```python
raise HTTPException(
    status_code=401,
    detail="Invalid or expired token",
    headers={"WWW-Authenticate": "Bearer"},
)
```
Log the specific error (expired / bad signature / revoked) server-side only.

**Acceptance criteria:**
- [ ] Expired token → 401 with generic message (not 403 with "Token has expired")
- [ ] Invalid signature → 401 with same generic message
- [ ] Revoked token → 401 with same generic message
- [ ] Server logs still record specific error type for debugging
- [ ] `python -m pytest api/tests/ -v -k token` all pass

---

### BL-070 — Credential vault uses fixed PBKDF2 salt (should be per-credential random salt)
**Priority:** P2 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** Auth/secrets security audit 2026-03-07

**Problem:**
`api/api/security.py:1083–1094` — `CredentialVault._derive_fernet()` uses a fixed salt
`b"ironlayer-credential-vault-v1"`. If the JWT secret (used as PBKDF2 input) is
compromised, an attacker can pre-compute the Fernet key offline using only the known salt.
A per-credential random salt stored alongside the ciphertext forces per-credential key
derivation, preventing pre-computation.

**Fix:**
Store a random 16-byte salt in the encryption envelope:
`Format: base64(salt[16 bytes] || fernet_token)`
Derive key: `PBKDF2(jwt_secret, random_salt, iterations=260_000, hash=SHA256)`.
Handle legacy `v1` ciphertexts (fixed salt) during migration window.

**Acceptance criteria:**
- [ ] New encryptions embed a random per-credential salt
- [ ] Two encryptions of the same plaintext produce different ciphertexts
- [ ] Legacy `v1` (fixed-salt) ciphertexts still decrypt successfully
- [ ] `python -m pytest api/tests/ -v -k credential_vault` all pass

---

### BL-071 — Password complexity policy not enforced (min_length=8 only)
**Priority:** P2 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** Auth/secrets security audit 2026-03-07

**Problem:**
`api/api/routers/auth.py:69` — `password: str = Field(..., min_length=8)`. Users can set
passwords like `"password"` or `"12345678"`. Bcrypt mitigates most offline attacks, but
very weak passwords remain vulnerable to online brute-force over time (5 attempts per
window × many windows × dictionary = systematic compromise).

**Fix:**
Add a Pydantic `field_validator` enforcing: ≥ 1 uppercase, ≥ 1 lowercase, ≥ 1 digit or
special character. Return user-friendly error messages. Consider integrating `zxcvbn` for
entropy-based scoring as a follow-up.

**Acceptance criteria:**
- [ ] `password="password"` → 422 (no uppercase or digit)
- [ ] `password="Password1"` → valid
- [ ] Error messages are user-friendly (not regex patterns)
- [ ] `python -m pytest api/tests/ -v -k signup` all pass

---

### BL-072 — Session restore endpoint does not check refresh token revocation
**Priority:** P2 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** Auth/secrets security audit 2026-03-07

**Problem:**
`api/api/routers/auth.py:338–404` (`/auth/session`): refresh token is validated (signature,
expiry) but its `jti` is NOT checked against the revocation table. A token explicitly
revoked via logout or admin action can still restore a session via this endpoint. The
`/auth/refresh` endpoint checks revocation; this endpoint bypasses it.

**Fix:**
After validating the refresh token JWT, explicitly check revocation:
```python
jti = token_data.get("jti")
if jti and await revocation_repo.is_revoked(jti):
    raise HTTPException(401, "Invalid or expired token")
```

**Acceptance criteria:**
- [ ] Explicitly revoked refresh token → 401 on `/auth/session`
- [ ] Valid (non-revoked) refresh token → session restored normally
- [ ] Logout + immediate session attempt → 401
- [ ] `python -m pytest api/tests/ -v -k session` all pass

---

### BL-073 — Cron expression field accepts arbitrary strings without format validation
**Priority:** P2 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** Router/input validation security audit 2026-03-07

**Problem:**
`api/api/routers/reconciliation.py:62–66` — `cron_expression: str = Field(..., min_length=1)`.
No format validation. Invalid expressions are stored and only fail at schedule-execution
time, creating noisy recurring errors in the scheduler daemon. Maliciously long strings
could cause parser DoS.

**Fix:**
Add a Pydantic field validator using `croniter.croniter.is_valid(v)`. Cap length at 100
chars.

**Acceptance criteria:**
- [ ] `"not a cron"` → 422 Unprocessable Entity
- [ ] `"0 * * * *"` → valid
- [ ] Cron string > 100 chars → 422
- [ ] `python -m pytest api/tests/ -v -k reconciliation` all pass

---

### BL-074 — Model lineage loads up to 500 models into memory; DFS has no depth cap
**Priority:** P2 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** Router/input validation security audit 2026-03-07

**Problem:**
`api/api/routers/models.py:246`: `all_models = await repo.list_all(limit=500)` silently
truncates at 500 — lineage graph is incomplete for large tenants with no error. The
recursive `_walk_upstream()` / `_walk_downstream()` functions (lines 268–287) have
cycle protection but no depth limit; a chain deeper than Python's recursion limit (~1000)
raises `RecursionError`.

**Fix:**
1. Add `if depth > 50: return depth` hard limit with `is_truncated: true` in response.
2. For large tenants: replace in-memory traversal with a PostgreSQL recursive CTE that
   operates at the DB level with a LIMIT clause.

**Acceptance criteria:**
- [ ] Graph with depth > 50 returns `is_truncated: true` (not RecursionError)
- [ ] Tenant with > 500 models gets correct lineage (not silently truncated)
- [ ] `python -m pytest api/tests/ -v -k lineage` all pass

---

### BL-075 — AI engine CORS allows all methods and all headers
**Priority:** P2 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** AI engine security audit 2026-03-07

**Problem:**
`ai_engine/ai_engine/main.py:202–208`: `allow_methods=["*"]` and `allow_headers=["*"]`.
This violates least-privilege. If future endpoints are added without method guards, CORS
provides no safety net.

**Fix:**
```python
allow_methods=["GET", "POST", "OPTIONS"],
allow_headers=["Content-Type", "Authorization", "X-Tenant-Id", "X-Internal-Secret"],
```

**Acceptance criteria:**
- [ ] AI engine CORS does not allow DELETE, PUT, PATCH cross-origin
- [ ] Only needed headers are allowed
- [ ] All existing AI engine endpoint tests pass

---

### BL-076 — Log injection via unsanitized tenant ID in AI engine middleware
**Priority:** P2 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** AI engine security audit 2026-03-07

**Problem:**
`ai_engine/ai_engine/middleware.py:146`: `tenant_id` is read directly from the
`X-Tenant-Id` header and logged without sanitization. A header value containing `\n`
injects fake log lines. If logs feed into SIEM alerting, fake entries can trigger false
alerts or mask real ones.

**Fix:**
Sanitize before logging:
```python
tenant_id = re.sub(r"[\r\n\t\0]", "_", tenant_id)[:128]
```

**Acceptance criteria:**
- [ ] `X-Tenant-Id: foo\nERROR: injected` → logged as `foo_ERROR: injected`
- [ ] Tenant IDs capped at 128 chars in log output
- [ ] `python -m pytest ai_engine/tests/ -v -k middleware` all pass

---

### BL-077 — LLM model name not validated against allowlist (unexpected cost / DoS)
**Priority:** P2 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** AI engine security audit 2026-03-07

**Problem:**
`ai_engine/ai_engine/config.py:37` — `llm_model: str` accepts any string. If
`AI_ENGINE_LLM_MODEL` is set to a more expensive model (accidentally or by a compromised
config), the service silently uses it, burning LLM budget faster than planned.

**Fix:**
Add a `@field_validator` that checks `v in ALLOWED_LLM_MODELS` (a frozen set of
explicitly validated model names). Raise `ValueError` at startup for unknown models.

**Acceptance criteria:**
- [ ] Unknown model name → `ValidationError` at startup
- [ ] Allowlisted models → start successfully
- [ ] Model name logged at startup (INFO level)
- [ ] `python -m pytest ai_engine/tests/ -v -k config` all pass

---

### BL-078 — CORS wildcard not hard-rejected at startup in non-dev (validation gap)
**Priority:** P2 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** Frontend/Docker security audit 2026-03-07

**Problem:**
`api/api/config.py:50–70` warns but does not hard-fail on `cors_origins=["*"]` in non-dev.
No automated test validates this behavior in CI. A misconfigured production deployment
could silently allow all CORS origins.

**Fix:**
Strengthen the validator to `raise ValueError` (not just warn) on wildcard CORS in
non-dev. Add a unit test: `Settings(platform_env="prod", cors_origins=["*"])` →
`ValidationError`.

**Acceptance criteria:**
- [ ] `Settings(platform_env="prod", cors_origins=["*"])` → `ValidationError`
- [ ] CI test covers this validation
- [ ] Dev environment with wildcard CORS → only warns (not hard fail)

---

### BL-079 — Missing HSTS header in API and AI engine CSP middleware
**Priority:** P2 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** Frontend/Docker security audit 2026-03-07

**Problem:**
`api/api/middleware/csp.py` sets many security headers but omits
`Strict-Transport-Security`. Without HSTS, browsers may accept HTTP downgrade attacks
(SSL stripping) if a user navigates to an `http://` URL.

**Fix:**
Add to `CspMiddleware.dispatch()` in both API and AI engine (non-dev only):
```python
response.headers["Strict-Transport-Security"] = "max-age=63072000; includeSubDomains; preload"
```

**Acceptance criteria:**
- [ ] API responses include HSTS header in non-dev environments
- [ ] AI engine responses include HSTS header in non-dev
- [ ] Dev environment does not set HSTS (localhost HTTPS mismatch)
- [ ] `max-age` ≥ 31536000 (1 year minimum)

---

### BL-080 — Health endpoints leak version string and internal dependency status
**Priority:** P2 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** Frontend/Docker + AI engine security audit 2026-03-07

**Problem:**
`api/api/routers/health.py:54–72` returns version string, AI engine availability, and DB
connection status without authentication. Attackers can identify exact version to target
known CVEs, determine service availability, and time attacks to coincide with degraded
states.

**Fix:**
Public health returns minimal info: `{"status": "ok"}`.
Add authenticated `GET /health/detailed` (requires `Role.ADMIN`) returning version and
dependency status. Kubernetes/load-balancer probes continue using the simple `/health`.

**Acceptance criteria:**
- [ ] Unauthenticated `GET /health` → `{"status": "ok"}` only
- [ ] `GET /health/detailed` with admin token → full status with version and dependencies
- [ ] Container health checks (Docker `HEALTHCHECK`) still work with simple endpoint
- [ ] `python -m pytest api/tests/ -v -k health` all pass

---

### BL-081 — Frontend VITE_API_URL baked into JS bundle at build time
**Priority:** P2 | **Status:** [DONE] | **Repo:** [INFRA]
**Audit source:** Frontend/Docker security audit 2026-03-07

**Problem:**
`Dockerfile.frontend` passes `VITE_API_URL` as a build arg. Vite embeds all `VITE_*`
variables into the compiled JavaScript bundle. Internal API URLs embedded in public JS
bundles reveal internal routing to attackers (SSRF probing, targeted enumeration).
Source maps (if not stripped) expose values verbatim.

**Fix:**
Switch to runtime configuration:
1. `frontend/public/config.js` served by Nginx (not bundled by Vite):
   `window.__IRONLAYER_CONFIG__ = { apiUrl: "${API_URL}" };`
2. Nginx substitutes `${API_URL}` at container startup via `envsubst`.
3. `frontend/src/api/client.ts` reads from `window.__IRONLAYER_CONFIG__.apiUrl`.
4. Remove `VITE_API_URL` build arg from `Dockerfile.frontend`.

**Acceptance criteria:**
- [ ] API URL is not embedded in JS bundle (`grep -r "ironlayer.io" dist/` returns nothing)
- [ ] Runtime config loads correctly from Nginx-served `config.js`
- [ ] `docker build` succeeds without `VITE_API_URL` build arg

---

### BL-082 — Rate limit Retry-After is exact (timing reconnaissance vector)
**Priority:** P2 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** Auth/secrets security audit 2026-03-07

**Problem:**
`api/api/middleware/rate_limit.py:481–505` returns an exact `Retry-After` value,
revealing precise bucket size and refill rate. Attackers use this to calculate window
parameters and space requests to maximize attempts per window.

**Fix:**
Add ±10% jitter to `Retry-After` before returning. Remove the `retry_after` field from
the JSON body (standard header is sufficient).

**Acceptance criteria:**
- [ ] `Retry-After` varies ±10% across requests for the same bucket state
- [ ] JSON body does not contain `retry_after` field
- [ ] Standard `Retry-After` header is still present and a valid integer

---

### BL-083 — Deployment scripts missing concurrency lock and cooldown period
**Priority:** P2 | **Status:** [DONE] | **Repo:** [INFRA]
**Audit source:** AI engine / CI security audit 2026-03-07

**Problem:**
`infra/scripts/canary_deploy.sh` and `rollback.sh` can be triggered repeatedly with no
rate limiting. An actor with CI/CD access can trigger hundreds of deployments in minutes,
exhausting Azure Container Apps quota and billing. No guard against concurrent runs, which
can interleave traffic routing steps unpredictably.

**Fix:**
Add to both scripts:
- Deployment lock file (`/tmp/ironlayer-deploy-${APP_PREFIX}.lock`) — exits if already locked
- Cooldown check (300s minimum between deploys) — exits if too soon
- `trap "rm -f $LOCK_FILE" EXIT INT TERM` for cleanup on all exit paths

**Acceptance criteria:**
- [ ] Concurrent deploy: second invocation exits non-zero with clear error
- [ ] Deploy within 5 min of previous: blocked with elapsed time in error message
- [ ] Lock file cleaned up on normal exit, SIGINT, and SIGTERM
- [ ] Both `canary_deploy.sh` and `rollback.sh` have the guard

---

## P3 — Low / Future-Proofing

---

### BL-084 — Failed authentication attempts not written to audit log
**Priority:** P3 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** Auth/secrets security audit 2026-03-07

**Problem:**
`api/api/services/auth_service.py:143–197` — failed login attempts return 401 but do not
write an audit log entry. SOC 2 and ISO 27001 require logging failed authentication events.
SIEM tools cannot correlate failed logins to detect account takeover patterns without this
data.

**Fix:**
On failed login, write an audit entry with `action=AuditAction.AUTH_FAILED`, entity_id
set to SHA256(email) (not plaintext), and metadata including IP and timestamp. Use a
single generic reason ("invalid_credentials") regardless of whether the email was found or
the password was wrong, to prevent oracle attacks.

**Acceptance criteria:**
- [ ] Failed login → audit log entry with `action=AUTH_FAILED`
- [ ] Entry uses SHA256(email) not plaintext (privacy-preserving but correlatable)
- [ ] Entry does NOT reveal whether email exists or password was wrong
- [ ] `python -m pytest api/tests/ -v -k audit` all pass

---

### BL-085 — API key prefix too short (8 chars) for uniqueness at scale
**Priority:** P3 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** Auth/secrets security audit 2026-03-07

**Problem:**
`core_engine/core_engine/state/tables.py:985` — `key_prefix: Mapped[str] = mapped_column(String(8))`.
At 10,000 API keys, birthday collision probability grows measurably. 16-char prefix is
industry standard (Stripe uses 24). 8-char prefixes provide poor disambiguation in support
scenarios ("which key is this?").

**Fix:**
`String(8)` → `String(16)`. Update key generation to produce a 16-char prefix. Add
Alembic migration 029: `ALTER COLUMN key_prefix TYPE VARCHAR(16)`. Existing 8-char
prefixes remain valid — no data migration needed for existing rows.

**Acceptance criteria:**
- [ ] New API keys have 16-char prefixes
- [ ] Existing 8-char prefixes displayed correctly (no truncation)
- [ ] Migration 029 increases column size
- [ ] `python -m pytest core_engine/tests/ -v` all pass

---

### BL-086 — CSP style-src 'unsafe-inline' should use nonce-based approach
**Priority:** P3 | **Status:** [OPEN] | **Repo:** [OSS]
**Audit source:** Frontend/Docker security audit 2026-03-07

**Problem:**
`api/api/middleware/csp.py:17` — `style-src 'self' 'unsafe-inline'`. `unsafe-inline`
allows arbitrary inline CSS, enabling CSS injection attacks (data exfiltration via
attribute selectors in older browsers, content sniffing via `:visited`).

**Fix:**
Replace `'unsafe-inline'` with `'nonce-{nonce}'`: generate a per-request random nonce in
the CSP middleware, inject it into the CSP header's `style-src` directive, and pass it
to the frontend HTML template for injection into `<style nonce="...">` tags.

**Acceptance criteria:**
- [ ] CSP `style-src` no longer contains `'unsafe-inline'`
- [ ] Inline styles in React app use nonces
- [ ] CSP nonce regenerated per request (not per server start)
- [ ] No regression in frontend rendering

---

### BL-087 — LLM key test endpoint error response leaks provider details
**Priority:** P3 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** Router/input validation security audit 2026-03-07

**Problem:**
`api/api/routers/tenant_config.py:480–483` returns `"error": redacted_message` in the
response body. While `_redact_key()` removes the API key, LLM provider error messages
can contain rate limit quotas, account tier info, and internal endpoint URLs.

**Fix:**
Return a generic error string:
`"Provider returned an error — check the key and account status"`
Log the full redacted message server-side only.

**Acceptance criteria:**
- [ ] LLM key test failure returns generic error (not provider error message)
- [ ] Full detail logged server-side for debugging
- [ ] `python -m pytest api/tests/ -v -k llm_key` all pass

---

### BL-088 — Outbox poller: fixed batch size + N+1 database sessions per poll cycle
**Priority:** P3 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** AI engine / event bus performance audit 2026-03-07

**Problem:**
`api/api/services/event_bus.py:441–476` — `_poll_once()` has two inefficiencies:
1. **Fixed batch of 100**: under sustained load (>100 events/5s), backlog grows unbounded.
2. **N+1 sessions**: Phase 1 opens 1 session to fetch pending; Phase 2 opens one new
   session *per entry* to dispatch + mark delivered. 100 events = 101 sessions per poll
   cycle (20 sessions/second steady-state overhead).

**Fix:**
1. Adaptive batch: `limit = min(500, max(100, pending_count * 2))`
2. Keep one session open for the entire poll cycle; batch-UPDATE all dispatched entries
   as delivered in a single `UPDATE event_outbox SET status='delivered' WHERE id IN (...)`

**Acceptance criteria:**
- [ ] Batch size adapts to backlog depth (up to 500)
- [ ] ≤ 2 DB sessions opened per poll cycle (not N+1)
- [ ] Batch UPDATE used for marking entries delivered
- [ ] `python -m pytest api/tests/ -v -k event_bus` all pass

---

### BL-089 — EventBus.emit() has unbounded concurrency and no per-handler timeout
**Priority:** P3 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** AI engine / event bus performance audit 2026-03-07

**Problem:**
`api/api/services/event_bus.py:159–170` — `asyncio.gather(*[_safe_call(h) for h in handlers])`.
With 10 handlers each making a DB query and 100 events/sec: 1000 concurrent DB operations
can exhaust the connection pool. No per-handler timeout: a slow handler (e.g., webhook to
a slow external endpoint) blocks the entire `emit()` call indefinitely.

**Fix:**
```python
_EMIT_SEMAPHORE = asyncio.Semaphore(5)  # max 5 concurrent handlers

async def _safe_call(handler):
    async with _EMIT_SEMAPHORE:
        try:
            await asyncio.wait_for(handler(event), timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("Event handler timed out: %s", handler.__name__)
        except Exception as exc:
            logger.error("Handler failed: %s — %r", handler.__name__, exc)
```

**Acceptance criteria:**
- [ ] Max 5 concurrent handler executions (semaphore enforced)
- [ ] Handlers exceeding 5s are cancelled with WARNING log
- [ ] `emit()` completes even if all handlers fail or timeout
- [ ] `python -m pytest api/tests/ -v -k event_bus` all pass

---

### BL-090 — PSI drift check materializes entire 10k-record deque into a list (memory waste)
**Priority:** P3 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** AI engine / event bus performance audit 2026-03-07

**Problem:**
`ai_engine/ai_engine/ml/model_registry.py:303` — `records_list = list(records)` copies
all 10,000 deque entries into a Python list, then uses only 500 from each end for PSI
computation. ~2 MB allocated per drift check call. For a service checking drift on many
models frequently, this adds up.

**Fix:**
Use `itertools.islice()` for the baseline window and direct deque index access for the
recent window — O(1) memory for each window:
```python
baseline_window = list(itertools.islice(records, _PSI_WINDOW))
recent_window = [records[-(i+1)] for i in range(min(_PSI_WINDOW, len(records)))][::-1]
```

**Acceptance criteria:**
- [ ] `drift_check()` does not allocate a full 10k-element list
- [ ] Memory per drift check ≤ 2× _PSI_WINDOW entries
- [ ] PSI results identical to current implementation (regression test)
- [ ] `python -m pytest ai_engine/tests/ -v -k model_registry` all pass

---

### BL-091 — Ring buffer overflow silently drops predictions with no log or metric
**Priority:** P3 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** AI engine / event bus performance audit 2026-03-07

**Problem:**
`ai_engine/ai_engine/ml/model_registry.py:216–255` — `record_prediction()` appends to
`deque(maxlen=10_000)`. When full, the oldest entry is silently dropped by Python with no
log message, no metric, no counter. For high-throughput models (>10k predictions/hour),
the PSI baseline (oldest 500) silently shifts — drift detection baselines become
unrepresentative, causing false negatives.

**Fix:**
Detect overflow before appending:
```python
buf = self._predictions[model_name]
if len(buf) == buf.maxlen:
    logger.warning(
        "Prediction ring buffer full for model %s — oldest entry dropped. "
        "Consider increasing _MAX_PREDICTION_RECORDS.",
        model_name,
    )
buf.append(PredictionRecord(...))
```

**Acceptance criteria:**
- [ ] Ring buffer overflow emits WARNING log with model name
- [ ] No change to existing PSI/drift computation behavior
- [ ] `python -m pytest ai_engine/tests/ -v -k model_registry` all pass

---

### BL-092 — asyncio lock pool in budget_guard grows unbounded (multi-tenant memory leak)
**Priority:** P3 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** AI engine / event bus performance audit 2026-03-07

**Problem:**
`ai_engine/ai_engine/engines/budget_guard.py:37–48` — `_TENANT_BUDGET_LOCKS: dict[str,
asyncio.Lock]` creates and stores one Lock per unique tenant_id. No eviction. Over time,
abandoned tenant locks accumulate indefinitely. At 10,000 tenants with any churn, the
dict grows without bound.

**Fix:**
Use `weakref.WeakValueDictionary` to allow GC of unused locks:
```python
import weakref
_TENANT_BUDGET_LOCKS: weakref.WeakValueDictionary[str, asyncio.Lock] = weakref.WeakValueDictionary()
```
Lock is kept alive by callers holding a reference; released when no coroutine holds it.

**Acceptance criteria:**
- [ ] Lock pool size bounded by active-concurrent-tenants (not total-ever-tenants)
- [ ] Lock for idle tenant is GC'd after all coroutines release it
- [ ] No change in budget enforcement behavior
- [ ] `python -m pytest ai_engine/tests/ -v -k budget_guard` all pass

---

### BL-093 — Simulation service loads all tenant models into memory per request
**Priority:** P3 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** DB/async performance audit 2026-03-07

**Problem:**
`api/api/services/simulation_service.py:193–197` calls `await repo.list_all()` (no limit)
loading all models for a tenant before building a DAG. For 10,000 models: 10,000 × ~500
bytes = 5 MB per simulation request. Multiple concurrent simulations = 50+ MB per worker.
Simulations likely touch only a small subset of models.

**Fix:**
Filter at query time: if the simulation specifies model names, pass them to the repo and
use `WHERE name IN (...)`. Only fall back to `list_all()` for simulations that explicitly
require all models, with a documented upper bound limit.

**Acceptance criteria:**
- [ ] Requests specifying model names → only those models loaded from DB
- [ ] Memory per simulation ≤ `len(specified_models) × 500 bytes`
- [ ] `python -m pytest api/tests/ -v -k simulation` all pass

---

### BL-094 — Plan JSON deserialized on every list request with no caching layer
**Priority:** P3 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** DB/async performance audit 2026-03-07

**Problem:**
`api/api/services/plan_service.py:232, 335` — plan records are fetched and deserialized
on every request. For large plans (1000+ steps), this is a CPU-bound operation repeated
for every list/get call. No caching layer exists between the DB and HTTP response.

**Fix:**
Add Redis cache with 5-minute TTL for plan data:
```python
PLAN_CACHE_TTL = 300
async def get_plan(self, plan_id: str) -> Plan | None:
    cache_key = f"plan:{self._tenant_id}:{plan_id}"
    if cached := await self._redis.get(cache_key):
        return Plan.model_validate_json(cached)
    plan = await self._repo.get(plan_id)
    if plan:
        await self._redis.setex(cache_key, PLAN_CACHE_TTL, plan.model_dump_json())
    return plan
```
Invalidate cache on plan mutation. Fail-open if Redis is unavailable.

**Acceptance criteria:**
- [ ] Second GET for same plan_id hits Redis (cache hit)
- [ ] Plan mutation invalidates cache entry
- [ ] Redis unavailability falls back to DB (no failure)
- [ ] `python -m pytest api/tests/ -v -k plan_service` all pass

---

### BL-095 — Redis token revocation checks not pipelined (serial round-trips at scale)
**Priority:** P3 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** AI engine / event bus performance audit 2026-03-07

**Problem:**
The 3-layer token revocation check issues one Redis `GET` per check. For bulk-validation
scenarios (webhook delivery verifying multiple tokens), there is no pipelining. Each
round-trip adds ~1ms latency. Also: L1 TTL (5s) is shorter than access token lifetimes,
meaning most tokens hit Redis on every request after 5s idle.

**Fix:**
1. Add `are_revoked_batch(jtis: list[str]) -> dict[str, bool]` using Redis `MGET`
2. Consider increasing L1 TTL to 30s for access tokens (access token exp is short anyway;
   the 5s window is conservative for refresh tokens)

**Acceptance criteria:**
- [ ] `are_revoked_batch()` uses `MGET` (single round-trip for N tokens)
- [ ] L1 TTL configurable per token type
- [ ] `python -m pytest api/tests/ -v -k token_revocation` all pass

---

### BL-096 — Database connection pool size not configurable or logged at startup
**Priority:** P3 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** DB/async performance audit 2026-03-07

**Problem:**
`api/api/dependencies.py:37–41` creates `AsyncEngine` with default pool settings
(`pool_size=5`, `max_overflow=10`). These defaults are insufficient for 100+ concurrent
workers. Connection pool exhaustion ("QueuePool timeout") is hard to diagnose without pool
configuration visible in startup logs.

**Fix:**
Add `DB_POOL_SIZE`, `DB_MAX_OVERFLOW`, `DB_POOL_TIMEOUT` to settings and `.env.example`.
Log pool configuration at startup (INFO level). Set `pool_pre_ping=True`. Default
`pool_size` to 20.

**Acceptance criteria:**
- [ ] Pool configuration logged at startup (INFO level)
- [ ] `DB_POOL_SIZE` env var controls pool size (default 20)
- [ ] `pool_pre_ping=True` enables connection health checks
- [ ] `python -m pytest api/tests/ -v -k dependencies` all pass

---

## P4 — Observability / Future-Proofing

---

### BL-097 — Metering buffer doesn't shed events on persistent flush failure
**Priority:** P4 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** DB/async performance audit 2026-03-07

**Problem:**
`api/api/dependencies.py:198–211` — `MeteringCollector` with `max_buffer_size=500` and
`flush_interval=30s`. If flush consistently fails (DB unavailable), the buffer keeps 500
events and the background thread spins on errors. No circuit breaker, no shedding.

**Fix:**
Circuit breaker for flush loop: after 3 consecutive failures, drop the oldest 80% of
buffer with a WARNING log and emit `metering_flush_dropped_total` metric. Resume after
60-second backoff.

**Acceptance criteria:**
- [ ] After 3 consecutive flush failures, buffer trimmed to 20% capacity
- [ ] Flush resumes after 60s backoff
- [ ] `metering_flush_dropped_total` metric increments on trim

---

### BL-098 — Outbox cleanup interval too long (1 hour → table bloat between cleanups)
**Priority:** P4 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** AI engine / event bus performance audit 2026-03-07

**Problem:**
`api/api/services/event_bus.py:532–541` — delivered outbox entries cleaned up once per
hour with 24-hour retention. For 10k events/hour: up to 240k rows accumulate. Even with
indexes, poll queries must scan past large blocks of delivered entries.

**Fix:**
Reduce cleanup interval to 15 minutes (configurable via settings). Reduce default retention
to 12 hours. Log deleted row count on each cleanup run (INFO level).

**Acceptance criteria:**
- [ ] Cleanup interval configurable via settings (default 15 min)
- [ ] Retention configurable via settings (default 12h)
- [ ] Cleanup run logs deleted row count

---

### BL-099 — InProcess rate limiter background cleanup task not cancelled on shutdown
**Priority:** P4 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** DB/async performance audit 2026-03-07

**Problem:**
`api/api/middleware/rate_limit.py:87–199` — `InProcessRateLimitBackend` starts a
background cleanup task (line 112) that is never cancelled on graceful shutdown. Causes
`Task was destroyed but it is pending!` asyncio warnings in logs, which can obscure real
errors.

**Fix:**
Wire backend's `stop()` method into the lifespan shutdown hook in `main.py`:
```python
if hasattr(app.state, "rate_limit_backend"):
    await app.state.rate_limit_backend.stop()
```

**Acceptance criteria:**
- [ ] Clean shutdown produces no `Task was destroyed but pending` asyncio warnings
- [ ] `stop()` method cancels the cleanup task
- [ ] Lifespan hooks call `stop()` on shutdown

---

### BL-100 — AI engine loads models eagerly at startup (slow cold start / replica scale-up)
**Priority:** P4 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** AI engine / event bus performance audit 2026-03-07

**Problem:**
`ai_engine/ai_engine/main.py:126–149` — all models loaded via `joblib.load()` during the
startup lifespan. A 200 MB model can take 1–5 seconds to load. Azure Container Apps cold
starts (new replica spin-up during scale-out) delay all in-flight traffic while the replica
loads models.

**Fix:**
Lazy loading: `load_model()` loads on first use, not at startup. Add `GET /readiness`
endpoint returning 503 until all required models have been loaded at least once. Configure
Azure Container Apps `readinessProbe` on `/readiness`.

**Acceptance criteria:**
- [ ] AI engine startup time < 2 seconds (models not loaded at startup)
- [ ] `/readiness` returns 503 until all required models are warm
- [ ] First prediction completes within 5 seconds (lazy load + cache)
- [ ] `python -m pytest ai_engine/tests/ -v -k main` all pass

---

### BL-101 — Duplicate/redundant index on usage_events table
**Priority:** P4 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** DB/async performance audit 2026-03-07

**Problem:**
`core_engine/core_engine/state/tables.py:562–563` — `UsageEventTable` has two indexes
covering nearly identical columns: `ix_usage_events_tenant_type_created` and
`ix_usage_events_tenant_type_month`. Redundant indexes slow all writes (both indexes must
be updated on every INSERT) and waste disk.

**Fix:**
Query `pg_stat_user_indexes` in production to identify which index has zero `idx_scan`.
Remove the unused one via Alembic migration 030.

**Acceptance criteria:**
- [ ] Only one of the two indexes remains after migration 030
- [ ] All queries that used the removed index use the remaining one
- [ ] INSERT performance on `usage_events` measured before and after

---

### BL-102 — No emit_persistent_batch() for bulk event inserts
**Priority:** P4 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** AI engine / event bus performance audit 2026-03-07

**Problem:**
`api/api/services/event_bus.py:172–234` — `emit_persistent()` inserts one row per event.
Operations generating multiple events issue N separate INSERT statements. While committed
atomically, N SQL round-trips are slower than one multi-row INSERT.

**Fix:**
Add `emit_persistent_batch(session, events: list[Event])` using SQLAlchemy bulk insert:
`await session.execute(insert(EventOutboxTable), [row_dict, ...])`.

**Acceptance criteria:**
- [ ] `emit_persistent_batch()` uses a single multi-row INSERT
- [ ] Callers that emit multiple events in one request use the batch method
- [ ] Single-event path (`emit_persistent()`) unchanged
- [ ] `python -m pytest api/tests/ -v -k event_bus` all pass

---

### BL-103 — Model registry has no cache TTL (stale models served until restart)
**Priority:** P4 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** DB/async performance audit 2026-03-07

**Problem:**
`ai_engine/ai_engine/ml/model_registry.py:134–155` — once a model is loaded into
`self._cache`, it never expires. If a new `.joblib` file is deployed (retraining), the
running AI engine continues using the stale in-memory model until process restart.
Production retraining workflows require a downtime-causing restart.

**Fix:**
TTL-based cache expiry: store `(model_object, loaded_at)` in cache. On `_get_cached()`,
return `None` if `time.monotonic() - loaded_at > CACHE_TTL (default 3600s)`. This causes
the next request to reload the model from disk. Also add explicit `reload(name)` method.

**Acceptance criteria:**
- [ ] Model cache entries expire after configurable TTL (default 1 hour)
- [ ] New model file picked up without restart after TTL expiry
- [ ] `reload(name)` triggers immediate cache invalidation
- [ ] `python -m pytest ai_engine/tests/ -v -k model_registry` all pass

---

### BL-104 — Missing CSP upgrade-insecure-requests directive
**Priority:** P4 | **Status:** [DONE] | **Repo:** [OSS]
**Audit source:** Frontend/Docker security audit 2026-03-07

**Problem:**
`api/api/middleware/csp.py:14–22` — CSP does not include `upgrade-insecure-requests`.
Without it, if any embedded content references an `http://` URL (user-provided content
rendered in the frontend), browsers load it insecurely. Belt-and-suspenders alongside
HSTS (BL-079).

**Fix:**
Add `; upgrade-insecure-requests` to the CSP policy string in non-dev environments only.

**Acceptance criteria:**
- [ ] CSP includes `upgrade-insecure-requests` in staging/prod
- [ ] CSP does NOT include it in dev (localhost HTTPS not configured)
- [ ] Existing CSP tests pass
