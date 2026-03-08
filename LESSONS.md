# LESSONS.md — IronLayer Lessons Learned Log

Dated entries capturing non-obvious findings, root causes, and process improvements
discovered during development. Add an entry when completing any backlog item that
surfaces a recurring pattern, gotcha, or architectural decision worth preserving.

Format:
```
## YYYY-MM-DD — <short title>
**Backlog item:** BL-XXX
**Root cause / finding:** ...
**What to do differently:** ...
```

---

## 2026-03-07 — Initial Production Readiness Audit

**Backlog item:** (pre-backlog audit)

**Root cause / findings:**

1. **Pytest venv shebang broken (recurring risk)**: When a venv is created from a
   symlinked or moved path, the shebang in `.venv/bin/pytest` bakes in the absolute
   path at creation time. If the repo moves or the symlink changes, the shebang
   breaks silently. Always use `python -m pytest` — never the bare `pytest` binary.
   Add this to CLAUDE.md and onboarding docs.

2. **HTTPException missing import (F821) survived code review**: The `models.py`
   router used `HTTPException` in 4 places without importing it. This is a runtime
   crash waiting to happen. Ruff's `F821` check catches this statically, but only if
   ruff runs against the specific file. The CI `ruff check .` run should have caught
   this — investigate why it didn't block the PR that introduced it.

3. **sync-to-public token sanitization invalidates tests**: The `sync-to-public.yml`
   workflow replaces real Databricks PATs (`dapi` + 32 hex chars) with
   `dapi_FAKE_TOKEN_FOR_TESTING`. Test assertions written against real token formats
   break on the sanitized string because `dapi_` has underscores and non-hex chars
   that don't match the PII detection regex. Rule: when sanitizing tokens for OSS
   publish, use a sanitized string that STILL matches the detection pattern, or
   update tests to use format-correct fake tokens (e.g., `dapi` + 32 `a`s).

4. **sqlglot version sensitivity in SQL validators**: `_validate_syntax` behavior
   changed between sqlglot v24 (permissive — accepted plain English as column refs)
   and v25+ (stricter). Tests written on one version break on another. Rule: always
   pin sqlglot to a minor version in tests (`sqlglot==26.x`) and test boundary
   behaviors explicitly.

5. **CLI tests silently broken**: All 8 CLI test files fail to collect because
   `core_engine` isn't on `PYTHONPATH`. This went undetected because the venv
   shebang failure masked the collection error in local runs. CI should enforce
   zero collection errors as a hard gate separate from test pass/fail counts.

6. **Coverage thresholds inconsistent across packages**: api enforces no explicit
   threshold in CI (`--cov` with no `--cov-fail-under`), core_engine has 70% in
   Makefile but CI uses 60%, ai_engine has no threshold. Result: coverage degrades
   silently. Rule: align all thresholds in one place (CI yml) and enforce them.

7. **security.py at 38% coverage**: The single most security-critical file in the
   codebase. The multi-auth path (JWT/KMS/OIDC), SSRF protection, credential
   encryption, and token revocation are largely untested. Any regression in these
   paths is invisible until a customer hits it in production.

8. **In-memory rate limiting invisible in load tests**: Rate limits work correctly
   in single-replica development but silently fail at scale. Load test suites must
   simulate multi-replica scenarios to catch this class of bug.

9. **`TokenManager | None` without None guard in auth.py:333**: FastAPI dependency
   injection can return `None` for optional dependencies. Accessing `.validate_token`
   on a `None` `TokenManager` raises `AttributeError` in auth middleware, which
   results in a 500 response that looks like a server error, not an auth error.
   Rule: always guard `Optional` dependency results before calling methods on them.

10. **Event bus is in-memory and lossy**: Any event published between a successful
    DB write and the event delivery is lost if the process crashes. For billing
    events and audit trail entries this is a correctness bug, not just a reliability
    bug.

**What to do differently:**
- Run `ruff check --select=F821` as a blocking pre-commit hook (not just CI)
- Use format-correct fake tokens in tests (`dapi` + 32 hex digits)
- Enforce `--cov-fail-under=75` uniformly across all packages in CI
- Write tests for security.py immediately after every new auth code path
- Add CLI `PYTHONPATH` setup to conftest.py so tests are never silently broken

---

## 2026-03-07 — P0 Items Implementation (BL-001 through BL-004)

**Backlog items:** BL-001, BL-002, BL-003, BL-004

**Root cause / findings:**

1. **Venv `.pth` editable-install files had the same broken path as the shebang**
   (BL-002): The original audit identified the shebang as broken. During
   implementation we discovered ALL editable-install `.pth` files also baked
   in the wrong path (`/GitHub Repos/ironlayer_OSS/` instead of `/GitHub
   Repos/IronLayer/ironlayer_OSS/`). Python silently ignores `.pth` entries
   for non-existent paths, so ALL workspace packages (`core_engine`, `api`,
   `cli`) were non-importable through the venv. Tests only passed when
   PYTHONPATH was set explicitly. **Rule: when a venv shebang is broken, ALL
   `.pth` files should be suspected too. Only a full rebuild (`uv venv && uv
   sync`) fixes both — Option B (sed shebang patch) would have left `.pth`
   files broken.**

2. **BL-002 venv rebuild completely fixed BL-003 (CLI collection errors)**:
   The 8 CLI test collection errors were caused by the same broken `.pth`
   files. After the venv rebuild, all 322 CLI tests collected and ran with
   zero changes to pyproject.toml required. The `pythonpath` safety net was
   still added to `cli/pyproject.toml` (defensive measure).

3. **sqlglot 29.x renamed `Explain` expression type to `Command`** (BL-004):
   `sqlglot.expressions.Explain` does not exist in sqlglot 29.0.1 — EXPLAIN
   statements fall back to `Command`. The `_SAFE_STATEMENT_TYPES` frozenset
   in `suggestion_validator.py` used the string `"Explain"` (checked via
   `type(stmt).__name__`), which still matched because the string was correct.
   But adding `sqlglot_exp.Explain` as a class reference raised `AttributeError`
   at import time. **Rule: when referencing sqlglot expression types by class
   (not by string), verify the class exists in the installed version. Prefer
   `type(stmt).__name__` checks or hasattr guards when sqlglot version may vary.**

4. **"THIS IS NOT SQL" parses to `Not` type in sqlglot 29** (BL-004): The
   `IS NOT` in the phrase is parsed as a NOT expression. The `_validate_syntax`
   fix using `isinstance(stmt, _SQL_STATEMENT_TYPES)` correctly rejects this
   because `Not` is not in the statement types tuple — no special-casing needed.

5. **sync-to-public.yml token replacement off by 2 characters**: The replacement
   string `dapiaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa` was 34 chars, not 36 (needed
   `dapi` + 32 `a`s = 36). Visual counting of repeated characters in sed
   replacements is unreliable. **Rule: always verify generated token strings
   with `python3 -c "print(len('...'))"` or regex test before committing
   sed patterns.**

**What to do differently:**
- After ANY venv issue, check `.pth` files as well as the shebang — they share the same creation-time path
- Use `type(stmt).__name__` string checks for sqlglot expression types to avoid `AttributeError` on version upgrades
- Use `python3 -c "print('dapi' + 'a'*32)"` to generate exact replacement strings; never count repeated chars visually

---

## P1 Sprint (2026-03-07)

### BL-010: JWT Secret Rotation
- **What**: Added `jwt_secret_previous: SecretStr | None` to `TokenConfig` and fallback logic in `_validate_jwt_token` that tries the previous secret on `InvalidSignatureError`.
- **Lesson**: Secret rotation requires ordered fallback, not parallel validation. Expire failures are always immediate (don't retry with old secret). `JWT_SECRET_PREVIOUS` env var must have no `API_` prefix to mirror `JWT_SECRET` convention.

### BL-011: Webhook Secret Entropy
- **What**: Raised `min_length` from 8 to 32 on `EventSubscriptionCreate.secret` and `EventSubscriptionUpdate.secret`.
- **Lesson**: 8-char minimum is inadequate for HMAC secrets used in webhook signatures — an attacker can brute-force 8-char ASCII secrets. 32 chars provides ~190 bits entropy with full ASCII.

### BL-012: OIDC DNS Async Chain
- **What**: Made `_validate_url_safe` async using `asyncio.get_running_loop().getaddrinfo()` with 2s timeout; propagated `async def` up through `OIDCProvider` and `TokenManager._validate_oidc_token`; added `validate_token_async()` for async callers.
- **Lesson**: Never make `TokenManager.validate_token` async if it's also tested synchronously — add a parallel `validate_token_async` method instead. Keep sync path for non-OIDC modes (dev/JWT/KMS) so existing sync tests pass without changes. The OIDC-specific tests that call `validate_token` directly need `@pytest.mark.asyncio` and `await`.

### BL-017: Transactional Event Outbox
- **What**: Added `EventOutboxTable` to `tables.py`, `EventOutboxRepository` to `repository.py`, `EventBus.emit_persistent()` method, `OutboxPoller` background task, and `init_outbox_poller` / `get_outbox_poller` module-level functions in `event_bus.py`.
- **Lesson**: The outbox table's `status` column needs a `CheckConstraint` with the exact values (`pending/delivered/failed`) to catch application bugs early. The poller must commit after each individual entry (not after the whole batch) to avoid losing all progress on a single handler failure. Keep the poller's `_poll_once` loop a flat for-loop rather than `asyncio.gather` so one failing entry doesn't cancel others.

### BL-018: Atomic Budget Enforcement
- **What**: Added `_TENANT_BUDGET_LOCKS: dict[str, asyncio.Lock]` and `_get_tenant_lock()` to `budget_guard.py`. Added a `lock` property on `BudgetGuard` that returns the per-tenant lock.
- **Lesson**: The lock must be module-level (not instance-level) because each request creates a new `BudgetGuard` instance. Asyncio is single-threaded so a plain dict is safe without a meta-lock. Callers MUST hold the lock across both `check_budget()` and `record_usage()` — the property alone does not enforce this, only documents it.

### BL-040: AI Engine Async/Sync Impedance + BudgetGuard Wiring
- **What**: Made `LLMClient._call_llm`, `classify_change`, `suggest_optimization`, `SemanticClassifier.classify`, `_llm_enrich`, `SQLOptimizer.optimize`, `_llm_suggestions`, and `EvaluationHarness.run_full_evaluation` all `async def`. Replaced `anthropic.Anthropic()` with `AsyncAnthropic()`. Created `InMemoryLLMUsageRepo` for budget tracking without a DB. Wired `BudgetGuard` in `main.py` lifespan. Updated all four affected test files (29 + 25 + full rewrite + regression) to use `async def` and `AsyncMock`.
- **Lesson 1 — Sync methods in async services are silent until they're not**: `SemanticClassifier.classify()` was `def`, not `async def`, called from an async FastAPI handler without `await`. It returned a real value (not a coroutine), so it appeared to work. But every LLM call inside blocked the event loop for the call's full duration (500ms–3s+), stalling all concurrent requests. The bug is completely invisible in unit tests and only surfaces under LLM-enabled load. **Rule: in any FastAPI or other async service, ALL methods that do I/O — even transitively — must be `async def`. If a method is `def` in an async context, it must provably do no I/O.**
- **Lesson 2 — Dead-code review should check constructor call sites, not just implementations**: BL-018 correctly implemented `BudgetGuard` with proper async locking and usage tracking. But `main.py` called `LLMClient(settings)` — the `budget_guard=` argument was never passed. Every test for `BudgetGuard` passed. The integration was silently no-op. **Rule: after implementing a new cross-cutting concern (budget guard, rate limiter, audit logger), grep for ALL call sites of the constructor that should wire it up. A missing keyword argument is not a compile error.**
- **Lesson 3 — Stateless services need in-process repos, not DB**: `BudgetGuard` requires a usage repo. The AI engine has no DB. Rather than adding a DB connection or moving enforcement to the API layer, an `InMemoryLLMUsageRepo` satisfies the interface with zero infrastructure. It's not durable (restarts reset counters) but is correct for platform-level rate limiting where brief windows of over-spending are acceptable. **Rule: when a stateless service needs to track state, evaluate in-process storage first — it may be the right trade-off.**
- **Lesson 4 — `MagicMock()` without spec does not auto-create `AsyncMock` for async methods**: `MagicMock(spec=MyClass)` auto-detects async methods and creates `AsyncMock` for them in Python 3.8+. But `MagicMock()` (no spec) does not. If you write `mock.classify = lambda *a, **kw: SemanticClassifyResponse(...)` or set `mock.classify.return_value = ...` on a `MagicMock()`, calling `await mock.classify(...)` raises `TypeError: object MagicMock can't be used in 'await' expression`. **Rule: when mocking async methods without spec, assign `mock.method = AsyncMock(return_value=...)` explicitly.**
- **Lesson 5 — The evaluation harness is a third caller, not just tests + routers**: When making `classify()` and `optimize()` async, we updated the two routers and all direct test calls. But `EvaluationHarness.run_full_evaluation()` was a third synchronous caller that also called these methods — found only when running the regression tests and seeing `AttributeError: 'coroutine' object has no attribute 'change_type'`. **Rule: before making any method async, grep ALL call sites — not just the ones you know about.**

### BL-038: Pre-commit F821
- **What**: Added `--extend-select=F821` to the ruff pre-commit hook and a `[tool.ruff.lint]` section in `pyproject.toml`.
- **Lesson**: F821 (undefined name) is not in ruff's default set because it can false-positive on dynamic code. For this repo it's safe and caught the `HTTPException` bug (BL-001). Using `extend-select` rather than full `select` preserves all other default rules.

### BL-039: Tenant RLS Integration Tests
- **What**: Created `api/tests/integration/test_tenant_isolation.py` with 7 test classes and 24 test methods covering cross-tenant 404s, token scoping, and cross-tenant ID injection.
- **Lesson**: When mocking `PlanService.get_plan()` return values, return a plain dict matching the exact shape the real service produces (after JSON parsing), not the raw ORM row. FastAPI serialises the mock return against the `response_model` schema — MagicMock attributes fail Pydantic's string-type validation silently until you check `ResponseValidationError` logs.

## 2026-03-07 — P2 Quality Sprint (BL-013 through BL-027, BL-036, BL-037)

### BL-016: Zero-Dependency Circuit Breaker
- **What**: Added `_CircuitBreaker` class directly to `ai_client.py` without adding `tenacity` or `pybreaker` as dependencies. States: closed→open→half_open→closed. Trips only on `httpx.RequestError` (network failure), not `HTTPStatusError` (server responded = circuit should stay closed).
- **Lesson**: Separate network-level failures from application-level failures when deciding circuit breaker trip conditions. A 500 response means the server is alive — the circuit should stay closed. Only network timeouts and connection errors warrant opening. A simple 30-line class is often all you need; avoid library overhead for a standard state machine.
- **Lesson**: Tests that bypass `__init__` via `Class.__new__(Class)` must manually set ALL instance attributes added since the test was written. When adding new instance attributes, grep for `__new__` patterns in the test suite immediately.

### BL-013 / BL-014 / BL-015: ASGI Lifespan in Async Tests
- **What**: Used `httpx.AsyncClient(app=app, base_url="http://test")` doesn't trigger ASGI lifespan events — module-level globals (engines, classifiers, etc.) remain `None`. The fix is a custom async context manager that manually sends `lifespan.startup` / `lifespan.shutdown` events before/after the client.
- **Lesson**: `ASGITransport` skips lifespan by design. If your app registers resources in `@asynccontextmanager` lifespan, tests that use `ASGITransport` directly will see `None` globals everywhere. Mirror what `TestClient` does internally: create a task for the ASGI app, drive the lifespan startup, run your test, then drive shutdown. Starlette's `TestClient` handles this automatically (it's synchronous); async tests must handle it manually.

### BL-019: Ruff F401 Auto-Fix Scale
- **What**: `ruff check --select=F401 --fix` removed 100 unused imports across `api/tests/`, `ai_engine/tests/`, and service files in a single pass.
- **Lesson**: Don't cherry-pick F401 fixes file-by-file — run the auto-fixer across the entire package at once. Check for `# noqa: F401` lines in `__init__.py` files before fixing; these re-export symbols intentionally and should be left alone.

### BL-020: mypy `no-any-return` with Third-Party SDKs
- **What**: `json.loads()`, `response.json()`, `resp.read()`, and Azure/AWS SDK return values all type-stub as `Any`. Six files needed `cast()` at return sites.
- **Lesson**: When enabling `no-any-return` in an existing codebase, the pattern is almost always `return cast(ConcreteType, result)` at the boundary with third-party APIs. Use `cast` rather than adding `# type: ignore` — cast documents intent and is checked by mypy. `warn_return_any = false` (already set) mutes the inverse problem (functions that return Any to typed callers) but `no-any-return` still catches the explicit `return <Any expression>` in typed functions.

### BL-025 / BL-026: Docker Image Digest Pinning
- **What**: Pinned all 5 base images in 3 Dockerfiles to SHA256 digests. Added `HEALTHCHECK` instructions using Python's stdlib `urllib.request` (no extra dependency) for Python containers, and `wget` (already in alpine) for nginx.
- **Lesson**: Use `docker inspect <image> --format='{{index .RepoDigests 0}}'` after `docker pull` to get the current digest. Add a comment above each `FROM` line with the tag name for human readability — the digest alone is opaque. Schedule a quarterly "refresh digests" task; stale digests mean you stay on old (potentially vulnerable) layers indefinitely.
- **Lesson**: `HEALTHCHECK` must be placed BEFORE `CMD` in the Dockerfile. The health check command runs as the container user — ensure the health check tool (python/wget) is accessible to that user. For nginx running as non-root, `wget` requires the nginx config to allow localhost connections without elevated permissions.

### BL-022 / BL-027: CI Coverage Thresholds and Migration Validation
- **What**: Raised `--cov-fail-under` from 60% to 75% (api, ai_engine) and 70% (core_engine). Added `validate-migrations` job that runs `upgrade head → downgrade -1 → upgrade head` against a fresh Postgres before any build job.
- **Lesson**: Set coverage thresholds AFTER you've done the coverage improvement work, not before. Setting them too early blocks CI without providing actionable guidance. The right time to raise a threshold is immediately after the coverage improvement sprint that clears it.
- **Lesson**: Migration downgrade tests catch a common class of bug: `upgrade` that adds a `NOT NULL` column without a default fails `downgrade` (the column still exists but now has nulls the old schema didn't expect). Testing upgrade→downgrade→upgrade in CI catches these before production.

### BL-041: Post-Sprint Code Review — False Alarms vs Genuine Defects
- **What**: A post-P2 code review flagged 10 issues. Deep reading showed 7 were false alarms, 3 were genuine.
- **Lesson (false alarms)**: Automated code-review agents frequently misidentify correct layered unit testing as "testing mock behavior". The pattern — mock `_post` in advisory-method tests while testing `_post` itself at the `httpx.AsyncClient.post` boundary — is intentional. Don't conflate "the test uses a mock" with "the test only tests the mock".
- **Lesson (false alarms)**: asyncio is single-threaded. "Thread safety" concerns about `+= 1` in a synchronous method are not applicable — context switches only occur at `await` points. Audit agent concurrency warnings against the actual execution model before acting.
- **Lesson (genuine — `time.sleep` in tests)**: Any `time.sleep()` in a pytest test is a smell. If the code under test uses `time.monotonic()`, mock it with `monkeypatch.setattr("module.time.monotonic", lambda: t[0])` and a mutable clock list `t = [start]`. This makes tests deterministic, instant, and tests more realistic values (30 s timeout instead of 10 ms).
- **Lesson (genuine — Docker HEALTHCHECK timeout)**: `urllib.request.urlopen(url)` with no timeout uses Python's global socket timeout, which defaults to `None` (blocks forever). Always pass an explicit `timeout` argument. Set it to `--timeout minus 2 s` (Docker `--timeout=5s` → `timeout=3` in Python).
- **Lesson (genuine — docstring accuracy)**: If a class docstring describes a behavioral constraint (e.g. "one probe request") but the implementation does not enforce it, fix the docstring. Misleading documentation is worse than no documentation because it causes callers to rely on behavior that doesn't exist.

---

## 2026-03-07 — P3 Sprint (BL-028 through BL-035)

### BL-028: Repository Coverage with Async SQLite
- **What**: Created `test_repository_coverage.py` (194 tests) targeting `core_engine/state/repository.py` using SQLite + aiosqlite. Coverage lifted from 29% → 90%.
- **Lesson**: Large repository files (~3700 lines) look untestable because integration tests use them transitively without counting coverage on them. Direct unit tests with a lightweight in-process DB (SQLite) are far faster to write and run than end-to-end tests and give better isolation. `asyncio_mode = "auto"` in `pyproject.toml` eliminates decorator boilerplate on every async test function.

### BL-029: OpenTelemetry Lazy-Import Pattern
- **What**: Added `_configure_otel(app: FastAPI) -> None` function to both `main.py` files. All OTel imports live inside the function body behind an `if not endpoint: return` guard.
- **Lesson**: OTel packages are heavy. Importing them unconditionally at module load time slows cold start and requires them as hard dependencies even when tracing is disabled. The lazy-import pattern — all imports inside the function, activated only when `OTEL_EXPORTER_OTLP_ENDPOINT` is set — keeps startup fast and the dependency optional. Use `# noqa: PLC0415` to suppress the "import not at top of file" lint warning. This pattern applies to any optional heavy SDK (DataDog, Sentry, etc.).
- **Lesson**: The API service should instrument both FastAPI (`FastAPIInstrumentor`) AND httpx (`HTTPXClientInstrumentor`) so the `traceparent` header propagates to downstream services. The AI engine only needs `FastAPIInstrumentor` — it doesn't make outbound HTTP calls. Don't instrument symmetrically when the services have asymmetric roles.

### BL-030: Multi-Burn-Rate Alerting (SRE Workbook Pattern)
- **What**: Wrote Prometheus alerting rules using the Google SRE Workbook multi-burn-rate pattern: fast burn (14.4×, 5m window) pages immediately; slow burn (6×, 30m window) creates a ticket. Both burn rates must fire together to reduce false positives.
- **Lesson**: Single-threshold alerts on raw error rates produce too many false positives (a 1-minute spike) or miss slow exhaustion (sustained 0.2% when SLO is 0.1%). Multi-burn-rate with dual windows gives both sensitivity (fast) and specificity (sustained). The threshold multipliers — 14.4× (1h to exhaust 30d budget) and 6× (5h to exhaust) — come directly from the SRE Workbook Chapter 5.
- **Lesson**: Grafana dashboard JSON should always use `${DS_PROMETHEUS}` variable references rather than hardcoding a datasource UID. This makes the dashboard portable across Grafana instances. The `__inputs` and `__requires` sections at the top enable `grafana-cli` import tooling.

### BL-031: GDPR Anonymization vs. Deletion
- **What**: Added `anonymize_user_entries(user_id)` that replaces `actor` with `[REDACTED]` and nulls `metadata_json`, rather than deleting rows. Added `cleanup_old_entries(retention_days)` for time-based deletion. New Alembic migration `025` adds `retention_days` with `server_default=text("365")` so existing rows are not null.
- **Lesson**: Deleting audit log rows for GDPR right-to-erasure can break audit integrity — row counts and event sequences have evidential value. Anonymizing PII fields in-place preserves the structural integrity of the audit trail while satisfying erasure requirements. Use `[REDACTED]` (bracket notation) rather than empty string or NULL for the `actor` field so query results clearly distinguish "this was anonymized" from "actor was unknown".
- **Lesson**: `server_default=text("365")` is required (not just `default=365`) when adding a NOT NULL column to a table that already has rows. SQLAlchemy's `default` is an application-level default applied when Python inserts a row; `server_default` writes a database-level DEFAULT clause so existing rows get the value via the migration itself.

### BL-032: ML Model Registry — Deque Ring Buffer for Drift Detection
- **What**: `ModelRegistry.record_prediction()` stores recent predictions in a `collections.deque(maxlen=10_000)` keyed by model name. `drift_check()` computes PSI between the stored distribution and training-time quantile bins.
- **Lesson**: PSI (Population Stability Index) requires equal-frequency binning from the training distribution, not equal-width. Compute quantiles from training data at registry load time and reuse them for all drift checks — this way the comparison is always apples-to-apples even if the prediction range shifts. PSI thresholds: <0.1 stable, 0.1–0.2 warning, >0.2 drift.
- **Lesson**: A deque with `maxlen` is the simplest possible sliding-window store for a stateless service. It requires zero infrastructure (no Redis, no DB) and automatically evicts old entries. The trade-off is that it resets on restart — acceptable for drift detection (you re-accumulate a window quickly) but not for billing or audit.
- **Lesson**: When a background agent writes a new module AND imports it into `main.py`, the concurrent main session must read `main.py` before writing to it. Running agents can modify files you haven't read yet — always re-read before editing a file that a concurrent agent may have touched.

### BL-033: SBOM — `cyclonedx-py environment` Not `cyclonedx-py poetry`
- **What**: The published SBOM example used `cyclonedx-py poetry > sbom.json`. The actual installed CLI uses `cyclonedx-py environment --output-format JSON --outfile sbom.json`.
- **Lesson**: The `cyclonedx-bom` CLI interface changes between major versions. The `poetry` subcommand was available in v3.x; v4.x switched to `environment` (captures the active virtualenv) or `requirements` (from a pip requirements file). Always pin the version in CI (`pip install cyclonedx-bom==4.x`) and validate the exact command against that version before writing the workflow step. `cyclonedx validate --input-format JSON --input-file sbom.json` catches mis-generated SBOMs immediately.

### BL-034: Azure Container Apps Canary — Revision Suffix Naming Constraints
- **What**: Implemented progressive canary deploy using Azure Container Apps multiple-revision mode. Traffic shifted 0% → 10% → 50% → 100% with auto-rollback via `trap rollback ERR`.
- **Lesson**: Azure Container Apps revision suffix names cannot contain dots. Version tags like `v1.2.3` must be sanitized to `v1-2-3` with `tr '.' '-'` before passing as `--revision-suffix`. Failing to do this gives a cryptic "invalid suffix" API error.
- **Lesson**: The `trap rollback ERR` pattern in bash is the cleanest way to implement auto-rollback: set the trap at the start, clear it with `trap - ERR` on success. Any `set -e` failure (non-zero exit from any command) automatically invokes rollback without explicit error checking at each step. Combine with `set -euo pipefail` for maximum strictness.
- **Lesson**: For smoke-testing canary revisions, target the revision FQDN directly (not the app ingress FQDN) using `az containerapp revision show --query properties.fqdn`. The ingress FQDN routes to active revisions; the revision FQDN is the canary endpoint. This ensures your smoke test hits the new code, not the old revision.

### BL-035: "Pre-completed" Items Are Still Worth Checking
- **What**: BL-035 (API versioning) was listed as [OPEN] but all 20 routers already had `prefix="/api/v1"` in `main.py`. Zero code changes needed.
- **Lesson**: Before implementing a backlog item, do a 30-second grep to verify the current state. It's common for work to be done incidentally as part of a related item. Catching this early saves implementation time and avoids accidentally "re-implementing" something in a slightly different way. Mark these as [DONE] with a note explaining why no changes were needed — it's useful audit trail for reviewers.

---

## 2026-03-07 — P3 Post-Sprint Quality Corrections (BL-042)

### Accessing a collaborator's private `_session` attribute to commit
- **What**: `AuditService.cleanup_old_logs` and `anonymize_user_data` called `await self._repo._session.commit()` — reaching into a private attribute of a collaborating object to trigger a side effect.
- **Why it's wrong**: It violates encapsulation (the repo's session is private by convention), and it bypasses the framework's transaction boundary. The service had already received the session in its own `__init__` — it just forgot to store it. When a service or class needs to commit, it must hold a direct reference to the session, not reach through a collaborator.
- **Fix**: Add `self._session = session` to `AuditService.__init__` and commit via `self._session.commit()`.
- **Rule**: Never access `obj._private_attribute` of a collaborator object to implement your own behavior. If you find yourself doing it, add the direct reference in your own constructor.

### GDPR anonymization silently breaking hash-chain verification
- **What**: `anonymize_user_entries` replaced `actor` and `metadata_json` with redacted values. `verify_chain` recomputes each entry's hash from its current fields — after anonymization, the recomputed hash differs from the stored hash (which was computed from the original data). Result: `verify_chain` permanently returned `False` for any tenant with GDPR erasure applied, making it impossible to distinguish legitimate erasure from real tampering.
- **Why it's subtle**: Both GDPR right-to-erasure AND hash-chain tamper detection are correctness requirements. They're in fundamental tension: erasure mutates data that the hash was computed from. The collision was invisible in tests because tests didn't chain erasure with verification.
- **Fix**: Add `is_anonymized: bool` column (Alembic 026). Set it to `True` during anonymization. In `verify_chain`, skip hash recomputation for `is_anonymized=True` entries — the stored `entry_hash` (from original data) still advances the chain link correctly, keeping all surrounding non-anonymized entries fully verifiable. The anonymized entry itself is "trust-broken" (you can't verify its original content) but the chain around it is intact.
- **Rule**: When two correctness requirements interact (audit integrity + GDPR erasure), explicitly design the interaction at schema level with a sentinel column. Don't let either requirement silently invalidate the other.

### `dict` iteration order is insertion order — not version order
- **What**: `ModelRegistry._current_version` iterated `self._records` (a `dict[tuple[str,str], ModelRecord]`) and returned the first version key found for the given model name. In Python 3.7+, dict iteration is insertion-ordered — so the FIRST loaded version was returned, not the most recently loaded one. When `load_model("model", "1.0.0")` was followed by `load_model("model", "2.0.0")`, predictions would be tagged with version `1.0.0` instead of `2.0.0`.
- **Why it's subtle**: The bug is invisible when only one version is ever loaded (the common case). It only surfaces when multiple versions are loaded in the same process lifetime — an uncommon but legitimate scenario during canary rollouts.
- **Fix**: Add `self._active_version: dict[str, str] = {}` to `__init__`. Update it on every successful `load_model` call. `_current_version` becomes a single `.get()` call.
- **Rule**: When you need "most recently modified" semantics, use an explicit tracking dict with direct assignment — don't rely on dict iteration order to give you recency.

### Azure Container Apps: `[?properties.active]` JMESPath filter is unreliable
- **What**: `rollback.sh` used `[?properties.active]` to filter revisions, assuming it would return only "currently serving" revisions. Azure Container Apps marks ALL deployed revisions as `active: true` until they're explicitly deactivated — so the filter returned all revisions, not just the ones serving traffic.
- **Fix**: Remove the filter. Use `sort_by(@, &properties.createdTime)[-2].name` to get the second-most-recently-created revision, regardless of active state. This reliably finds the previous deploy.
- **Rule**: When using cloud provider APIs for automation, verify the exact semantics of filtering fields in a real environment. Don't assume "active" means "serving traffic" — it often means "not deleted."

### `uv pip install` in CI pollutes the SBOM with the SBOM tool itself
- **What**: The SBOM generation step ran `uv pip install cyclonedx-bom` before generating the SBOM. Since `uv pip install` adds the package to the active workspace venv, `cyclonedx-bom` itself appeared in the SBOM it was generating.
- **Fix**: Use `uv tool install cyclonedx-bom` instead. `uv tool` installs packages into an isolated global tool directory (separate from the project venv), so the tool is available on PATH but doesn't appear in the project's dependency graph.
- **Rule**: Never use `pip install` (or `uv pip install`) to install dev tools in CI pipelines that later inspect the same environment. Use isolated tool runners (`uv tool install`, `pipx`, `uvx`) for tools that must not appear in the project's dependency tree.

### Alert deduplication: don't page twice for the same threshold
- **What**: `APIErrorSpike` paged when error rate > 1%. `APIHighErrorRateFastBurn` also paged at 14.4× × 0.1% = 1.44%. At error rates between 1% and 1.44%, only `APIErrorSpike` fired; above 1.44%, BOTH fired simultaneously — double-paging for the same incident.
- **Fix**: Raise `APIErrorSpike` threshold to > 2% (clearly above the fast-burn trigger) and demote severity to `ticket`. The fast-burn alert handles the paging; `APIErrorSpike` acts as a catch-all for extreme rates that somehow bypass the burn-rate windows.
- **Rule**: When designing layered alerting (burn-rate + absolute threshold), ensure absolute thresholds are strictly above the highest burn-rate trigger to avoid alert fanout. Document the overlap analysis in the alert annotation.
