"""Orchestrates plan generation, persistence, and AI augmentation."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import date
from pathlib import Path
from typing import Any, cast

from core_engine.contracts.schema_validator import (
    ContractValidationResult,
    validate_schema_contracts_batch,
)
from core_engine.diff.structural_diff import compute_structural_diff
from core_engine.graph.dag_builder import build_dag
from core_engine.loader.model_loader import load_models_from_directory
from core_engine.metering.collector import MeteringCollector
from core_engine.metering.events import UsageEventType
from core_engine.models.diff import DiffResult
from core_engine.models.model_definition import ModelDefinition, SchemaContractMode
from core_engine.models.plan import Plan
from core_engine.parser.sql_guard import (
    SQLGuardConfig,
    SQLGuardViolation,
    UnsafeSQLError,
    check_sql_safety,
)
from core_engine.planner import generate_plan
from core_engine.state.repository import (
    ModelRepository,
    PlanRepository,
    RunRepository,
    TenantConfigRepository,
    WatermarkRepository,
)
from sqlalchemy.ext.asyncio import AsyncSession

from api.config import APISettings
from api.services.ai_client import AIServiceClient

logger = logging.getLogger(__name__)

# BL-094: Redis cache TTL for plan JSON (5 minutes).
PLAN_CACHE_TTL = 300

_GIT_SHA_PATTERN = re.compile(r"^[0-9a-fA-F]{4,40}$")


def _plan_cache_key_for(tenant_id: str, plan_id: str) -> str:
    """Canonical cache key for a plan — single source of truth."""
    return f"plan:{tenant_id}:{plan_id}"


async def invalidate_plan_cache(redis: Any, tenant_id: str, plan_id: str) -> None:
    """Evict a single plan from the Redis cache (standalone helper).

    This is the canonical invalidation function — use it from any module
    (e.g. approval/rejection routers) instead of hard-coding the key format.
    Fail-open: Redis errors are swallowed.
    """
    if redis is None:
        return
    try:
        await redis.delete(_plan_cache_key_for(tenant_id, plan_id))
        logger.debug("Plan cache INVALIDATED for %s (standalone)", plan_id[:12])
    except Exception:
        logger.debug("Plan cache DELETE failed (Redis unavailable)", exc_info=True)


class PlanService:
    """High-level service coordinating plan lifecycle operations.

    Parameters
    ----------
    session:
        Active database session for state-store access.
    ai_client:
        HTTP client for the AI advisory engine.
    settings:
        Application settings.
    """

    def __init__(
        self,
        session: AsyncSession,
        ai_client: AIServiceClient,
        settings: APISettings,
        *,
        tenant_id: str = "default",
        metering: MeteringCollector | None = None,
        redis: Any | None = None,
    ) -> None:
        self._session = session
        self._ai = ai_client
        self._settings = settings
        self._tenant_id = tenant_id
        self._metering = metering
        self._redis = redis  # BL-094: optional Redis client for plan caching
        self._plan_repo = PlanRepository(session, tenant_id=tenant_id)
        self._model_repo = ModelRepository(session, tenant_id=tenant_id)
        self._watermark_repo = WatermarkRepository(session, tenant_id=tenant_id)
        self._run_repo = RunRepository(session, tenant_id=tenant_id)
        self._tenant_config_repo = TenantConfigRepository(session, tenant_id=tenant_id)

    # ------------------------------------------------------------------
    # Plan generation
    # ------------------------------------------------------------------

    async def generate_plan(
        self,
        repo_path: str,
        base_sha: str,
        target_sha: str,
    ) -> dict[str, Any]:
        """Generate a deterministic execution plan from a git diff.

        Steps
        -----
        1. Validate the repository path.
        2. Load model definitions from the repo.
        3. Build the dependency DAG.
        4. Identify changed files between ``base_sha`` and ``target_sha``.
        5. Parse models at both commits to produce content-hash snapshots.
        6. Compute structural diff.
        7. Gather watermarks and historical stats from the database.
        8. Invoke the interval planner.
        9. Persist the plan.
        10. Return the plan as a dictionary.
        """
        from api.validation import resolve_repo_path_under_base

        allowed_base = Path(self._settings.allowed_repo_base).resolve()
        repo = resolve_repo_path_under_base(repo_path, allowed_base)

        if not (repo / ".git").is_dir():
            raise ValueError(f"Not a valid git repository: {repo_path}")

        # Load models at the target commit ---------------------------------
        models_dir = self._resolve_models_dir(repo)
        model_list = load_models_from_directory(models_dir)
        models_by_name: dict[str, ModelDefinition] = {m.name: m for m in model_list}

        # Meter model loading.
        if self._metering is not None:
            self._metering.record_event(
                tenant_id=self._tenant_id,
                event_type=UsageEventType.MODEL_LOADED,
                quantity=len(model_list),
                metadata={
                    "repo_path": repo_path,
                    "model_names": [m.name for m in model_list],
                },
            )

        # SQL safety check -- catch dangerous operations early -------------
        self._validate_models_sql_safety(model_list)

        # Build DAG ---------------------------------------------------------
        dag = build_dag(model_list)

        # Identify changed SQL files between base and target ----------------
        changed_files = await self._git_changed_files(repo, base_sha, target_sha)

        # Build content-hash snapshots at base and target -------------------
        base_versions = await self._build_version_map(repo, base_sha, changed_files)
        target_versions: dict[str, str] = {m.name: m.content_hash for m in model_list}

        # Structural diff ---------------------------------------------------
        diff_result: DiffResult = compute_structural_diff(base_versions, target_versions)

        # Watermarks and historical stats — fetched in batch to avoid N+1 ----
        model_name_list = list(models_by_name.keys())
        watermarks: dict[str, tuple[Any, Any]] = await self._watermark_repo.get_watermarks_batch(
            model_name_list
        )
        batch_stats = await self._run_repo.get_historical_stats_batch(model_name_list)
        run_stats: dict[str, dict[str, Any]] = {
            name: stats for name, stats in batch_stats.items() if stats["run_count"] > 0
        }

        # Run schema contract validation for models with active contracts ---
        models_with_contracts = [m for m in model_list if m.contract_mode != SchemaContractMode.DISABLED]
        contract_results: ContractValidationResult | None = None
        if models_with_contracts:
            contract_results = validate_schema_contracts_batch(models_with_contracts)
            if contract_results.violations:
                logger.info(
                    "Schema contract validation: %d violation(s) across %d model(s) (%d breaking)",
                    len(contract_results.violations),
                    contract_results.models_checked,
                    contract_results.breaking_count,
                )

        # Generate plan ------------------------------------------------------
        plan: Plan = generate_plan(
            models=models_by_name,
            diff_result=diff_result,
            dag=dag,
            watermarks=watermarks,
            run_stats=run_stats,
            base=base_sha,
            target=target_sha,
            as_of_date=date.today(),
            contract_results=contract_results,
        )

        # Persist ------------------------------------------------------------
        plan_json_str = plan.model_dump_json(indent=2)
        saved_row = await self._plan_repo.save_plan(
            plan_id=plan.plan_id,
            base_sha=base_sha,
            target_sha=target_sha,
            plan_json=plan_json_str,
        )

        # BL-094: Pre-warm the read cache so the first GET is a cache hit.
        # The cached shape must match what get_plan() builds from a DB row
        # (including created_at) so callers see a consistent dict regardless
        # of whether the response came from cache or DB.
        # save_plan() returns the flushed PlanTable row, so created_at is
        # already populated by the DB default — no re-fetch needed.
        plan_dict = plan.model_dump()
        plan_dict.setdefault("approvals", [])
        plan_dict.setdefault("auto_approved", False)
        plan_dict["created_at"] = (
            saved_row.created_at.isoformat() if saved_row.created_at else None
        )
        await self._cache_set(plan.plan_id, plan_dict)

        return plan_dict

    # ------------------------------------------------------------------
    # AI augmentation
    # ------------------------------------------------------------------

    async def generate_augmented_plan(self, plan_id: str) -> dict[str, Any]:
        """Attach AI advisory metadata to an existing plan.

        If the AI engine is unavailable the plan is returned as-is with
        a ``None`` advisory payload and a warning is logged.

        Respects per-tenant LLM opt-out: if ``tenant_config.llm_enabled``
        is ``False``, all LLM calls are skipped and only rule-based
        advisory results are returned.
        """
        plan_row = await self._plan_repo.get_plan(plan_id)
        if plan_row is None:
            raise ValueError(f"Plan {plan_id} not found")

        # Look up per-tenant LLM opt-out.
        tenant_config = await self._tenant_config_repo.get()
        llm_enabled = tenant_config.llm_enabled if tenant_config is not None else True

        # Fetch per-tenant LLM API key (if stored).
        tenant_api_key: str | None = None
        if llm_enabled:
            try:
                from api.security import CredentialVault

                vault = CredentialVault(self._settings.credential_encryption_key.get_secret_value())
                tenant_api_key = await vault.get_credential(self._session, self._tenant_id, "llm_api_key")
            except Exception:
                logger.debug(
                    "Could not retrieve tenant LLM key for %s — using platform key",
                    self._tenant_id,
                )

        plan_data: dict[str, Any] = json.loads(plan_row.plan_json)  # type: ignore[arg-type]
        advisory: dict[str, Any] = {}

        steps = plan_data.get("steps", [])
        step_model_names = [step.get("model", "") for step in steps if step.get("model", "")]

        # Pre-load all per-model DB data in batch before entering the step loop.
        batch_stats = await self._run_repo.get_historical_stats_batch(step_model_names)
        batch_failure_rates = await self._run_repo.get_failure_rates_batch(step_model_names)
        batch_models = await self._model_repo.get_models_batch(step_model_names)

        for step in steps:
            model_name = step.get("model", "")
            if not model_name:
                continue

            model_advisory: dict[str, Any] = {}

            # Semantic classification ----------------------------------------
            classification = await self._ai.semantic_classify(
                old_sql="",
                new_sql=step.get("reason", ""),
                tenant_id=self._tenant_id,
                llm_enabled=llm_enabled,
                api_key=tenant_api_key,
            )
            if classification is not None:
                model_advisory["semantic_classification"] = classification
                if self._metering is not None:
                    self._metering.record_event(
                        tenant_id=self._tenant_id,
                        event_type=UsageEventType.AI_CALL,
                        metadata={"call_type": "semantic_classify", "model": model_name},
                    )

            # Cost prediction — use pre-loaded batch stats -------------------
            stats = batch_stats.get(model_name, {"avg_runtime_seconds": None, "avg_cost_usd": None, "run_count": 0})
            avg_runtime = stats.get("avg_runtime_seconds")
            cost_pred = await self._ai.predict_cost(
                model_name=model_name,
                partition_count=1,
                historical_runtime_avg=avg_runtime,
                data_volume=None,
                cluster_size="small",
                tenant_id=self._tenant_id,
                llm_enabled=llm_enabled,
            )
            if cost_pred is not None:
                model_advisory["cost_prediction"] = cost_pred
                if self._metering is not None:
                    self._metering.record_event(
                        tenant_id=self._tenant_id,
                        event_type=UsageEventType.AI_CALL,
                        metadata={"call_type": "predict_cost", "model": model_name},
                    )

            # Risk scoring — use pre-loaded batch model/failure data ---------
            model_row = batch_models.get(model_name)
            tags: list[str] = json.loads(model_row.tags) if model_row and model_row.tags else []
            failure_rate = batch_failure_rates.get(model_name, 0.0)

            risk = await self._ai.score_risk(
                model_name=model_name,
                downstream_depth=0,
                sla_tags=[],
                dashboard_deps=[],
                model_tags=tags,
                failure_rate=failure_rate,
                tenant_id=self._tenant_id,
                llm_enabled=llm_enabled,
            )
            if risk is not None:
                model_advisory["risk_score"] = risk
                if self._metering is not None:
                    self._metering.record_event(
                        tenant_id=self._tenant_id,
                        event_type=UsageEventType.AI_CALL,
                        metadata={"call_type": "score_risk", "model": model_name},
                    )

            if model_advisory:
                advisory[model_name] = model_advisory

        plan_data["advisory"] = advisory if advisory else None

        # Ensure the response shape matches get_plan() — include DB-managed
        # fields so callers get a consistent dict regardless of endpoint.
        plan_data.setdefault(
            "approvals",
            json.loads(plan_row.approvals_json) if plan_row.approvals_json else [],  # type: ignore[arg-type]
        )
        plan_data.setdefault("auto_approved", plan_row.auto_approved)
        plan_data.setdefault(
            "created_at",
            plan_row.created_at.isoformat() if plan_row.created_at else None,
        )
        return plan_data

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # BL-094: Redis cache helpers (fail-open)
    # ------------------------------------------------------------------

    def _plan_cache_key(self, plan_id: str) -> str:
        """Return the Redis key for a plan, scoped to the tenant."""
        return _plan_cache_key_for(self._tenant_id, plan_id)

    async def _cache_get(self, plan_id: str) -> dict[str, Any] | None:
        """Try to load plan data from Redis; return None on miss or error."""
        if self._redis is None:
            return None
        try:
            raw = await self._redis.get(self._plan_cache_key(plan_id))
            if raw is not None:
                logger.debug("Plan cache HIT for %s", plan_id[:12])
                return cast(dict[str, Any], json.loads(raw))
        except Exception:
            logger.debug("Plan cache GET failed (Redis unavailable) — falling back to DB", exc_info=True)
        return None

    async def _cache_set(self, plan_id: str, data: dict[str, Any]) -> None:
        """Persist plan data to Redis with the configured TTL; fail-open."""
        if self._redis is None:
            return
        try:
            await self._redis.setex(self._plan_cache_key(plan_id), PLAN_CACHE_TTL, json.dumps(data))
            logger.debug("Plan cache SET for %s (TTL=%ds)", plan_id[:12], PLAN_CACHE_TTL)
        except Exception:
            logger.debug("Plan cache SET failed (Redis unavailable) — continuing without cache", exc_info=True)

    async def invalidate_plan(self, plan_id: str) -> None:
        """Evict a plan from the Redis cache (BL-094: called after mutations)."""
        if self._redis is None:
            return
        try:
            await self._redis.delete(self._plan_cache_key(plan_id))
            logger.debug("Plan cache INVALIDATED for %s", plan_id[:12])
        except Exception:
            logger.debug("Plan cache DELETE failed (Redis unavailable)", exc_info=True)

    async def get_plan(self, plan_id: str) -> dict[str, Any] | None:
        """Load a single plan by ID.

        BL-094: checks Redis cache first (5-minute TTL); falls back to the
        database on cache miss, Redis error, or when Redis is not configured.
        """
        cached = await self._cache_get(plan_id)
        if cached is not None:
            return cached
        row = await self._plan_repo.get_plan(plan_id)
        if row is None:
            return None
        data = json.loads(row.plan_json)  # type: ignore[arg-type]
        data["approvals"] = json.loads(row.approvals_json) if row.approvals_json else []  # type: ignore[arg-type]
        data["auto_approved"] = row.auto_approved
        data["created_at"] = row.created_at.isoformat() if row.created_at else None
        result = cast(dict[str, Any], data)
        await self._cache_set(plan_id, result)
        return result

    async def list_plans(self, limit: int = 20, offset: int = 0) -> list[dict[str, Any]]:
        """Return a paginated list of plan summaries (offset-based, for backward compat)."""
        rows = await self._plan_repo.list_recent(limit=limit, offset=offset)
        return self._rows_to_summaries(rows)

    async def list_plans_after_cursor(
        self,
        cursor_ts: str,
        cursor_id: str,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """BL-120: Return plans using keyset (cursor) pagination.

        Queries ``WHERE (created_at < cursor_ts) OR (created_at = cursor_ts AND
        plan_id < cursor_id)`` — O(1) with a composite index.
        """
        rows = await self._plan_repo.list_after_cursor(
            cursor_ts=cursor_ts, cursor_id=cursor_id, limit=limit
        )
        return self._rows_to_summaries(rows)

    def _rows_to_summaries(self, rows: list) -> list[dict[str, Any]]:
        """Convert PlanTable rows to summary dicts (shared by list methods)."""
        summaries: list[dict[str, Any]] = []
        for row in rows:
            plan_data = json.loads(row.plan_json)  # type: ignore[arg-type]
            summary = plan_data.get("summary", {})
            summaries.append(
                {
                    "plan_id": row.plan_id,
                    "base_sha": row.base_sha,
                    "target_sha": row.target_sha,
                    "total_steps": summary.get("total_steps", 0),
                    "estimated_cost_usd": summary.get("estimated_cost_usd", 0.0),
                    "models_changed": summary.get("models_changed", []),
                    "created_at": (row.created_at.isoformat() if row.created_at else None),
                }
            )
        return summaries

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_models_sql_safety(
        models: list[ModelDefinition],
        config: SQLGuardConfig | None = None,
    ) -> None:
        """Run the SQL safety guard on every model's clean SQL.

        Aggregates all CRITICAL violations across models into a single
        :class:`UnsafeSQLError` so the caller receives a complete picture.
        Non-critical violations are logged as warnings.
        """
        all_violations: list[SQLGuardViolation] = []
        for model in models:
            sql = model.clean_sql or model.raw_sql
            if not sql.strip():
                continue
            violations = check_sql_safety(sql, config)
            for v in violations:
                logger.warning(
                    "SQL guard violation in model '%s': [%s] %s",
                    model.name,
                    v.operation.value,
                    v.description,
                )
            all_violations.extend(violations)

        critical = [v for v in all_violations if v.severity.value == "CRITICAL"]
        if critical:
            raise UnsafeSQLError(critical)

    @staticmethod
    def _resolve_models_dir(repo: Path) -> Path:
        """Locate the ``models/`` directory within the repository."""
        candidates = [repo / "models", repo / "sql" / "models", repo / "dbt" / "models"]
        for candidate in candidates:
            if candidate.is_dir():
                return candidate
        raise ValueError(f"Cannot locate a models directory in {repo}. Looked in: {[str(c) for c in candidates]}")

    @staticmethod
    async def _git_changed_files(repo: Path, base_sha: str, target_sha: str) -> list[str]:
        """Return a list of file paths changed between two git commits."""
        for label, sha in [("base_sha", base_sha), ("target_sha", target_sha)]:
            if not _GIT_SHA_PATTERN.fullmatch(sha):
                raise ValueError(f"Invalid git SHA for {label}: must be 4-40 hex characters, got '{sha[:80]}'")

        proc = await asyncio.create_subprocess_exec(
            "git", "diff", "--name-only", base_sha, target_sha,
            cwd=str(repo),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30.0)
        if proc.returncode != 0:
            raise RuntimeError(f"git diff failed (exit {proc.returncode}): {stderr.decode().strip()}")
        return [line.strip() for line in stdout.decode().splitlines() if line.strip()]

    @staticmethod
    async def _build_version_map(repo: Path, commit_sha: str, changed_files: list[str]) -> dict[str, str]:
        """Build a model-name -> content-hash map at a given commit.

        Only models whose files appear in *changed_files* are included,
        so newly-added models (absent from the base commit) are naturally
        excluded.
        """
        version_map: dict[str, str] = {}
        for file_path in changed_files:
            if not file_path.endswith(".sql"):
                continue
            proc = await asyncio.create_subprocess_exec(
                "git", "show", f"{commit_sha}:{file_path}",
                cwd=str(repo),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, _stderr = await asyncio.wait_for(proc.communicate(), timeout=15.0)
            if proc.returncode != 0:
                # File did not exist at that commit (new file).
                continue
            sql_content = stdout.decode()
            try:
                import hashlib

                from core_engine.loader.model_loader import parse_yaml_header

                header = parse_yaml_header(sql_content)
                name = header.get("name", "")
                if name:
                    content_hash = hashlib.sha256(sql_content.encode("utf-8")).hexdigest()
                    version_map[name] = content_hash
            except Exception:
                continue
        return version_map
