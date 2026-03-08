"""Orchestrates plan generation, persistence, and AI augmentation."""

from __future__ import annotations

import json
import logging
import re
import subprocess
from datetime import date
from pathlib import Path
from typing import Any

_GIT_SHA_PATTERN = re.compile(r"^[0-9a-fA-F]{4,40}$")

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
    ) -> None:
        self._session = session
        self._ai = ai_client
        self._settings = settings
        self._tenant_id = tenant_id
        self._metering = metering
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
        repo = Path(repo_path).resolve()

        # Defense-in-depth: validate repo_path is under the allowed base.
        # Uses is_relative_to() instead of string prefix to prevent bypass
        # via paths like /workspace2/evil when base is /workspace.
        allowed_base = Path(self._settings.allowed_repo_base).resolve()
        if not repo.is_relative_to(allowed_base):
            raise ValueError(f"Repository path {repo} is outside the allowed base directory {allowed_base}")

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
        changed_files = self._git_changed_files(repo, base_sha, target_sha)

        # Build content-hash snapshots at base and target -------------------
        base_versions = self._build_version_map(repo, base_sha, changed_files)
        target_versions: dict[str, str] = {m.name: m.content_hash for m in model_list}

        # Structural diff ---------------------------------------------------
        diff_result: DiffResult = compute_structural_diff(base_versions, target_versions)

        # Watermarks and historical stats -----------------------------------
        watermarks: dict[str, tuple[Any, Any]] = {}
        run_stats: dict[str, dict[str, Any]] = {}
        for name in models_by_name:
            wm = await self._watermark_repo.get_watermark(name)
            if wm is not None:
                watermarks[name] = wm
            stats = await self._run_repo.get_historical_stats(name)
            if stats["run_count"] > 0:
                run_stats[name] = stats

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
        await self._plan_repo.save_plan(
            plan_id=plan.plan_id,
            base_sha=base_sha,
            target_sha=target_sha,
            plan_json=plan_json_str,
        )

        return plan.model_dump()

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

            # Cost prediction ------------------------------------------------
            stats = await self._run_repo.get_historical_stats(model_name)
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

            # Risk scoring ---------------------------------------------------
            model_row = await self._model_repo.get(model_name)
            tags: list[str] = json.loads(model_row.tags) if model_row and model_row.tags else []
            total_runs = await self._run_repo.count_for_model(model_name)
            failed_runs = await self._run_repo.count_by_status(model_name, "FAILED")
            failure_rate = 0.0 if total_runs == 0 else failed_runs / total_runs

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
        return plan_data

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    async def get_plan(self, plan_id: str) -> dict[str, Any] | None:
        """Load a single plan by ID."""
        row = await self._plan_repo.get_plan(plan_id)
        if row is None:
            return None
        data = json.loads(row.plan_json)  # type: ignore[arg-type]
        data["approvals"] = json.loads(row.approvals_json) if row.approvals_json else []  # type: ignore[arg-type]
        data["auto_approved"] = row.auto_approved
        data["created_at"] = row.created_at.isoformat() if row.created_at else None
        return data

    async def list_plans(self, limit: int = 20, offset: int = 0) -> list[dict[str, Any]]:
        """Return a paginated list of plan summaries."""
        rows = await self._plan_repo.list_recent(limit=limit + offset)
        # Apply manual offset since the repository only supports limit.
        rows = rows[offset : offset + limit]
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
    def _git_changed_files(repo: Path, base_sha: str, target_sha: str) -> list[str]:
        """Return a list of file paths changed between two git commits."""
        for label, sha in [("base_sha", base_sha), ("target_sha", target_sha)]:
            if not _GIT_SHA_PATTERN.fullmatch(sha):
                raise ValueError(f"Invalid git SHA for {label}: must be 4-40 hex characters, got '{sha[:80]}'")

        result = subprocess.run(
            ["git", "diff", "--name-only", base_sha, target_sha],
            cwd=str(repo),
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(f"git diff failed (exit {result.returncode}): {result.stderr.strip()}")
        return [line.strip() for line in result.stdout.splitlines() if line.strip()]

    @staticmethod
    def _build_version_map(repo: Path, commit_sha: str, changed_files: list[str]) -> dict[str, str]:
        """Build a model-name -> content-hash map at a given commit.

        Only models whose files appear in *changed_files* are included,
        so newly-added models (absent from the base commit) are naturally
        excluded.
        """
        version_map: dict[str, str] = {}
        for file_path in changed_files:
            if not file_path.endswith(".sql"):
                continue
            result = subprocess.run(
                ["git", "show", f"{commit_sha}:{file_path}"],
                cwd=str(repo),
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
            if result.returncode != 0:
                # File did not exist at that commit (new file).
                continue
            sql_content = result.stdout
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
