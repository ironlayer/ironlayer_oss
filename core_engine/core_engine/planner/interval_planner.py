"""Deterministic interval planner for the IronLayer core engine.

Given a set of model definitions, a structural diff result, a dependency DAG,
watermark state, and historical run statistics, this module generates a fully
deterministic execution plan.  *Determinism* is the cardinal invariant: the same
inputs must always produce byte-identical plan JSON.

Key design decisions
--------------------
* **No timestamps** are embedded in the plan.  Callers pass ``as_of_date`` so
  that date arithmetic is reproducible.
* **Step IDs** and **plan IDs** are SHA-256 digests derived from content, not
  random UUIDs.
* **All collections** (affected models, plan steps, dependency lists) are sorted
  alphabetically by model name before being emitted.
* **Conservative defaults**: when in doubt, a model is scheduled for a full
  refresh rather than being skipped.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import date, timedelta

import networkx as nx
from core_engine.sql_toolkit import Dialect, get_sql_toolkit
from pydantic import BaseModel, Field

from core_engine.contracts.schema_validator import ContractValidationResult
from core_engine.graph.dag_builder import topological_sort
from core_engine.models.diff import DiffResult
from core_engine.models.model_definition import ModelDefinition, ModelKind
from core_engine.models.plan import (
    DateRange,
    Plan,
    PlanStep,
    PlanSummary,
    RunType,
)
from core_engine.telemetry.profiling import profile_operation

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


class PlannerConfig(BaseModel):
    """Tunable knobs for the interval planner."""

    default_lookback_days: int = Field(
        default=30,
        ge=1,
        description=("Number of days to look back when no watermark exists for an incremental model."),
    )
    cost_per_compute_second: float = Field(
        default=0.0007,
        ge=0.0,
        description="Estimated USD cost per compute-second (m5d.large rate).",
    )
    skip_cosmetic_changes: bool = Field(
        default=True,
        description=(
            "When True, models whose only changes are cosmetic "
            "(whitespace, formatting, comment-only) are excluded from the plan. "
            "This reduces unnecessary compute costs."
        ),
    )


# ---------------------------------------------------------------------------
# Default configuration singleton
# ---------------------------------------------------------------------------

_DEFAULT_CONFIG = PlannerConfig()

# ---------------------------------------------------------------------------
# Default estimated runtime when no historical data exists
# ---------------------------------------------------------------------------

_DEFAULT_ESTIMATED_SECONDS: float = 300.0  # 5 minutes


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@profile_operation("plan.generate")
def generate_plan(
    models: dict[str, ModelDefinition],
    diff_result: DiffResult,
    dag: nx.DiGraph,
    watermarks: dict[str, tuple[date, date]],
    run_stats: dict[str, dict],
    config: PlannerConfig | None = None,
    *,
    base: str = "",
    target: str = "",
    as_of_date: date | None = None,
    base_sql: dict[str, str] | None = None,
    contract_results: ContractValidationResult | None = None,
) -> Plan:
    """Generate a deterministic execution plan.

    Parameters
    ----------
    models:
        Mapping of ``model_name -> ModelDefinition`` for every known model in
        the target snapshot.
    diff_result:
        Structural diff between base and target snapshots.
    dag:
        Directed acyclic graph of model dependencies (edges point from upstream
        to downstream).
    watermarks:
        Mapping of ``model_name -> (range_start, range_end)`` representing the
        last successfully materialised date range for each incremental model.
    run_stats:
        Mapping of ``model_name -> {"avg_runtime_seconds": float, ...}``
        containing historical execution metrics.
    config:
        Planner configuration.  Falls back to sane defaults when ``None``.
    base:
        Identifier string for the base (previous) snapshot.
    target:
        Identifier string for the target (current) snapshot.
    as_of_date:
        The reference date for all date arithmetic.  Must be provided
        explicitly to ensure deterministic plan generation.
    base_sql:
        Mapping of ``model_name -> old_sql`` for models in the base snapshot.
        When provided together with ``config.skip_cosmetic_changes=True``,
        modified models whose changes are purely cosmetic (whitespace,
        formatting, comment-only) are excluded from the plan.  When ``None``,
        no cosmetic filtering is performed (backward-compatible default).
    contract_results:
        Optional schema contract validation result.  When provided,
        violations are embedded into the corresponding plan steps and
        summarised in the plan summary.

    Returns
    -------
    Plan
        A fully-resolved, deterministic execution plan.
    """
    if config is None:
        config = _DEFAULT_CONFIG
    if as_of_date is None:
        raise ValueError("as_of_date is required for deterministic planning")

    # 1. Collect directly changed models (added + modified). ----------------
    directly_changed: set[str] = set(diff_result.added_models) | set(diff_result.modified_models)

    # 1b. Filter out cosmetic-only changes when enabled. -------------------
    cosmetic_models: set[str] = set()
    if config.skip_cosmetic_changes and base_sql is not None:
        for model_name in sorted(set(diff_result.modified_models)):
            if model_name in base_sql and model_name in models:
                old_sql = base_sql[model_name]
                new_sql = models[model_name].clean_sql
                if _is_cosmetic_change(old_sql, new_sql):
                    cosmetic_models.add(model_name)
                    logger.info(
                        "Skipping %s: cosmetic-only change detected",
                        model_name,
                    )
        directly_changed -= cosmetic_models

    # 2. Compute transitive downstream closure via the DAG. -----------------
    all_affected: set[str] = set(directly_changed)
    for model_name in sorted(directly_changed):
        if dag.has_node(model_name):
            downstream = nx.descendants(dag, model_name)
            all_affected |= downstream

    # Only keep models that actually exist in the target model set.
    all_affected = {m for m in all_affected if m in models}

    # 3. Sort for determinism. ---------------------------------------------
    affected_sorted: list[str] = sorted(all_affected)

    # 4. Assign parallel groups via topological layering. -------------------
    parallel_groups = _assign_parallel_groups(affected_sorted, dag)

    # 5. Build a step for each affected model. -----------------------------
    steps: list[PlanStep] = []
    # Pre-compute step IDs so we can wire up depends_on references.
    step_id_map: dict[str, str] = {name: _compute_step_id(name, base, target) for name in affected_sorted}

    for model_name in affected_sorted:
        model_def = models[model_name]
        run_type, input_range = _determine_run_type(
            model_def=model_def,
            diff_result=diff_result,
            watermarks=watermarks,
            dag=dag,
            all_affected=all_affected,
            config=config,
            as_of_date=as_of_date,
        )
        est_seconds, est_usd = _estimate_cost(model_name, run_stats, config)
        reason = _build_reason(
            model_name=model_name,
            diff_result=diff_result,
            is_direct=(model_name in directly_changed),
            dag=dag,
            directly_changed=directly_changed,
        )

        # Compute depends_on: upstream models within the affected set.
        depends_on_models: list[str] = (
            sorted(pred for pred in dag.predecessors(model_name) if pred in all_affected)
            if dag.has_node(model_name)
            else []
        )
        depends_on_step_ids: list[str] = [step_id_map[dep] for dep in depends_on_models]

        # Embed contract violations for this model if available.
        step_violations: list[dict] = []
        if contract_results is not None:
            model_violations = contract_results.violations_for_model(model_name)
            step_violations = [
                {
                    "column_name": v.column_name,
                    "violation_type": v.violation_type,
                    "severity": v.severity.value,
                    "expected": v.expected,
                    "actual": v.actual,
                    "message": v.message,
                }
                for v in model_violations
            ]

        steps.append(
            PlanStep(
                step_id=step_id_map[model_name],
                model=model_name,
                run_type=run_type,
                input_range=input_range,
                depends_on=depends_on_step_ids,
                parallel_group=parallel_groups.get(model_name, 0),
                reason=reason,
                estimated_compute_seconds=est_seconds,
                estimated_cost_usd=est_usd,
                contract_violations=step_violations,
            )
        )

    # 6. Compute plan summary. ---------------------------------------------
    total_cost = sum(s.estimated_cost_usd for s in steps)
    total_violations = sum(len(s.contract_violations) for s in steps)
    breaking_violations = sum(1 for s in steps for v in s.contract_violations if v.get("severity") == "BREAKING")
    summary = PlanSummary(
        total_steps=len(steps),
        estimated_cost_usd=round(total_cost, 6),
        models_changed=affected_sorted,
        cosmetic_changes_skipped=sorted(cosmetic_models),
        contract_violations_count=total_violations,
        breaking_contract_violations=breaking_violations,
    )

    # 7. Compute deterministic plan_id. ------------------------------------
    plan_id = _compute_plan_id(base, target, steps)

    return Plan(
        plan_id=plan_id,
        base=base,
        target=target,
        summary=summary,
        steps=steps,
    )


# ---------------------------------------------------------------------------
# Run type determination
# ---------------------------------------------------------------------------


def _determine_run_type(
    *,
    model_def: ModelDefinition,
    diff_result: DiffResult,
    watermarks: dict[str, tuple[date, date]],
    dag: nx.DiGraph,
    all_affected: set[str],
    config: PlannerConfig,
    as_of_date: date,
) -> tuple[RunType, DateRange | None]:
    """Decide the run type and optional input range for a single model.

    Returns
    -------
    tuple[RunType, DateRange | None]
        The run type and, for incremental runs, the date range to process.
    """
    name = model_def.name
    kind = model_def.kind

    # Newly added models always get a full refresh regardless of kind.
    if name in diff_result.added_models:
        return RunType.FULL_REFRESH, None

    # FULL_REFRESH and MERGE_BY_KEY models always do a full refresh.
    if kind in (ModelKind.FULL_REFRESH, ModelKind.MERGE_BY_KEY):
        return RunType.FULL_REFRESH, None

    # INCREMENTAL_BY_TIME_RANGE and APPEND_ONLY models get incremental runs
    # with a computed date range.
    if kind in (ModelKind.INCREMENTAL_BY_TIME_RANGE, ModelKind.APPEND_ONLY):
        upstream_affected = (
            {pred for pred in dag.predecessors(name) if pred in all_affected} if dag.has_node(name) else set()
        )
        input_range = _compute_incremental_range(
            model=model_def,
            watermarks=watermarks,
            upstream_models=upstream_affected,
            config=config,
            as_of_date=as_of_date,
        )
        return RunType.INCREMENTAL, input_range

    # Defensive fallback for any future model kind.
    return RunType.FULL_REFRESH, None


# ---------------------------------------------------------------------------
# Incremental range computation
# ---------------------------------------------------------------------------


def _compute_incremental_range(
    model: ModelDefinition,
    watermarks: dict[str, tuple[date, date]],
    upstream_models: set[str],
    config: PlannerConfig,
    as_of_date: date,
) -> DateRange:
    """Compute the input date range for an incremental model run.

    Logic
    -----
    1. If a watermark exists for this model, start from the watermark end date
       (the day *after* the last successfully processed date).
    2. If no watermark exists, fall back to ``as_of_date - default_lookback_days``.
    3. The end date is always ``as_of_date``.
    4. If any upstream model in the affected set has a watermark whose start is
       *earlier*, widen this model's start to cover the upstream gap.  This
       ensures that if an upstream was re-processed from an earlier date, the
       downstream sees the full affected window.

    Parameters
    ----------
    model:
        The model definition (used for its name).
    watermarks:
        Global watermark state.
    upstream_models:
        Set of upstream model names that are also in the affected set.
    config:
        Planner configuration for lookback defaults.
    as_of_date:
        Reference date for all date arithmetic.

    Returns
    -------
    DateRange
        An inclusive ``[start, end]`` date range.
    """
    # Determine start date from this model's own watermark.
    if model.name in watermarks:
        _wm_start, wm_end = watermarks[model.name]
        start = wm_end  # resume from where we left off
    else:
        start = as_of_date - timedelta(days=config.default_lookback_days)

    # Widen to cover any upstream model whose watermark starts earlier.
    for upstream_name in sorted(upstream_models):
        if upstream_name in watermarks:
            upstream_start, _upstream_end = watermarks[upstream_name]
            if upstream_start < start:
                start = upstream_start

    end = as_of_date

    # Guard: start must not exceed end.
    if start > end:
        start = end

    return DateRange(start=start, end=end)


# ---------------------------------------------------------------------------
# Parallel group assignment
# ---------------------------------------------------------------------------


def _assign_parallel_groups(
    affected_models: list[str],
    dag: nx.DiGraph,
) -> dict[str, int]:
    """Assign each affected model to a parallel execution group.

    Models are layered by longest-path distance from source nodes in the
    subgraph induced by the affected set.  Models in the same layer have no
    mutual dependency and may execute concurrently.

    Returns
    -------
    dict[str, int]
        Mapping of ``model_name -> group_index`` (0-based).
    """
    if not affected_models:
        return {}

    affected_set = set(affected_models)
    subgraph = dag.subgraph(affected_set & set(dag.nodes))

    # If the subgraph is empty (no edges among affected nodes) or a model is
    # not even present in the DAG, everything lands in group 0.
    if subgraph.number_of_nodes() == 0:
        return {m: 0 for m in affected_models}

    groups: dict[str, int] = {}

    # Topological layering via longest path from any source.
    try:
        topo_order = topological_sort(subgraph)
    except Exception:
        # Cycle detected -- defensive fallback to sequential execution.
        logger.warning("Cycle detected in affected subgraph; assigning sequential groups.")
        return {m: idx for idx, m in enumerate(affected_models)}

    longest_path: dict[str, int] = {}
    for node in topo_order:
        preds_in_sub = [p for p in subgraph.predecessors(node) if p in longest_path]
        if preds_in_sub:
            longest_path[node] = max(longest_path[p] for p in preds_in_sub) + 1
        else:
            longest_path[node] = 0

    # Any affected model not in the subgraph (isolated node) gets group 0.
    for model_name in affected_models:
        groups[model_name] = longest_path.get(model_name, 0)

    return groups


# ---------------------------------------------------------------------------
# Cost estimation
# ---------------------------------------------------------------------------


def _estimate_cost(
    model_name: str,
    run_stats: dict[str, dict],
    config: PlannerConfig,
) -> tuple[float, float]:
    """Estimate compute time and USD cost for a single model step.

    Parameters
    ----------
    model_name:
        Canonical model name.
    run_stats:
        Historical run statistics keyed by model name.  Each entry is expected
        to have at least an ``"avg_runtime_seconds"`` key.
    config:
        Planner configuration containing the cost rate.

    Returns
    -------
    tuple[float, float]
        ``(estimated_seconds, estimated_usd)``
    """
    stats = run_stats.get(model_name)
    if stats and "avg_runtime_seconds" in stats:
        seconds = float(stats["avg_runtime_seconds"])
    else:
        seconds = _DEFAULT_ESTIMATED_SECONDS

    usd = round(seconds * config.cost_per_compute_second, 6)
    return seconds, usd


# ---------------------------------------------------------------------------
# Reason generation
# ---------------------------------------------------------------------------


def _build_reason(
    *,
    model_name: str,
    diff_result: DiffResult,
    is_direct: bool,
    dag: nx.DiGraph,
    directly_changed: set[str],
) -> str:
    """Produce a human-readable reason string for why a model is in the plan.

    Parameters
    ----------
    model_name:
        The model being explained.
    diff_result:
        Structural diff result.
    is_direct:
        Whether the model itself was added or modified (as opposed to being
        a downstream dependency).
    dag:
        Full dependency DAG.
    directly_changed:
        Set of directly changed model names.

    Returns
    -------
    str
        A short reason string.
    """
    if model_name in diff_result.added_models:
        return "new model added"

    if model_name in diff_result.modified_models:
        return "SQL logic changed"

    # Downstream model -- find the *closest* changed upstream for context.
    if not is_direct and dag.has_node(model_name):
        changed_upstreams = sorted(pred for pred in dag.predecessors(model_name) if pred in directly_changed)
        if changed_upstreams:
            return f"downstream of {changed_upstreams[0]}"

        # The model may be multiple hops downstream.  Walk ancestors in the
        # affected set to find the root cause.
        try:
            ancestors = nx.ancestors(dag, model_name)
            root_causes = sorted(ancestors & directly_changed)
            if root_causes:
                return f"downstream of {root_causes[0]}"
        except nx.NetworkXError:
            pass

    return "included by planner policy"


# ---------------------------------------------------------------------------
# Cosmetic change detection
# ---------------------------------------------------------------------------


def _is_cosmetic_change(old_sql: str, new_sql: str) -> bool:
    """Determine if the difference between two SQL strings is purely cosmetic.

    Cosmetic changes include whitespace differences, comment changes,
    and formatting differences that produce identical canonical SQL
    when parsed and regenerated by the SQL toolkit.

    Returns True only when confident the change is cosmetic; defaults
    to False (conservative) on any parse error.
    """
    tk = get_sql_toolkit()
    try:
        result = tk.differ.diff(old_sql, new_sql, Dialect.DATABRICKS)
        return result.is_cosmetic_only or result.is_identical
    except Exception:  # noqa: BLE001
        # Parse error -> conservative: assume it's NOT cosmetic.
        return False


# ---------------------------------------------------------------------------
# Deterministic ID computation
# ---------------------------------------------------------------------------


def _compute_step_id(model_name: str, base: str, target: str) -> str:
    """Derive a deterministic step ID from the model name and snapshot IDs.

    The ID is a SHA-256 hex digest of ``"{model_name}:{base}:{target}"``.
    """
    payload = f"{model_name}:{base}:{target}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _compute_plan_id(base: str, target: str, steps: list[PlanStep]) -> str:
    """Derive a deterministic plan ID from the base, target, and step content.

    The plan ID is the SHA-256 digest of the concatenation of:
    ``base``, ``target``, and every step's ``step_id`` (in list order, which is
    already deterministically sorted by model name).
    """
    hasher = hashlib.sha256()
    hasher.update(base.encode("utf-8"))
    hasher.update(target.encode("utf-8"))
    for step in steps:
        hasher.update(step.step_id.encode("utf-8"))
    return hasher.hexdigest()
