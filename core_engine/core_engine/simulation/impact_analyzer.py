"""What-if impact simulation for IronLayer models.

Simulates three classes of hypothetical change against the model dependency
DAG **without** executing anything:

1. **Column change** — add, remove, rename, or change type of a column.
2. **Model removal** — remove a model entirely.
3. **Type change** — change a column's data type and assess compatibility.

All analysis is read-only.  No mutations to plans, models, or the database.
Traversals are deterministic (sorted at every step).
"""

from __future__ import annotations

import logging
from collections import deque
from enum import Enum

from pydantic import BaseModel, Field

from core_engine.sql_toolkit import Dialect, get_sql_toolkit

from core_engine.models.model_definition import (
    ColumnContract,
    ModelDefinition,
    SchemaContractMode,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Enums and models
# ---------------------------------------------------------------------------


class ChangeAction(str, Enum):
    """Type of column change being simulated."""

    ADD = "ADD"
    REMOVE = "REMOVE"
    RENAME = "RENAME"
    TYPE_CHANGE = "TYPE_CHANGE"


class ReferenceSeverity(str, Enum):
    """How severely a downstream model is affected."""

    BREAKING = "BREAKING"
    WARNING = "WARNING"
    INFO = "INFO"


class ColumnChange(BaseModel):
    """A single hypothetical column change."""

    action: ChangeAction = Field(..., description="The type of column change.")
    column_name: str = Field(..., description="Column being changed.")
    new_name: str | None = Field(default=None, description="New column name (for RENAME).")
    old_type: str | None = Field(default=None, description="Previous data type.")
    new_type: str | None = Field(default=None, description="New data type.")


class ContractViolation(BaseModel):
    """A contract violation produced by the simulation."""

    model_name: str
    column_name: str
    violation_type: str
    severity: ReferenceSeverity
    message: str


class AffectedModel(BaseModel):
    """A model affected by a simulated change."""

    model_name: str = Field(..., description="Affected model name.")
    reference_type: str = Field(..., description="'direct' or 'transitive'.")
    columns_affected: list[str] = Field(
        default_factory=list,
        description="Columns in this model that reference the changed column(s).",
    )
    contract_violations: list[ContractViolation] = Field(
        default_factory=list,
        description="Contract violations triggered in this model.",
    )
    severity: ReferenceSeverity = Field(
        default=ReferenceSeverity.INFO,
        description="Worst-case severity for this model.",
    )


class ImpactReport(BaseModel):
    """Complete impact report for a simulated change."""

    source_model: str = Field(..., description="Model where the change originates.")
    column_changes: list[ColumnChange] = Field(
        default_factory=list,
        description="Changes being simulated.",
    )
    directly_affected: list[AffectedModel] = Field(
        default_factory=list,
        description="Models that directly reference the source model.",
    )
    transitively_affected: list[AffectedModel] = Field(
        default_factory=list,
        description="Models affected through transitive dependency.",
    )
    contract_violations: list[ContractViolation] = Field(
        default_factory=list,
        description="All contract violations across all affected models.",
    )
    breaking_count: int = Field(default=0, description="Number of models with BREAKING severity.")
    warning_count: int = Field(default=0, description="Number of models with WARNING severity.")
    orphaned_models: list[str] = Field(
        default_factory=list,
        description="Models that would be orphaned (no remaining upstream).",
    )
    summary: str = Field(default="", description="Human-readable impact summary.")


class ModelRemovalReport(BaseModel):
    """Impact report for a model removal simulation."""

    removed_model: str = Field(..., description="Model being removed.")
    directly_affected: list[AffectedModel] = Field(default_factory=list)
    transitively_affected: list[AffectedModel] = Field(default_factory=list)
    orphaned_models: list[str] = Field(
        default_factory=list,
        description="Models whose only upstream dependency was the removed model.",
    )
    breaking_count: int = 0
    summary: str = ""


# ---------------------------------------------------------------------------
# Type compatibility matrix
# ---------------------------------------------------------------------------

# Maps (old_type, new_type) -> is_safe.  If not in the matrix,
# defaults to BREAKING (conservative).
_TYPE_COMPAT: dict[tuple[str, str], bool] = {
    # Widening integer types — safe
    ("INT", "BIGINT"): True,
    ("INT", "FLOAT"): True,
    ("INT", "DOUBLE"): True,
    ("BIGINT", "DOUBLE"): True,
    ("FLOAT", "DOUBLE"): True,
    ("SMALLINT", "INT"): True,
    ("SMALLINT", "BIGINT"): True,
    ("TINYINT", "INT"): True,
    ("TINYINT", "BIGINT"): True,
    # String widening — safe
    ("VARCHAR", "STRING"): True,
    ("CHAR", "STRING"): True,
    ("CHAR", "VARCHAR"): True,
    # Date to timestamp — safe (no data loss)
    ("DATE", "TIMESTAMP"): True,
    # Same type — safe
    ("STRING", "STRING"): True,
    ("INT", "INT"): True,
    ("BIGINT", "BIGINT"): True,
    ("FLOAT", "FLOAT"): True,
    ("DOUBLE", "DOUBLE"): True,
    ("BOOLEAN", "BOOLEAN"): True,
    ("DATE", "DATE"): True,
    ("TIMESTAMP", "TIMESTAMP"): True,
    ("DECIMAL", "DECIMAL"): True,
    # Narrowing / lossy — breaking
    ("BIGINT", "INT"): False,
    ("DOUBLE", "FLOAT"): False,
    ("DOUBLE", "INT"): False,
    ("FLOAT", "INT"): False,
    ("STRING", "INT"): False,
    ("STRING", "BOOLEAN"): False,
    ("TIMESTAMP", "DATE"): False,
    ("STRING", "DATE"): False,
    ("INT", "BOOLEAN"): False,
    ("INT", "STRING"): False,
}


def _is_type_compatible(old_type: str, new_type: str) -> bool:
    """Check if a type change is safe (non-breaking).

    Uses the compatibility matrix; unknown pairs are conservatively
    treated as breaking.
    """
    old_norm = old_type.upper().strip()
    new_norm = new_type.upper().strip()
    if old_norm == new_norm:
        return True
    return _TYPE_COMPAT.get((old_norm, new_norm), False)


# ---------------------------------------------------------------------------
# Column extraction from SQL
# ---------------------------------------------------------------------------


def _extract_referenced_columns(sql: str) -> set[str]:
    """Extract column names referenced in a SQL statement via the SQL toolkit.

    Falls back to an empty set if parsing fails.
    """
    tk = get_sql_toolkit()
    try:
        result = tk.scope_analyzer.extract_columns(sql, Dialect.DATABRICKS)
        return {ref.name.lower() for ref in result.referenced_columns if ref.name}
    except Exception:
        logger.debug("SQL parse failed during column extraction; returning empty set")
        return set()


# ---------------------------------------------------------------------------
# Impact analyzer
# ---------------------------------------------------------------------------


_DEFAULT_MAX_DEPTH: int = 100


class ImpactAnalyzer:
    """Analyse the impact of hypothetical changes on the model DAG.

    Fully deterministic and stateless -- all traversals are sorted.

    Parameters
    ----------
    models:
        Mapping of model_name -> ModelDefinition for all known models.
    dag:
        Adjacency list mapping model_name -> [upstream_dep, ...].
    max_depth:
        Maximum traversal depth for graph walks.  Raises ``ValueError``
        if the traversal exceeds this depth, which indicates either a
        cyclic graph or an unexpectedly deep dependency chain.
    """

    def __init__(
        self,
        models: dict[str, ModelDefinition],
        dag: dict[str, list[str]],
        *,
        max_depth: int = _DEFAULT_MAX_DEPTH,
    ) -> None:
        self._models = models
        self._dag = dag
        self._max_depth = max_depth
        self._reverse_dag = self._build_reverse_dag(dag)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def simulate_column_change(
        self,
        source_model: str,
        changes: list[ColumnChange],
    ) -> ImpactReport:
        """Simulate one or more column changes on *source_model*.

        Walks downstream models, checks if their SQL references affected
        columns, and validates contracts where present.
        """
        if source_model not in self._models:
            return ImpactReport(
                source_model=source_model,
                column_changes=changes,
                summary=f"Model '{source_model}' not found.",
            )

        changed_col_names = self._resolve_changed_columns(changes)

        direct: list[AffectedModel] = []
        transitive: list[AffectedModel] = []
        all_violations: list[ContractViolation] = []

        # BFS downstream from source_model.
        visited: set[str] = set()
        # Queue: (model_name, depth)
        queue: deque[tuple[str, int]] = deque()
        for child in sorted(self._reverse_dag.get(source_model, [])):
            queue.append((child, 1))

        while queue:
            model_name, depth = queue.popleft()
            if depth > self._max_depth:
                raise ValueError(
                    f"Impact analysis exceeded max_depth={self._max_depth} "
                    f"at model '{model_name}'. This may indicate a cyclic "
                    f"dependency graph or an unexpectedly deep chain."
                )
            if model_name in visited:
                continue
            visited.add(model_name)

            model_def = self._models.get(model_name)
            if model_def is None:
                continue

            # Check if this model references any of the changed columns.
            referenced = _extract_referenced_columns(model_def.clean_sql or model_def.raw_sql)
            affected_cols = sorted(changed_col_names & referenced)

            # Check contracts.
            violations = self._check_contracts(model_name, model_def, changes)
            all_violations.extend(violations)

            severity = ReferenceSeverity.INFO
            if violations:
                severity = max(
                    (v.severity for v in violations),
                    key=lambda s: ["INFO", "WARNING", "BREAKING"].index(s.value),
                )
            elif affected_cols:
                # Column is referenced but no contract -- still a warning.
                severity = self._classify_change_severity(changes)

            affected = AffectedModel(
                model_name=model_name,
                reference_type="direct" if depth == 1 else "transitive",
                columns_affected=affected_cols,
                contract_violations=violations,
                severity=severity,
            )

            if depth == 1:
                direct.append(affected)
            else:
                transitive.append(affected)

            # Continue BFS.
            for child in sorted(self._reverse_dag.get(model_name, [])):
                if child not in visited:
                    queue.append((child, depth + 1))

        breaking_count = sum(1 for a in direct + transitive if a.severity == ReferenceSeverity.BREAKING)
        warning_count = sum(1 for a in direct + transitive if a.severity == ReferenceSeverity.WARNING)

        summary = self._generate_column_summary(
            source_model, changes, direct, transitive, breaking_count, warning_count
        )

        return ImpactReport(
            source_model=source_model,
            column_changes=changes,
            directly_affected=direct,
            transitively_affected=transitive,
            contract_violations=all_violations,
            breaking_count=breaking_count,
            warning_count=warning_count,
            summary=summary,
        )

    def simulate_model_removal(self, model_name: str) -> ModelRemovalReport:
        """Simulate removing *model_name* entirely.

        Identifies all downstream models and detects orphaned ones (models
        whose sole upstream dependency was the removed model).
        """
        if model_name not in self._models:
            return ModelRemovalReport(
                removed_model=model_name,
                summary=f"Model '{model_name}' not found.",
            )

        direct: list[AffectedModel] = []
        transitive: list[AffectedModel] = []
        orphaned: list[str] = []

        visited: set[str] = set()
        queue: deque[tuple[str, int]] = deque()
        for child in sorted(self._reverse_dag.get(model_name, [])):
            queue.append((child, 1))

        while queue:
            current, depth = queue.popleft()
            if depth > self._max_depth:
                raise ValueError(
                    f"Impact analysis exceeded max_depth={self._max_depth} "
                    f"at model '{current}'. This may indicate a cyclic "
                    f"dependency graph or an unexpectedly deep chain."
                )
            if current in visited:
                continue
            visited.add(current)

            # Check if this model would be orphaned.
            upstream = [u for u in self._dag.get(current, []) if u != model_name]
            is_orphaned = len(upstream) == 0

            if is_orphaned:
                orphaned.append(current)

            affected = AffectedModel(
                model_name=current,
                reference_type="direct" if depth == 1 else "transitive",
                severity=ReferenceSeverity.BREAKING,
            )

            if depth == 1:
                direct.append(affected)
            else:
                transitive.append(affected)

            for child in sorted(self._reverse_dag.get(current, [])):
                if child not in visited:
                    queue.append((child, depth + 1))

        orphaned.sort()
        breaking_count = len(direct) + len(transitive)

        parts: list[str] = [
            f"Removing '{model_name}' would affect {len(direct)} direct and {len(transitive)} transitive models."
        ]
        if orphaned:
            parts.append(f"{len(orphaned)} model(s) would be orphaned: {', '.join(orphaned)}.")

        return ModelRemovalReport(
            removed_model=model_name,
            directly_affected=direct,
            transitively_affected=transitive,
            orphaned_models=orphaned,
            breaking_count=breaking_count,
            summary=" ".join(parts),
        )

    def simulate_type_change(
        self,
        source_model: str,
        column_name: str,
        old_type: str,
        new_type: str,
    ) -> ImpactReport:
        """Convenience: simulate a TYPE_CHANGE on a single column."""
        change = ColumnChange(
            action=ChangeAction.TYPE_CHANGE,
            column_name=column_name,
            old_type=old_type,
            new_type=new_type,
        )
        return self.simulate_column_change(source_model, [change])

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_reverse_dag(
        dag: dict[str, list[str]],
    ) -> dict[str, list[str]]:
        """Build parent → children adjacency list."""
        reverse: dict[str, list[str]] = {name: [] for name in dag}
        for child, parents in dag.items():
            for parent in parents:
                if parent not in reverse:
                    reverse[parent] = []
                reverse[parent].append(child)
        return reverse

    @staticmethod
    def _resolve_changed_columns(changes: list[ColumnChange]) -> set[str]:
        """Collect the set of column names affected by the changes."""
        names: set[str] = set()
        for c in changes:
            names.add(c.column_name.lower())
            if c.new_name:
                names.add(c.new_name.lower())
        return names

    @staticmethod
    def _classify_change_severity(changes: list[ColumnChange]) -> ReferenceSeverity:
        """Classify the overall severity based on change actions."""
        for c in changes:
            if c.action in (ChangeAction.REMOVE, ChangeAction.RENAME):
                return ReferenceSeverity.BREAKING
            if c.action == ChangeAction.TYPE_CHANGE:
                if c.old_type and c.new_type:
                    if not _is_type_compatible(c.old_type, c.new_type):
                        return ReferenceSeverity.BREAKING
                    return ReferenceSeverity.WARNING
                return ReferenceSeverity.WARNING
        return ReferenceSeverity.INFO

    @staticmethod
    def _check_contracts(
        model_name: str,
        model_def: ModelDefinition,
        changes: list[ColumnChange],
    ) -> list[ContractViolation]:
        """Check if any column changes violate contracts on *model_def*."""
        if model_def.contract_mode == SchemaContractMode.DISABLED or not model_def.contract_columns:
            return []

        violations: list[ContractViolation] = []
        contract_map: dict[str, ColumnContract] = {cc.name.lower(): cc for cc in model_def.contract_columns}

        for change in changes:
            col_lower = change.column_name.lower()
            contract = contract_map.get(col_lower)
            if not contract:
                continue

            if change.action == ChangeAction.REMOVE:
                violations.append(
                    ContractViolation(
                        model_name=model_name,
                        column_name=change.column_name,
                        violation_type="COLUMN_REMOVED",
                        severity=ReferenceSeverity.BREAKING,
                        message=(
                            f"Contract on '{model_name}' requires column "
                            f"'{change.column_name}' ({contract.data_type}), "
                            f"but it would be removed."
                        ),
                    )
                )

            elif change.action == ChangeAction.RENAME:
                violations.append(
                    ContractViolation(
                        model_name=model_name,
                        column_name=change.column_name,
                        violation_type="COLUMN_RENAMED",
                        severity=ReferenceSeverity.BREAKING,
                        message=(
                            f"Contract on '{model_name}' requires column "
                            f"'{change.column_name}', but it would be renamed "
                            f"to '{change.new_name}'."
                        ),
                    )
                )

            elif (
                change.action == ChangeAction.TYPE_CHANGE
                and change.new_type
                and not _is_type_compatible(contract.data_type, change.new_type)
            ):
                violations.append(
                    ContractViolation(
                        model_name=model_name,
                        column_name=change.column_name,
                        violation_type="TYPE_CHANGED",
                        severity=ReferenceSeverity.BREAKING,
                        message=(
                            f"Contract on '{model_name}' declares "
                            f"'{change.column_name}' as {contract.data_type}, "
                            f"but it would change to {change.new_type}."
                        ),
                    )
                )

        return sorted(violations, key=lambda v: v.column_name)

    @staticmethod
    def _generate_column_summary(
        source_model: str,
        changes: list[ColumnChange],
        direct: list[AffectedModel],
        transitive: list[AffectedModel],
        breaking: int,
        warning: int,
    ) -> str:
        """Build a human-readable impact summary."""
        change_desc = ", ".join(f"{c.action.value} '{c.column_name}'" for c in changes)
        parts = [
            f"Simulating {change_desc} on '{source_model}': "
            f"{len(direct)} direct and {len(transitive)} transitive models affected."
        ]
        if breaking > 0:
            parts.append(f"{breaking} BREAKING impact(s).")
        if warning > 0:
            parts.append(f"{warning} WARNING impact(s).")
        if breaking == 0 and warning == 0 and not direct and not transitive:
            parts.append("No downstream impact detected.")
        return " ".join(parts)
