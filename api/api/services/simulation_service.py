"""Simulation service — delegates to the core engine ImpactAnalyzer.

Loads model records from the database, converts them to lightweight
:class:`ModelDefinition` objects suitable for impact analysis, and
delegates to the stateless :class:`ImpactAnalyzer`.

All operations are read-only — no mutations to plans, models, or the
database.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from core_engine.models.model_definition import (
    ColumnContract,
    Materialization,
    ModelDefinition,
    ModelKind,
    SchemaContractMode,
)
from core_engine.simulation.impact_analyzer import (
    ChangeAction,
    ColumnChange,
    ImpactAnalyzer,
    ImpactReport,
    ModelRemovalReport,
)
from core_engine.state.repository import ModelRepository
from core_engine.state.tables import ModelTable
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


def _table_to_definition(row: ModelTable) -> ModelDefinition:
    """Convert a ``ModelTable`` ORM row to a ``ModelDefinition``.

    The conversion is intentionally lenient: fields that don't exist on
    the table are set to safe defaults so the analyzer can still
    inspect dependencies and contracts.
    """
    tags: list[str] = []
    if row.tags:
        try:
            tags = json.loads(row.tags) if isinstance(row.tags, str) else row.tags
        except (json.JSONDecodeError, TypeError):
            tags = []

    # Dependencies and referenced_tables are stored as JSON strings
    # in model_version snapshots, but may not be available from the
    # model table directly.  Fall back to empty.
    dependencies: list[str] = []
    referenced_tables: list[str] = []
    raw_sql = ""
    clean_sql = ""
    output_columns: list[str] = []
    contract_mode = SchemaContractMode.DISABLED
    contract_columns: list[ColumnContract] = []

    # Try to extract metadata from JSON metadata column if present.
    metadata = getattr(row, "metadata_json", None) or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (json.JSONDecodeError, TypeError):
            metadata = {}

    if isinstance(metadata, dict):
        dependencies = metadata.get("dependencies", [])
        referenced_tables = metadata.get("referenced_tables", [])
        raw_sql = metadata.get("raw_sql", "")
        clean_sql = metadata.get("clean_sql", "")
        output_columns = metadata.get("output_columns", [])
        cm = metadata.get("contract_mode", "DISABLED")
        try:
            contract_mode = SchemaContractMode(cm)
        except ValueError:
            contract_mode = SchemaContractMode.DISABLED
        for cc in metadata.get("contract_columns", []):
            if isinstance(cc, dict):
                contract_columns.append(
                    ColumnContract(
                        name=cc.get("name", ""),
                        data_type=cc.get("data_type", "STRING"),
                        nullable=cc.get("nullable", True),
                    )
                )

    kind = ModelKind.FULL_REFRESH
    try:
        kind = ModelKind(row.kind)
    except (ValueError, KeyError):
        pass

    materialization = Materialization.TABLE
    try:
        materialization = Materialization(row.materialization)
    except (ValueError, KeyError):
        pass

    return ModelDefinition(
        name=row.model_name,
        kind=kind,
        materialization=materialization,
        file_path=row.repo_path or f"models/{row.model_name}.sql",
        raw_sql=raw_sql or "SELECT 1",
        clean_sql=clean_sql,
        time_column=row.time_column,
        unique_key=row.unique_key,
        owner=row.owner,
        tags=tags,
        dependencies=dependencies,
        referenced_tables=referenced_tables,
        output_columns=output_columns,
        contract_mode=contract_mode,
        contract_columns=contract_columns,
    )


class SimulationService:
    """Orchestrate what-if impact simulations.

    Parameters
    ----------
    session:
        Async database session for reading model records.
    tenant_id:
        Tenant scope for data access.
    """

    def __init__(self, session: AsyncSession, tenant_id: str) -> None:
        self._session = session
        self._tenant_id = tenant_id

    async def simulate_column_changes(
        self,
        source_model: str,
        changes: list[dict[str, Any]],
    ) -> ImpactReport:
        """Simulate column changes on a model and return the impact report."""
        models = await self._load_models()
        dag = self._build_adjacency_list(models)
        model_map = {m.name: m for m in models}

        parsed_changes = [
            ColumnChange(
                action=ChangeAction(c["action"]),
                column_name=c["column_name"],
                new_name=c.get("new_name"),
                old_type=c.get("old_type"),
                new_type=c.get("new_type"),
            )
            for c in changes
        ]

        analyzer = ImpactAnalyzer(models=model_map, dag=dag)
        return analyzer.simulate_column_change(source_model, parsed_changes)

    async def simulate_model_removal(
        self,
        model_name: str,
    ) -> ModelRemovalReport:
        """Simulate removing a model and return the impact report."""
        models = await self._load_models()
        dag = self._build_adjacency_list(models)
        model_map = {m.name: m for m in models}

        analyzer = ImpactAnalyzer(models=model_map, dag=dag)
        return analyzer.simulate_model_removal(model_name)

    async def simulate_type_change(
        self,
        source_model: str,
        column_name: str,
        old_type: str,
        new_type: str,
    ) -> ImpactReport:
        """Simulate a column type change and return the impact report."""
        models = await self._load_models()
        dag = self._build_adjacency_list(models)
        model_map = {m.name: m for m in models}

        analyzer = ImpactAnalyzer(models=model_map, dag=dag)
        return analyzer.simulate_type_change(source_model, column_name, old_type, new_type)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _load_models(self) -> list[ModelDefinition]:
        """Load all model records and convert to ModelDefinitions."""
        repo = ModelRepository(self._session, tenant_id=self._tenant_id)
        rows = await repo.list_all()
        return [_table_to_definition(row) for row in rows]

    @staticmethod
    def _build_adjacency_list(
        models: list[ModelDefinition],
    ) -> dict[str, list[str]]:
        """Build an adjacency list (model -> [upstream deps]) from models.

        Uses the same sources as ``build_dag``: referenced_tables and
        explicit dependencies.
        """
        model_names = {m.name for m in models}
        dag: dict[str, list[str]] = {}

        for model in models:
            upstream: list[str] = []
            all_refs = set(model.referenced_tables) | set(model.dependencies)
            for ref in sorted(all_refs):
                if ref in model_names and ref != model.name:
                    upstream.append(ref)
            dag[model.name] = upstream

        return dag
