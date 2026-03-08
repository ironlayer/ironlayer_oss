"""Cross-model column-level lineage.

Traces a column from a target model backward through the DAG to its
ultimate source tables/columns.  Each hop resolves per-model lineage
via :class:`~core_engine.sql_toolkit.SqlLineageAnalyzer` and stitches
source columns of one model to the output columns of its upstream
dependencies.

This module operates entirely on in-memory data structures — no
database or warehouse connection required.  External source tables
(those not managed by IronLayer) terminate the trace naturally: they
appear as the final ``source_table`` entries with no further upstream.
"""

from __future__ import annotations

import logging
from collections import deque
from typing import Any

import networkx as nx

from core_engine.sql_toolkit._types import (
    ColumnLineageNode,
    ColumnLineageResult,
    CrossModelColumnLineage,
    Dialect,
    SqlLineageError,
    SqlToolkitError,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Single-model lineage helper
# ---------------------------------------------------------------------------


def compute_model_column_lineage(
    model_name: str,
    sql: str,
    dialect: Dialect = Dialect.DATABRICKS,
    *,
    schema: dict[str, dict[str, str]] | None = None,
) -> ColumnLineageResult:
    """Compute column-level lineage for a single model.

    This is a convenience wrapper around
    :meth:`SqlLineageAnalyzer.trace_column_lineage` that fills in the
    ``model_name`` field.

    Parameters
    ----------
    model_name:
        Canonical model name (e.g. ``"analytics.orders_daily"``).
    sql:
        The clean SQL for this model (after ref() substitution).
    dialect:
        SQL dialect for parsing.
    schema:
        Optional mapping ``{table_name: {column_name: type}}`` for
        resolving ``SELECT *`` and unqualified columns.

    Returns
    -------
    ColumnLineageResult
        Column lineage with ``model_name`` populated.
    """
    from core_engine.sql_toolkit import get_sql_toolkit

    toolkit = get_sql_toolkit()
    result = toolkit.lineage_analyzer.trace_column_lineage(sql, dialect, schema=schema)
    # Replace the empty model_name with the actual model name.
    return ColumnLineageResult(
        model_name=model_name,
        column_lineage=result.column_lineage,
        unresolved_columns=result.unresolved_columns,
        dialect=result.dialect,
    )


# ---------------------------------------------------------------------------
# Cross-model DAG traversal
# ---------------------------------------------------------------------------


def trace_column_across_dag(
    dag: nx.DiGraph,
    target_model: str,
    target_column: str,
    model_sql_map: dict[str, str],
    dialect: Dialect = Dialect.DATABRICKS,
    *,
    schema: dict[str, dict[str, str]] | None = None,
    max_depth: int = 50,
) -> CrossModelColumnLineage:
    """Trace a column backward through the model DAG to source tables.

    Starting at *target_model*.*target_column*, this function:

    1. Computes per-model column lineage for the target model.
    2. For each source column that references an upstream model managed
       by IronLayer, recursively computes lineage on the upstream model.
    3. Continues until it reaches external source tables or the depth
       limit is hit.

    The result is a flattened sequence of :class:`ColumnLineageNode`
    entries representing the full lineage path from target column to
    all ultimate source columns.

    Parameters
    ----------
    dag:
        The model DAG built by :func:`~core_engine.graph.build_dag`.
    target_model:
        The model whose output column we are tracing.
    target_column:
        The column name to trace.
    model_sql_map:
        Mapping ``{model_name: clean_sql}`` for every model in the DAG.
        Only models with SQL can be analyzed — others are skipped.
    dialect:
        SQL dialect.
    schema:
        Optional schema mapping used to resolve ``SELECT *`` and
        unqualified column references.  Keyed by table name.
    max_depth:
        Safety limit to prevent infinite loops in case of malformed
        data.  Default 50 hops is sufficient for any real pipeline.

    Returns
    -------
    CrossModelColumnLineage
        The complete lineage path.

    Raises
    ------
    SqlLineageError
        If the target model cannot be analyzed.
    """
    from core_engine.sql_toolkit import get_sql_toolkit

    toolkit = get_sql_toolkit()
    all_model_names: set[str] = set(dag.nodes)
    lineage_path: list[ColumnLineageNode] = []
    visited: set[tuple[str, str]] = set()  # (model, column) pairs

    # BFS queue: (model_name, column_name, depth)
    queue: deque[tuple[str, str, int]] = deque()
    queue.append((target_model, target_column, 0))

    while queue:
        model_name, column_name, depth = queue.popleft()

        # Cycle / depth protection.
        visit_key = (model_name, column_name)
        if visit_key in visited:
            continue
        visited.add(visit_key)

        if depth > max_depth:
            logger.warning(
                "Column lineage depth limit (%d) reached at %s.%s",
                max_depth,
                model_name,
                column_name,
            )
            # Skip this path but continue processing other queued items
            # at shallower depths.
            continue

        # If this model is not in the DAG or has no SQL, it's an external
        # source — record it as a terminal node and stop tracing.
        if model_name not in all_model_names or model_name not in model_sql_map:
            lineage_path.append(
                ColumnLineageNode(
                    column=column_name,
                    source_table=model_name,
                    source_column=column_name,
                    transform_type="direct",
                    transform_sql="",
                )
            )
            continue

        sql = model_sql_map[model_name]
        if not sql.strip():
            continue

        # Compute per-model lineage for this column.
        try:
            nodes = toolkit.lineage_analyzer.trace_single_column(column_name, sql, dialect, schema=schema)
        except (SqlLineageError, SqlToolkitError, ValueError):
            logger.debug(
                "Could not trace column '%s' in model '%s', marking as terminal",
                column_name,
                model_name,
                exc_info=True,
            )
            lineage_path.append(
                ColumnLineageNode(
                    column=column_name,
                    source_table=model_name,
                    source_column=column_name,
                    transform_type="direct",
                    transform_sql="",
                )
            )
            continue

        for node in nodes:
            # Record this hop in the lineage path, preserving the
            # actual column name at this hop (not the original target).
            lineage_path.append(
                ColumnLineageNode(
                    column=column_name,
                    source_table=node.source_table,
                    source_column=node.source_column,
                    transform_type=node.transform_type,
                    transform_sql=node.transform_sql,
                )
            )

            # If the source table is an upstream IronLayer model,
            # continue tracing into that model.
            if node.source_table and node.source_table in all_model_names and node.source_table in model_sql_map:
                # Use source_column if available; fall back to the
                # current column_name for the next hop.
                next_column = node.source_column or column_name
                queue.append((node.source_table, next_column, depth + 1))

    return CrossModelColumnLineage(
        target_model=target_model,
        target_column=target_column,
        lineage_path=tuple(lineage_path),
    )


# ---------------------------------------------------------------------------
# Batch lineage computation
# ---------------------------------------------------------------------------


def compute_all_column_lineage(
    dag: nx.DiGraph,
    model_sql_map: dict[str, str],
    dialect: Dialect = Dialect.DATABRICKS,
    *,
    schema: dict[str, dict[str, str]] | None = None,
) -> dict[str, ColumnLineageResult]:
    """Compute column lineage for all models in the DAG.

    Returns a mapping ``{model_name: ColumnLineageResult}``.  Models
    whose SQL cannot be analyzed are omitted silently (errors logged).

    This is useful for building a complete lineage catalog for a
    project.  For single-model or single-column tracing, use
    :func:`compute_model_column_lineage` or
    :func:`trace_column_across_dag` instead.

    Parameters
    ----------
    dag:
        The model DAG.
    model_sql_map:
        ``{model_name: clean_sql}`` for every model.
    dialect:
        SQL dialect.
    schema:
        Optional schema mapping.

    Returns
    -------
    dict[str, ColumnLineageResult]
        Per-model column lineage results.
    """
    results: dict[str, ColumnLineageResult] = {}

    for model_name in dag.nodes:
        sql = model_sql_map.get(model_name)
        if not sql or not sql.strip():
            continue

        try:
            result = compute_model_column_lineage(model_name, sql, dialect, schema=schema)
            results[model_name] = result
        except (SqlLineageError, SqlToolkitError, ValueError) as exc:
            logger.debug(
                "Skipping column lineage for model '%s': %s",
                model_name,
                exc,
                exc_info=True,
            )

    return results
