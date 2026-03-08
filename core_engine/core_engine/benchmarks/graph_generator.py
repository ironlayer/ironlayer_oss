"""Synthetic DAG generators for performance benchmarking.

All generators are deterministic: same inputs always produce identical
``ModelDefinition`` lists.  Models are named with zero-padded indices
(``m_000``, ``m_001``, ...) and carry syntactically valid SQL that
references upstream model names as table references.

Four graph topologies are provided, each stressing a different axis:

* **linear_chain** — maximum depth, minimum breadth.
* **wide_fanout** — maximum breadth, minimum depth.
* **diamond** — converging + diverging layers (many-to-many).
* **realistic** — random DAG with configurable density and mixed model kinds.
"""

from __future__ import annotations

import hashlib
import logging
import math
import random as _random_mod

from core_engine.models.model_definition import (
    Materialization,
    ModelDefinition,
    ModelKind,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQL templates
# ---------------------------------------------------------------------------

_SIMPLE_SELECT = "SELECT {columns} FROM {source}"
_JOIN_SELECT = "SELECT {columns} FROM {source_a} a JOIN {source_b} b ON a.id = b.id"
_CTE_SELECT = "WITH base AS (\n  SELECT {columns} FROM {source}\n)\nSELECT {columns} FROM base"
_WINDOW_SELECT = "SELECT {columns}, ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at) AS rn FROM {source}"
_AGG_SELECT = "SELECT id, SUM(amount) AS total_amount, COUNT(*) AS cnt FROM {source} GROUP BY id"

_DEFAULT_COLUMNS = "id, name, created_at, amount"
_DEFAULT_OUTPUT_COLS = ["id", "name", "created_at", "amount"]


def _model_name(idx: int) -> str:
    """Zero-padded model name."""
    return f"m_{idx:04d}"


def _make_model(
    name: str,
    kind: ModelKind = ModelKind.FULL_REFRESH,
    dependencies: list[str] | None = None,
    sql: str = "",
    time_column: str | None = None,
    unique_key: str | None = None,
    output_columns: list[str] | None = None,
) -> ModelDefinition:
    """Create a ModelDefinition with all required fields populated."""
    deps = dependencies or []
    raw_sql = sql or f"SELECT {_DEFAULT_COLUMNS} FROM source_table"
    content_hash = hashlib.sha256(raw_sql.encode()).hexdigest()[:16]

    return ModelDefinition(
        name=name,
        kind=kind,
        materialization=Materialization.TABLE,
        time_column=time_column,
        unique_key=unique_key,
        dependencies=deps,
        file_path=f"models/{name}.sql",
        raw_sql=raw_sql,
        clean_sql=raw_sql,
        content_hash=content_hash,
        referenced_tables=deps,
        output_columns=output_columns or list(_DEFAULT_OUTPUT_COLS),
    )


class SyntheticGraphGenerator:
    """Generate synthetic model graphs for benchmarking.

    All methods are static and deterministic.
    """

    @staticmethod
    def generate_linear_chain(n: int) -> list[ModelDefinition]:
        """Generate a linear chain of *n* models.

        Each model depends on exactly one predecessor, forming the worst
        case for DAG depth (depth = n - 1).

        Parameters
        ----------
        n:
            Number of models to generate (must be >= 1).
        """
        if n < 1:
            raise ValueError(f"n must be >= 1, got {n}")

        models: list[ModelDefinition] = []

        for i in range(n):
            name = _model_name(i)
            if i == 0:
                sql = _SIMPLE_SELECT.format(
                    columns=_DEFAULT_COLUMNS,
                    source="raw_data.source_table",
                )
                deps: list[str] = []
            else:
                upstream = _model_name(i - 1)
                sql = _CTE_SELECT.format(
                    columns=_DEFAULT_COLUMNS,
                    source=upstream,
                )
                deps = [upstream]

            models.append(_make_model(name=name, dependencies=deps, sql=sql))

        return models

    @staticmethod
    def generate_wide_fanout(
        n: int,
        fanout: int = 10,
    ) -> list[ModelDefinition]:
        """Generate a wide fanout graph.

        One root model fans out to *fanout* children, each of which fans
        out to *fanout* grandchildren.  Total models = min(n, 1 + fanout + fanout^2).
        This is the worst case for DAG breadth.

        Parameters
        ----------
        n:
            Maximum number of models.
        fanout:
            Number of children per parent node.
        """
        if n < 1:
            raise ValueError(f"n must be >= 1, got {n}")

        models: list[ModelDefinition] = []
        idx = 0

        # Root.
        root_name = _model_name(idx)
        models.append(
            _make_model(
                name=root_name,
                sql=_AGG_SELECT.format(source="raw_data.events"),
                output_columns=["id", "total_amount", "cnt"],
            )
        )
        idx += 1

        if idx >= n:
            return models

        # Children of root.
        child_names: list[str] = []
        for _ in range(fanout):
            if idx >= n:
                break
            name = _model_name(idx)
            sql = _SIMPLE_SELECT.format(
                columns=_DEFAULT_COLUMNS,
                source=root_name,
            )
            models.append(_make_model(name=name, dependencies=[root_name], sql=sql))
            child_names.append(name)
            idx += 1

        # Grandchildren: each child fans out.
        for child_name in child_names:
            for _ in range(fanout):
                if idx >= n:
                    break
                name = _model_name(idx)
                sql = _WINDOW_SELECT.format(
                    columns=_DEFAULT_COLUMNS,
                    source=child_name,
                )
                models.append(
                    _make_model(
                        name=name,
                        dependencies=[child_name],
                        sql=sql,
                        output_columns=_DEFAULT_OUTPUT_COLS + ["rn"],
                    )
                )
                idx += 1

        return models

    @staticmethod
    def generate_diamond(n: int) -> list[ModelDefinition]:
        """Generate a diamond / lattice graph with converging and diverging layers.

        Models are arranged in layers where each model in layer *k*
        depends on all models in layer *k-1*.  Layer sizes follow a
        pattern: 1, 2, 4, ..., peak, ..., 4, 2, 1 to create a diamond
        shape.  This tests many-to-many dependency resolution.

        Parameters
        ----------
        n:
            Approximate number of models (actual may be slightly different
            to form a symmetric diamond).
        """
        if n < 1:
            raise ValueError(f"n must be >= 1, got {n}")

        # Build layer sizes for a diamond shape.
        layer_sizes: list[int] = []
        remaining = n
        width = 1
        expanding = True

        while remaining > 0:
            actual = min(width, remaining)
            layer_sizes.append(actual)
            remaining -= actual
            if expanding:
                width *= 2
                if width > max(4, int(math.sqrt(n))):
                    expanding = False
            else:
                width = max(1, width // 2)

        models: list[ModelDefinition] = []
        idx = 0
        prev_layer_names: list[str] = []

        for _layer_idx, size in enumerate(layer_sizes):
            current_layer_names: list[str] = []
            for _ in range(size):
                name = _model_name(idx)
                if not prev_layer_names:
                    sql = _SIMPLE_SELECT.format(
                        columns=_DEFAULT_COLUMNS,
                        source="raw_data.source_table",
                    )
                    deps: list[str] = []
                elif len(prev_layer_names) == 1:
                    sql = _CTE_SELECT.format(
                        columns=_DEFAULT_COLUMNS,
                        source=prev_layer_names[0],
                    )
                    deps = list(prev_layer_names)
                else:
                    # Depend on all models in the previous layer.
                    primary = prev_layer_names[0]
                    secondary = prev_layer_names[1] if len(prev_layer_names) > 1 else prev_layer_names[0]
                    sql = _JOIN_SELECT.format(
                        columns=_DEFAULT_COLUMNS,
                        source_a=primary,
                        source_b=secondary,
                    )
                    deps = list(prev_layer_names)

                models.append(_make_model(name=name, dependencies=deps, sql=sql))
                current_layer_names.append(name)
                idx += 1

            prev_layer_names = current_layer_names

        return models

    @staticmethod
    def generate_realistic(
        n: int,
        avg_deps: float = 2.0,
        seed: int = 42,
    ) -> list[ModelDefinition]:
        """Generate a random DAG with realistic properties.

        Uses a fixed seed for reproducibility.  Models have mixed
        ``ModelKind`` values and realistic SQL templates with CTEs,
        JOINs, window functions, and aggregations.

        Parameters
        ----------
        n:
            Number of models.
        avg_deps:
            Average number of upstream dependencies per model.
        seed:
            Random seed for reproducibility.
        """
        if n < 1:
            raise ValueError(f"n must be >= 1, got {n}")

        rng = _random_mod.Random(seed)

        # Pre-generate model names.
        names = [_model_name(i) for i in range(n)]

        # Available model kinds with their requirements.
        kind_configs = [
            (ModelKind.FULL_REFRESH, None, None),
            (ModelKind.INCREMENTAL_BY_TIME_RANGE, "created_at", None),
            (ModelKind.APPEND_ONLY, None, None),
            (ModelKind.MERGE_BY_KEY, None, "id"),
        ]

        # SQL templates indexed by complexity.
        sql_templates = [
            _SIMPLE_SELECT,
            _CTE_SELECT,
            _WINDOW_SELECT,
            _AGG_SELECT,
        ]

        column_sets = [
            ("id, name, created_at, amount", ["id", "name", "created_at", "amount"]),
            ("user_id, email, signup_date, total_orders", ["user_id", "email", "signup_date", "total_orders"]),
            ("product_id, category, price, stock_count", ["product_id", "category", "price", "stock_count"]),
            ("region, store_id, revenue, txn_date", ["region", "store_id", "revenue", "txn_date"]),
            ("event_id, event_type, ts, user_id", ["event_id", "event_type", "ts", "user_id"]),
        ]

        models: list[ModelDefinition] = []

        for i in range(n):
            name = names[i]

            # Determine upstream dependencies (only from earlier models).
            if i == 0:
                deps: list[str] = []
            else:
                max_deps = min(i, max(1, int(avg_deps * 1.5)))
                num_deps = max(1, min(rng.randint(1, max(1, int(avg_deps + 0.5))), max_deps))
                # Pick from recent models with bias toward closer ones.
                candidates = names[max(0, i - min(20, i)) : i]
                deps = sorted(rng.sample(candidates, min(num_deps, len(candidates))))

            # Pick model kind.
            kind_cfg = rng.choice(kind_configs)
            kind, time_col, unique_col = kind_cfg

            # Pick columns.
            cols_str, cols_list = rng.choice(column_sets)

            # Pick SQL template.
            if not deps:
                sql = _SIMPLE_SELECT.format(
                    columns=cols_str,
                    source="raw_data.source_table",
                )
            elif len(deps) >= 2:
                template = rng.choice([_JOIN_SELECT, _CTE_SELECT])
                if template == _JOIN_SELECT:
                    sql = _JOIN_SELECT.format(
                        columns=cols_str,
                        source_a=deps[0],
                        source_b=deps[1],
                    )
                else:
                    sql = _CTE_SELECT.format(columns=cols_str, source=deps[0])
            else:
                template = rng.choice(sql_templates)
                if template == _JOIN_SELECT and len(deps) >= 2:
                    sql = template.format(
                        columns=cols_str,
                        source_a=deps[0],
                        source_b=deps[1],
                    )
                elif template in (_AGG_SELECT, _WINDOW_SELECT):
                    sql = template.format(
                        columns=cols_str,
                        source=deps[0],
                    )
                else:
                    sql = template.format(columns=cols_str, source=deps[0])

            output_cols = list(cols_list)
            if template == _AGG_SELECT:
                output_cols = ["id", "total_amount", "cnt"]
            elif template == _WINDOW_SELECT:
                output_cols = cols_list + ["rn"]

            models.append(
                _make_model(
                    name=name,
                    kind=kind,
                    dependencies=deps,
                    sql=sql,
                    time_column=time_col,
                    unique_key=unique_col,
                    output_columns=output_cols,
                )
            )

        logger.info(
            "Generated realistic graph: %d models, avg_deps=%.1f, seed=%d",
            n,
            avg_deps,
            seed,
        )
        return models
