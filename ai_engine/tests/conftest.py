"""Shared fixtures for AI engine tests.

Provides mock LLMClient instances, reusable SQL snippets, and
Pydantic request/response factories so that individual test modules
stay concise and self-contained.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, PropertyMock

import pytest

from ai_engine.engines.llm_client import LLMClient
from ai_engine.models.requests import (
    CostPredictRequest,
    OptimizeSQLRequest,
    RiskScoreRequest,
    SemanticClassifyRequest,
)

# ------------------------------------------------------------------ #
# LLM mocks
# ------------------------------------------------------------------ #


@pytest.fixture()
def mock_llm_enabled() -> MagicMock:
    """Return a MagicMock that behaves like an *enabled* LLMClient."""
    llm = MagicMock(spec=LLMClient)
    type(llm).enabled = PropertyMock(return_value=True)
    return llm


@pytest.fixture()
def mock_llm_disabled() -> MagicMock:
    """Return a MagicMock that behaves like a *disabled* LLMClient."""
    llm = MagicMock(spec=LLMClient)
    type(llm).enabled = PropertyMock(return_value=False)
    return llm


# ------------------------------------------------------------------ #
# SQL snippets
# ------------------------------------------------------------------ #


@pytest.fixture()
def simple_select_sql() -> str:
    return "SELECT id, name, amount FROM orders"


@pytest.fixture()
def simple_select_with_alias_sql() -> str:
    return "SELECT id, name AS customer_name, amount FROM orders"


@pytest.fixture()
def select_star_sql() -> str:
    return "SELECT * FROM orders"


@pytest.fixture()
def aggregation_sql() -> str:
    return "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id"


@pytest.fixture()
def changed_aggregation_sql() -> str:
    return "SELECT customer_id, AVG(amount) AS total FROM orders GROUP BY customer_id"


@pytest.fixture()
def window_partition_sql() -> str:
    return "SELECT id, amount, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at) AS rn FROM orders"


@pytest.fixture()
def changed_window_partition_sql() -> str:
    return "SELECT id, amount, ROW_NUMBER() OVER (PARTITION BY region ORDER BY created_at) AS rn FROM orders"


@pytest.fixture()
def multi_join_no_where_sql() -> str:
    return (
        "SELECT a.id, b.name, c.amount "
        "FROM orders a "
        "JOIN customers b ON a.customer_id = b.id "
        "JOIN payments c ON a.id = c.order_id"
    )


@pytest.fixture()
def cte_sql() -> str:
    return "WITH active AS (SELECT id, name FROM customers WHERE active = 1) SELECT a.id, a.name FROM active a"


# ------------------------------------------------------------------ #
# Request factories
# ------------------------------------------------------------------ #


@pytest.fixture()
def make_semantic_request():
    """Factory fixture that returns a callable producing SemanticClassifyRequest."""

    def _factory(
        old_sql: str = "SELECT id FROM t",
        new_sql: str = "SELECT id FROM t",
        schema_diff: dict | None = None,
        column_lineage: dict | None = None,
    ) -> SemanticClassifyRequest:
        return SemanticClassifyRequest(
            old_sql=old_sql,
            new_sql=new_sql,
            schema_diff=schema_diff,
            column_lineage=column_lineage,
        )

    return _factory


@pytest.fixture()
def make_cost_request():
    """Factory fixture that returns a callable producing CostPredictRequest."""

    def _factory(
        model_name: str = "catalog.schema.model_a",
        partition_count: int = 10,
        historical_runtime_avg: float | None = None,
        data_volume_bytes: int | None = None,
        cluster_size: str = "medium",
        num_workers: int | None = None,
    ) -> CostPredictRequest:
        return CostPredictRequest(
            model_name=model_name,
            partition_count=partition_count,
            historical_runtime_avg=historical_runtime_avg,
            data_volume_bytes=data_volume_bytes,
            cluster_size=cluster_size,
            num_workers=num_workers,
        )

    return _factory


@pytest.fixture()
def make_risk_request():
    """Factory fixture that returns a callable producing RiskScoreRequest."""

    def _factory(
        model_name: str = "catalog.schema.model_a",
        downstream_depth: int = 0,
        sla_tags: list[str] | None = None,
        dashboard_dependencies: list[str] | None = None,
        model_tags: list[str] | None = None,
        historical_failure_rate: float = 0.0,
    ) -> RiskScoreRequest:
        return RiskScoreRequest(
            model_name=model_name,
            downstream_depth=downstream_depth,
            sla_tags=sla_tags or [],
            dashboard_dependencies=dashboard_dependencies or [],
            model_tags=model_tags or [],
            historical_failure_rate=historical_failure_rate,
        )

    return _factory


@pytest.fixture()
def make_optimize_request():
    """Factory fixture that returns a callable producing OptimizeSQLRequest."""

    def _factory(
        sql: str = "SELECT id FROM orders",
        table_statistics: dict | None = None,
        query_metrics: dict | None = None,
    ) -> OptimizeSQLRequest:
        return OptimizeSQLRequest(
            sql=sql,
            table_statistics=table_statistics,
            query_metrics=query_metrics,
        )

    return _factory
