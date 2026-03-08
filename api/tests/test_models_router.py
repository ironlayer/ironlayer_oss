"""Tests for api/api/routers/models.py

Covers:
- GET /api/models: list all models, with kind/owner/search filters
- GET /api/models/{name}: get model detail, non-existent returns 404
- GET /api/models/{name}/lineage: returns upstream/downstream graph
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import AsyncClient

# ---------------------------------------------------------------------------
# Helpers to build mock model rows
# ---------------------------------------------------------------------------


def _make_model_row(
    name: str,
    kind: str = "sql",
    materialization: str = "table",
    owner: str | None = "data-team",
    tags: list[str] | None = None,
    version: str = "v1",
) -> MagicMock:
    row = MagicMock()
    row.model_name = name
    row.kind = kind
    row.materialization = materialization
    row.owner = owner
    row.tags = json.dumps(tags) if tags else None
    row.current_version = version
    row.created_at = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    row.last_modified_at = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
    row.repo_path = f"models/{name.replace('.', '/')}.sql"
    row.time_column = "created_at"
    row.unique_key = "id"
    return row


# ---------------------------------------------------------------------------
# GET /api/models
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_models_returns_all(client: AsyncClient) -> None:
    """List models returns all registered models."""
    rows = [
        _make_model_row("staging.orders", kind="sql", owner="data-team"),
        _make_model_row("marts.revenue", kind="sql", owner="analytics"),
    ]

    with patch("api.routers.models.ModelRepository") as MockRepo:
        instance = MockRepo.return_value
        instance.list_all = AsyncMock(return_value=rows)

        resp = await client.get("/api/v1/models")

    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 2
    assert body[0]["model_name"] == "staging.orders"
    assert body[1]["model_name"] == "marts.revenue"
    assert body[0]["kind"] == "sql"
    assert body[1]["owner"] == "analytics"


@pytest.mark.asyncio
async def test_list_models_filter_by_kind(client: AsyncClient) -> None:
    """Filtering by kind returns only matching models (DB-side filtering)."""
    filtered_rows = [
        _make_model_row("ml.features", kind="python"),
    ]

    with patch("api.routers.models.ModelRepository") as MockRepo:
        instance = MockRepo.return_value
        instance.list_filtered = AsyncMock(return_value=filtered_rows)

        resp = await client.get("/api/v1/models?kind=python")

    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    assert body[0]["model_name"] == "ml.features"
    assert body[0]["kind"] == "python"


@pytest.mark.asyncio
async def test_list_models_filter_by_owner(client: AsyncClient) -> None:
    """Filtering by owner returns only matching models (DB-side filtering)."""
    filtered_rows = [
        _make_model_row("marts.revenue", owner="analytics"),
    ]

    with patch("api.routers.models.ModelRepository") as MockRepo:
        instance = MockRepo.return_value
        instance.list_filtered = AsyncMock(return_value=filtered_rows)

        resp = await client.get("/api/v1/models?owner=analytics")

    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    assert body[0]["model_name"] == "marts.revenue"


@pytest.mark.asyncio
async def test_list_models_filter_by_search(client: AsyncClient) -> None:
    """Text search on model name is case-insensitive (DB-side filtering)."""
    filtered_rows = [
        _make_model_row("staging.orders"),
    ]

    with patch("api.routers.models.ModelRepository") as MockRepo:
        instance = MockRepo.return_value
        instance.list_filtered = AsyncMock(return_value=filtered_rows)

        resp = await client.get("/api/v1/models?search=ORDERS")

    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    assert body[0]["model_name"] == "staging.orders"


@pytest.mark.asyncio
async def test_list_models_empty(client: AsyncClient) -> None:
    """List models returns empty list when no models registered."""
    with patch("api.routers.models.ModelRepository") as MockRepo:
        instance = MockRepo.return_value
        instance.list_all = AsyncMock(return_value=[])

        resp = await client.get("/api/v1/models")

    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_list_models_tags_serialization(client: AsyncClient) -> None:
    """Tags are deserialized from JSON string to list."""
    rows = [_make_model_row("staging.orders", tags=["sla", "core"])]

    with patch("api.routers.models.ModelRepository") as MockRepo:
        instance = MockRepo.return_value
        instance.list_all = AsyncMock(return_value=rows)

        resp = await client.get("/api/v1/models")

    assert resp.status_code == 200
    body = resp.json()
    assert body[0]["tags"] == ["sla", "core"]


# ---------------------------------------------------------------------------
# GET /api/models/{model_name}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_model_detail(client: AsyncClient) -> None:
    """Fetching an existing model returns full detail."""
    model_row = _make_model_row(
        "staging.orders",
        tags=["staging", "core"],
    )
    watermark = (date(2024, 1, 1), date(2024, 6, 30))
    stats = {"avg_runtime_seconds": 45.0, "avg_cost_usd": None, "run_count": 10}

    # Mock RunRepository.get_by_plan for the empty-string call in the router
    mock_run_rows: list = []

    with (
        patch("api.routers.models.ModelRepository") as MockModelRepo,
        patch("api.routers.models.WatermarkRepository") as MockWmRepo,
        patch("api.routers.models.RunRepository") as MockRunRepo,
    ):
        MockModelRepo.return_value.get = AsyncMock(return_value=model_row)
        MockWmRepo.return_value.get_watermark = AsyncMock(return_value=watermark)
        MockRunRepo.return_value.get_historical_stats = AsyncMock(return_value=stats)
        MockRunRepo.return_value.get_by_plan = AsyncMock(return_value=mock_run_rows)

        resp = await client.get("/api/v1/models/staging.orders")

    assert resp.status_code == 200
    body = resp.json()
    assert body["model_name"] == "staging.orders"
    assert body["kind"] == "sql"
    assert body["materialization"] == "table"
    assert body["owner"] == "data-team"
    assert body["tags"] == ["staging", "core"]
    assert body["watermark"]["partition_start"] == "2024-01-01"
    assert body["watermark"]["partition_end"] == "2024-06-30"
    assert body["historical_stats"]["run_count"] == 10
    assert body["historical_stats"]["avg_runtime_seconds"] == 45.0


@pytest.mark.asyncio
async def test_get_model_not_found(client: AsyncClient) -> None:
    """Fetching a non-existent model returns 404."""
    with patch("api.routers.models.ModelRepository") as MockModelRepo:
        MockModelRepo.return_value.get = AsyncMock(return_value=None)

        resp = await client.get("/api/v1/models/nonexistent.model")

    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_get_model_no_watermark(client: AsyncClient) -> None:
    """Model with no watermark returns null partition range."""
    model_row = _make_model_row("staging.orders")
    stats = {"avg_runtime_seconds": None, "avg_cost_usd": None, "run_count": 0}

    with (
        patch("api.routers.models.ModelRepository") as MockModelRepo,
        patch("api.routers.models.WatermarkRepository") as MockWmRepo,
        patch("api.routers.models.RunRepository") as MockRunRepo,
    ):
        MockModelRepo.return_value.get = AsyncMock(return_value=model_row)
        MockWmRepo.return_value.get_watermark = AsyncMock(return_value=None)
        MockRunRepo.return_value.get_historical_stats = AsyncMock(return_value=stats)
        MockRunRepo.return_value.get_by_plan = AsyncMock(return_value=[])

        resp = await client.get("/api/v1/models/staging.orders")

    assert resp.status_code == 200
    body = resp.json()
    assert body["watermark"]["partition_start"] is None
    assert body["watermark"]["partition_end"] is None


# ---------------------------------------------------------------------------
# GET /api/models/{model_name}/lineage
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_model_lineage(client: AsyncClient) -> None:
    """Lineage endpoint returns upstream and downstream lists."""
    target_row = _make_model_row("marts.revenue")
    all_rows = [
        _make_model_row("staging.orders"),
        _make_model_row("staging.customers"),
        target_row,
    ]

    with patch("api.routers.models.ModelRepository") as MockModelRepo:
        instance = MockModelRepo.return_value
        instance.get = AsyncMock(return_value=target_row)
        instance.list_all = AsyncMock(return_value=all_rows)

        resp = await client.get("/api/v1/models/marts.revenue/lineage")

    assert resp.status_code == 200
    body = resp.json()
    assert body["model_name"] == "marts.revenue"
    assert isinstance(body["upstream"], list)
    assert isinstance(body["downstream"], list)
    assert "depth" in body


@pytest.mark.asyncio
async def test_get_model_lineage_not_found(client: AsyncClient) -> None:
    """Lineage for a non-existent model returns 404."""
    with patch("api.routers.models.ModelRepository") as MockModelRepo:
        instance = MockModelRepo.return_value
        instance.get = AsyncMock(return_value=None)

        resp = await client.get("/api/v1/models/nonexistent.model/lineage")

    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_get_model_lineage_no_deps(client: AsyncClient) -> None:
    """Model with no dependencies has empty upstream/downstream."""
    target_row = _make_model_row("isolated.model")
    all_rows = [target_row]

    with patch("api.routers.models.ModelRepository") as MockModelRepo:
        instance = MockModelRepo.return_value
        instance.get = AsyncMock(return_value=target_row)
        instance.list_all = AsyncMock(return_value=all_rows)

        resp = await client.get("/api/v1/models/isolated.model/lineage")

    assert resp.status_code == 200
    body = resp.json()
    assert body["upstream"] == []
    assert body["downstream"] == []
    assert body["depth"] == 0
