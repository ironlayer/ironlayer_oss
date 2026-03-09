"""HTTP-level tests for all 7 AI engine routers (BL-014).

Uses httpx.AsyncClient with ASGITransport so the full ASGI stack
(middleware → router → engine) runs for every request, exercising
authentication, validation, and response-schema correctness.

The app lifespan is driven manually so that module-level engine
globals (classifier, predictor, etc.) are properly initialised before
any request is made.  This mirrors what Starlette's TestClient does
internally under the hood.

Environment
-----------
AI_ENGINE_SHARED_SECRET is set to ``test-secret`` before create_app()
is called, so SharedSecretMiddleware validates against that fixed value
in every test.
"""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager

import httpx
import pytest

SECRET = "test-secret"
AUTH_HEADER = {"Authorization": f"Bearer {SECRET}"}


# ---------------------------------------------------------------------------
# Lifespan helper
# ---------------------------------------------------------------------------


@asynccontextmanager
async def _lifespan_client(app, base_url: str = "http://testserver"):
    """Async context manager: drives app lifespan, then yields an AsyncClient.

    Sends the ASGI ``lifespan.startup`` event before yielding so that all
    module-level engine globals are initialised.  Sends ``lifespan.shutdown``
    on exit.
    """
    receive_queue: asyncio.Queue = asyncio.Queue()
    send_queue: asyncio.Queue = asyncio.Queue()

    async def _receive():
        return await receive_queue.get()

    async def _send(message):
        await send_queue.put(message)

    lifespan_task = asyncio.create_task(
        app({"type": "lifespan", "asgi": {"version": "3.0"}}, _receive, _send)
    )

    await receive_queue.put({"type": "lifespan.startup"})
    startup_msg = await send_queue.get()
    assert startup_msg["type"] == "lifespan.startup.complete", f"Unexpected: {startup_msg}"

    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url=base_url,
        ) as client:
            yield client
    finally:
        await receive_queue.put({"type": "lifespan.shutdown"})
        await send_queue.get()
        lifespan_task.cancel()
        try:
            await lifespan_task
        except (asyncio.CancelledError, Exception):
            pass


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def app():
    """Create the FastAPI app with a known shared secret."""
    os.environ["AI_ENGINE_SHARED_SECRET"] = SECRET
    from ai_engine.main import create_app  # noqa: PLC0415

    return create_app()


@pytest.fixture(scope="module")
async def client(app):
    """Module-scoped async HTTP client with lifespan driven."""
    async with _lifespan_client(app) as c:
        yield c


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _auth(extra: dict | None = None) -> dict[str, str]:
    """Return headers dict with a valid Authorization header."""
    headers = dict(AUTH_HEADER)
    if extra:
        headers.update(extra)
    return headers


# ===========================================================================
# 1. POST /semantic_classify
# ===========================================================================


class TestSemanticClassify:
    ENDPOINT = "/semantic_classify"
    VALID_BODY = {
        "old_sql": "SELECT id FROM orders",
        "new_sql": "SELECT id, name FROM orders",
    }

    async def test_happy_path(self, client):
        resp = await client.post(self.ENDPOINT, json=self.VALID_BODY, headers=_auth())
        assert resp.status_code == 200
        data = resp.json()
        assert "change_type" in data
        assert "confidence" in data
        assert "requires_full_rebuild" in data
        assert "impact_scope" in data

    async def test_missing_required_field_returns_422(self, client):
        # new_sql is required
        resp = await client.post(self.ENDPOINT, json={"old_sql": "SELECT 1"}, headers=_auth())
        assert resp.status_code == 422

    async def test_missing_auth_header_returns_401(self, client):
        resp = await client.post(self.ENDPOINT, json=self.VALID_BODY)
        assert resp.status_code == 401

    async def test_wrong_auth_header_returns_401(self, client):
        resp = await client.post(
            self.ENDPOINT,
            json=self.VALID_BODY,
            headers={"Authorization": "Bearer wrong-secret"},
        )
        assert resp.status_code == 401


# ===========================================================================
# 2. POST /predict_cost
# ===========================================================================


class TestPredictCost:
    ENDPOINT = "/predict_cost"
    VALID_BODY = {
        "model_name": "catalog.schema.model_a",
        "partition_count": 10,
        "cluster_size": "medium",
    }

    async def test_happy_path(self, client):
        resp = await client.post(self.ENDPOINT, json=self.VALID_BODY, headers=_auth())
        assert resp.status_code == 200
        data = resp.json()
        assert "estimated_runtime_minutes" in data
        assert "estimated_cost_usd" in data
        assert "confidence" in data

    async def test_missing_required_field_returns_422(self, client):
        # cluster_size is required (pattern-validated)
        resp = await client.post(
            self.ENDPOINT,
            json={"model_name": "m", "partition_count": 1},
            headers=_auth(),
        )
        assert resp.status_code == 422

    async def test_missing_auth_header_returns_401(self, client):
        resp = await client.post(self.ENDPOINT, json=self.VALID_BODY)
        assert resp.status_code == 401

    async def test_wrong_auth_header_returns_401(self, client):
        resp = await client.post(
            self.ENDPOINT,
            json=self.VALID_BODY,
            headers={"Authorization": "Bearer wrong-secret"},
        )
        assert resp.status_code == 401


# ===========================================================================
# 3. POST /risk_score
# ===========================================================================


class TestRiskScore:
    ENDPOINT = "/risk_score"
    VALID_BODY = {
        "model_name": "catalog.schema.model_b",
        "downstream_depth": 3,
        "historical_failure_rate": 0.1,
    }

    async def test_happy_path(self, client):
        resp = await client.post(self.ENDPOINT, json=self.VALID_BODY, headers=_auth())
        assert resp.status_code == 200
        data = resp.json()
        assert "risk_score" in data
        assert "business_critical" in data
        assert "approval_required" in data
        assert "risk_factors" in data

    async def test_missing_required_field_returns_422(self, client):
        # historical_failure_rate is required
        resp = await client.post(
            self.ENDPOINT,
            json={"model_name": "m", "downstream_depth": 0},
            headers=_auth(),
        )
        assert resp.status_code == 422

    async def test_missing_auth_header_returns_401(self, client):
        resp = await client.post(self.ENDPOINT, json=self.VALID_BODY)
        assert resp.status_code == 401

    async def test_wrong_auth_header_returns_401(self, client):
        resp = await client.post(
            self.ENDPOINT,
            json=self.VALID_BODY,
            headers={"Authorization": "Bearer wrong-secret"},
        )
        assert resp.status_code == 401


# ===========================================================================
# 4. POST /optimize_sql
# ===========================================================================


class TestOptimizeSQL:
    ENDPOINT = "/optimize_sql"
    VALID_BODY = {
        "sql": "SELECT * FROM orders WHERE customer_id = 42",
    }

    async def test_happy_path(self, client):
        resp = await client.post(self.ENDPOINT, json=self.VALID_BODY, headers=_auth())
        assert resp.status_code == 200
        data = resp.json()
        assert "suggestions" in data
        assert isinstance(data["suggestions"], list)

    async def test_missing_required_field_returns_422(self, client):
        # sql is required
        resp = await client.post(self.ENDPOINT, json={}, headers=_auth())
        assert resp.status_code == 422

    async def test_missing_auth_header_returns_401(self, client):
        resp = await client.post(self.ENDPOINT, json=self.VALID_BODY)
        assert resp.status_code == 401

    async def test_wrong_auth_header_returns_401(self, client):
        resp = await client.post(
            self.ENDPOINT,
            json=self.VALID_BODY,
            headers={"Authorization": "Bearer wrong-secret"},
        )
        assert resp.status_code == 401


# ===========================================================================
# 5. GET /cache/stats  &  POST /cache/invalidate
# ===========================================================================


class TestCacheRoutes:
    async def test_cache_stats_happy_path(self, client):
        resp = await client.get("/cache/stats", headers=_auth())
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, dict)

    async def test_cache_stats_missing_auth_returns_401(self, client):
        resp = await client.get("/cache/stats")
        assert resp.status_code == 401

    async def test_cache_stats_wrong_auth_returns_401(self, client):
        resp = await client.get(
            "/cache/stats",
            headers={"Authorization": "Bearer wrong"},
        )
        assert resp.status_code == 401

    async def test_cache_invalidate_all_happy_path(self, client):
        resp = await client.post("/cache/invalidate", headers=_auth())
        assert resp.status_code == 200
        data = resp.json()
        assert "removed" in data

    async def test_cache_invalidate_all_missing_auth_returns_401(self, client):
        resp = await client.post("/cache/invalidate")
        assert resp.status_code == 401

    async def test_cache_invalidate_by_type_happy_path(self, client):
        resp = await client.post("/cache/invalidate/semantic_classify", headers=_auth())
        assert resp.status_code == 200
        data = resp.json()
        assert "request_type" in data
        assert "removed" in data

    async def test_cache_invalidate_by_type_missing_auth_returns_401(self, client):
        resp = await client.post("/cache/invalidate/semantic_classify")
        assert resp.status_code == 401


# ===========================================================================
# 6. POST /predict_failure
# ===========================================================================


class TestPredictFailure:
    ENDPOINT = "/predict_failure"
    VALID_BODY = {
        "model_name": "catalog.schema.model_c",
        "total_runs": 100,
        "failed_runs": 5,
    }

    async def test_happy_path(self, client):
        resp = await client.post(self.ENDPOINT, json=self.VALID_BODY, headers=_auth())
        assert resp.status_code == 200
        data = resp.json()
        assert "model_name" in data
        assert "failure_probability" in data
        assert "risk_level" in data
        assert "factors" in data
        assert "suggested_actions" in data

    async def test_missing_required_field_returns_422(self, client):
        # model_name is required
        resp = await client.post(self.ENDPOINT, json={}, headers=_auth())
        assert resp.status_code == 422

    async def test_missing_auth_header_returns_401(self, client):
        resp = await client.post(self.ENDPOINT, json=self.VALID_BODY)
        assert resp.status_code == 401

    async def test_wrong_auth_header_returns_401(self, client):
        resp = await client.post(
            self.ENDPOINT,
            json=self.VALID_BODY,
            headers={"Authorization": "Bearer wrong-secret"},
        )
        assert resp.status_code == 401


# ===========================================================================
# 7. POST /fragility_score
# ===========================================================================


class TestFragilityScore:
    ENDPOINT = "/fragility_score"
    VALID_BODY = {
        "model_name": "model_a",
        "dag": {"model_a": ["model_b"], "model_b": []},
        "failure_predictions": {"model_a": 0.1, "model_b": 0.05},
    }

    async def test_happy_path(self, client):
        resp = await client.post(self.ENDPOINT, json=self.VALID_BODY, headers=_auth())
        assert resp.status_code == 200
        data = resp.json()
        assert "model_name" in data
        assert "own_risk" in data
        assert "upstream_risk" in data
        assert "cascade_risk" in data
        assert "fragility_score" in data
        assert "critical_path" in data
        assert "risk_factors" in data

    async def test_missing_required_field_returns_422(self, client):
        # dag and failure_predictions are required
        resp = await client.post(
            self.ENDPOINT,
            json={"model_name": "model_a"},
            headers=_auth(),
        )
        assert resp.status_code == 422

    async def test_missing_auth_header_returns_401(self, client):
        resp = await client.post(self.ENDPOINT, json=self.VALID_BODY)
        assert resp.status_code == 401

    async def test_wrong_auth_header_returns_401(self, client):
        resp = await client.post(
            self.ENDPOINT,
            json=self.VALID_BODY,
            headers={"Authorization": "Bearer wrong-secret"},
        )
        assert resp.status_code == 401
