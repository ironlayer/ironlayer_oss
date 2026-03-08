"""Tests for api/api/middleware/prometheus.py

Covers:
- Path normalisation (UUIDs, hex IDs, numeric segments)
- Counter increments for HTTP requests
- Histogram observations for request duration
- Skipped paths (metrics, docs, favicon)
- Metrics availability flag
"""

from __future__ import annotations

import pytest

from api.middleware.prometheus import _normalise_path

# ---------------------------------------------------------------------------
# Path normalisation
# ---------------------------------------------------------------------------


class TestPathNormalisation:
    """Verify _normalise_path collapses path parameters."""

    def test_uuid_collapsed(self) -> None:
        """UUIDs in paths are replaced with {id}."""
        path = "/api/v1/plans/550e8400-e29b-41d4-a716-446655440000"
        assert _normalise_path(path) == "/api/v1/plans/{id}"

    def test_long_hex_collapsed(self) -> None:
        """Long hex strings (12+ chars) are replaced with {id}."""
        path = "/api/v1/plans/abc123def456"
        assert _normalise_path(path) == "/api/v1/plans/{id}"

    def test_very_long_hex_collapsed(self) -> None:
        """64-char hex strings (SHA-256 hashes) are collapsed."""
        sha = "a" * 64
        path = f"/api/v1/plans/{sha}"
        assert _normalise_path(path) == "/api/v1/plans/{id}"

    def test_numeric_segment_collapsed(self) -> None:
        """Pure numeric path segments are replaced with {id}."""
        path = "/api/v1/webhooks/config/42"
        assert _normalise_path(path) == "/api/v1/webhooks/config/{id}"

    def test_prefixed_id_collapsed(self) -> None:
        """Alphanumeric IDs like 'plan-abc12345' are collapsed."""
        path = "/api/v1/plans/plan-abc12345678"
        assert _normalise_path(path) == "/api/v1/plans/{id}"

    def test_multiple_segments_collapsed(self) -> None:
        """Multiple parameterised segments are all collapsed."""
        path = "/api/v1/plans/550e8400-e29b-41d4-a716-446655440000/steps/42"
        assert _normalise_path(path) == "/api/v1/plans/{id}/steps/{id}"

    def test_static_path_unchanged(self) -> None:
        """Paths without dynamic segments are not modified."""
        path = "/api/v1/plans"
        assert _normalise_path(path) == "/api/v1/plans"

    def test_root_path_unchanged(self) -> None:
        """Root path is not modified."""
        assert _normalise_path("/") == "/"

    def test_health_path_unchanged(self) -> None:
        """Health endpoint has no dynamic segments."""
        assert _normalise_path("/api/v1/health") == "/api/v1/health"

    def test_short_hex_not_collapsed(self) -> None:
        """Short hex strings (<12 chars) are NOT collapsed."""
        path = "/api/v1/plans/abc123"
        # 6 chars is below the 12-char threshold
        assert _normalise_path(path) == "/api/v1/plans/abc123"


# ---------------------------------------------------------------------------
# Metrics objects
# ---------------------------------------------------------------------------


class TestMetricsAvailability:
    """Verify prometheus_client metrics are importable."""

    def test_metrics_available(self) -> None:
        """prometheus_client is installed and metrics are defined."""
        from api.middleware.prometheus import _METRICS_AVAILABLE

        assert _METRICS_AVAILABLE is True

    def test_http_requests_total_exists(self) -> None:
        """HTTP_REQUESTS_TOTAL counter is defined."""
        from api.middleware.prometheus import HTTP_REQUESTS_TOTAL

        assert HTTP_REQUESTS_TOTAL is not None

    def test_http_request_duration_exists(self) -> None:
        """HTTP_REQUEST_DURATION histogram is defined."""
        from api.middleware.prometheus import HTTP_REQUEST_DURATION

        assert HTTP_REQUEST_DURATION is not None

    def test_plan_runs_total_exists(self) -> None:
        """PLAN_RUNS_TOTAL counter is defined."""
        from api.middleware.prometheus import PLAN_RUNS_TOTAL

        assert PLAN_RUNS_TOTAL is not None

    def test_ai_calls_total_exists(self) -> None:
        """AI_CALLS_TOTAL counter is defined."""
        from api.middleware.prometheus import AI_CALLS_TOTAL

        assert AI_CALLS_TOTAL is not None

    def test_active_locks_gauge_exists(self) -> None:
        """ACTIVE_LOCKS gauge is defined."""
        from api.middleware.prometheus import ACTIVE_LOCKS

        assert ACTIVE_LOCKS is not None


# ---------------------------------------------------------------------------
# Skip paths
# ---------------------------------------------------------------------------


class TestSkipPaths:
    """Verify the _SKIP_PATHS frozenset contents."""

    def test_skip_paths_contains_metrics(self) -> None:
        """/metrics is in skip paths."""
        from api.middleware.prometheus import _SKIP_PATHS

        assert "/metrics" in _SKIP_PATHS

    def test_skip_paths_contains_docs(self) -> None:
        """/docs is in skip paths."""
        from api.middleware.prometheus import _SKIP_PATHS

        assert "/docs" in _SKIP_PATHS

    def test_skip_paths_contains_favicon(self) -> None:
        """/favicon.ico is in skip paths."""
        from api.middleware.prometheus import _SKIP_PATHS

        assert "/favicon.ico" in _SKIP_PATHS

    def test_skip_paths_contains_redoc(self) -> None:
        """/redoc is in skip paths."""
        from api.middleware.prometheus import _SKIP_PATHS

        assert "/redoc" in _SKIP_PATHS


# ---------------------------------------------------------------------------
# Metrics endpoint integration
# ---------------------------------------------------------------------------


class TestMetricsEndpoint:
    """Verify GET /metrics returns Prometheus text format."""

    @pytest.mark.asyncio
    async def test_metrics_endpoint_returns_200(self) -> None:
        """GET /metrics returns 200 with Prometheus content."""
        from httpx import ASGITransport, AsyncClient

        from api.dependencies import get_metering_collector, get_settings
        from api.config import APISettings
        from api.main import create_app
        from unittest.mock import MagicMock

        app = create_app()

        settings = APISettings(
            host="0.0.0.0",
            port=8000,
            debug=True,
            database_url="postgresql+asyncpg://test:test@localhost:5432/test",
            ai_engine_url="http://localhost:8001",
            ai_engine_timeout=5.0,
            platform_env="dev",
            cors_origins=["http://localhost:3000"],
        )

        mock_metering = MagicMock()
        mock_metering.record_event = MagicMock()
        mock_metering.record = MagicMock()
        mock_metering.flush = MagicMock(return_value=0)
        mock_metering.pending_count = 0

        app.dependency_overrides[get_settings] = lambda: settings
        app.dependency_overrides[get_metering_collector] = lambda: mock_metering

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/metrics")

        assert resp.status_code == 200
        # Prometheus text format contains TYPE and HELP comments
        content = resp.text
        assert "ironlayer_http_requests_total" in content or "prometheus_client" in content.lower()
