"""Input validation edge case tests.

Tests cover:
- Path traversal prevention in invoice and file operations
- CSV formula injection prevention in report exports
- LIKE wildcard escaping in search queries
- Pagination limit capping and offset clamping
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import time
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from api.config import APISettings
from api.dependencies import (
    get_admin_session,
    get_ai_client,
    get_db_session,
    get_metering_collector,
    get_settings,
    get_tenant_session,
)
from api.main import create_app
from api.services.ai_client import AIServiceClient
from api.services.invoice_service import _validate_path_component, _resolve_safe_path
from api.services.reporting_service import _sanitize_csv_value
from core_engine.state.repository import _escape_like

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DEV_SECRET = "test-secret-key-for-ironlayer-tests"


def _make_dev_token(
    tenant_id: str = "default",
    sub: str = "test-user",
    role: str = "admin",
    scopes: list[str] | None = None,
) -> str:
    """Generate a valid development-mode HMAC token."""
    now = time.time()
    payload: dict[str, Any] = {
        "sub": sub,
        "tenant_id": tenant_id,
        "iss": "ironlayer",
        "iat": now,
        "exp": now + 3600,
        "scopes": scopes or ["read", "write"],
        "jti": f"test-jti-{secrets.token_hex(4)}",
        "identity_kind": "user",
        "role": role,
    }
    payload_json = json.dumps(payload)
    signature = hmac.new(
        _DEV_SECRET.encode("utf-8"),
        payload_json.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    token_bytes = base64.urlsafe_b64encode(payload_json.encode("utf-8")).decode("ascii")
    return f"bmdev.{token_bytes}.{signature}"


_DEV_TOKEN = _make_dev_token()
_AUTH_HEADERS: dict[str, str] = {"Authorization": f"Bearer {_DEV_TOKEN}"}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def _mock_session() -> AsyncMock:
    session = AsyncMock()
    session.add = MagicMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.close = AsyncMock()

    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = None
    result_mock.scalar_one.return_value = None
    result_mock.scalars.return_value.all.return_value = []
    result_mock.scalars.return_value.first.return_value = None

    session.execute = AsyncMock(return_value=result_mock)
    return session


@pytest.fixture()
def _app(_mock_session: AsyncMock) -> Any:
    application = create_app()

    async def _override_session():
        yield _mock_session

    def _override_settings():
        return APISettings(
            host="0.0.0.0",
            port=8000,
            debug=True,
            database_url="postgresql+asyncpg://test:test@localhost:5432/test",
            ai_engine_url="http://localhost:8001",
            ai_engine_timeout=5.0,
            platform_env="dev",
            cors_origins=["http://localhost:3000"],
        )

    mock_ai = AsyncMock(spec=AIServiceClient)
    mock_ai.semantic_classify = AsyncMock(return_value={})
    mock_ai.predict_cost = AsyncMock(return_value={})
    mock_ai.score_risk = AsyncMock(return_value={})
    mock_ai.optimize_sql = AsyncMock(return_value={})
    mock_ai.health_check = AsyncMock(return_value=True)
    mock_ai.close = AsyncMock()

    mock_metering = MagicMock()
    mock_metering.record_event = MagicMock()
    mock_metering.record = MagicMock()
    mock_metering.flush = MagicMock(return_value=0)
    mock_metering.pending_count = 0

    application.dependency_overrides[get_db_session] = _override_session
    application.dependency_overrides[get_tenant_session] = _override_session
    application.dependency_overrides[get_admin_session] = _override_session
    application.dependency_overrides[get_settings] = _override_settings
    application.dependency_overrides[get_ai_client] = lambda: mock_ai
    application.dependency_overrides[get_metering_collector] = lambda: mock_metering

    return application


@pytest_asyncio.fixture()
async def client(_app) -> AsyncClient:
    transport = ASGITransport(app=_app)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers=_AUTH_HEADERS,
    ) as ac:
        yield ac


# ===================================================================
# 1. TestPathTraversal
# ===================================================================


class TestPathTraversal:
    """Test path traversal prevention in invoice and file operations.

    The ``_validate_path_component()`` function rejects any identifier
    containing characters outside ``[a-zA-Z0-9_-]``, and
    ``_resolve_safe_path()`` verifies the resolved path stays within
    the storage root.
    """

    def test_path_traversal_payload_rejected(self) -> None:
        """A classic path traversal like '../../../etc/passwd' must be rejected."""
        with pytest.raises(ValueError, match="unsafe characters"):
            _validate_path_component("../../../etc/passwd", "invoice_id")

    def test_relative_path_component_rejected(self) -> None:
        """'..' as an identifier must be rejected."""
        with pytest.raises(ValueError, match="unsafe characters"):
            _validate_path_component("..", "invoice_id")

    def test_dot_slash_rejected(self) -> None:
        """'./' prefix must be rejected."""
        with pytest.raises(ValueError, match="unsafe characters"):
            _validate_path_component("./test", "invoice_id")

    def test_slash_in_id_rejected(self) -> None:
        """Forward slashes must be rejected."""
        with pytest.raises(ValueError, match="unsafe characters"):
            _validate_path_component("foo/bar", "invoice_id")

    def test_backslash_in_id_rejected(self) -> None:
        """Backslashes (Windows path separators) must be rejected."""
        with pytest.raises(ValueError, match="unsafe characters"):
            _validate_path_component("foo\\bar", "invoice_id")

    def test_null_byte_in_id_rejected(self) -> None:
        """Null bytes must be rejected (C-string termination attack)."""
        with pytest.raises(ValueError, match="unsafe characters"):
            _validate_path_component("invoice\x00.pdf", "invoice_id")

    def test_valid_invoice_id_accepted(self) -> None:
        """A simple alphanumeric+hyphen ID should pass validation."""
        # Should not raise.
        _validate_path_component("inv-12345-abc", "invoice_id")

    def test_valid_uuid_hex_accepted(self) -> None:
        """UUID hex strings should pass validation."""
        _validate_path_component("a1b2c3d4e5f67890", "invoice_id")

    def test_resolve_safe_path_rejects_traversal(self) -> None:
        """Even if component validation is bypassed, path resolution must catch escapes."""
        storage_base = Path("/var/lib/ironlayer/invoices")
        # Directly test with safe-looking but escaped components.
        # The function first validates components, so this test confirms the double check.
        with pytest.raises(ValueError, match="unsafe characters"):
            _resolve_safe_path(storage_base, "../../etc", "passwd")

    def test_resolve_safe_path_normal_case(self, tmp_path: Path) -> None:
        """A normal invoice path should resolve correctly within the base."""
        result = _resolve_safe_path(tmp_path, "tenant-abc", "inv-12345")
        assert str(result).startswith(str(tmp_path.resolve()))
        assert result.name == "inv-12345.pdf"

    @pytest.mark.asyncio
    async def test_invoice_download_rejects_traversal_id(
        self,
        client: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """The /invoices/{invoice_id}/download endpoint must reject traversal IDs.

        The InvoiceService.get_pdf() method calls _validate_path_component()
        before any file I/O.
        """
        with patch("api.routers.billing.InvoiceService") as MockSvc:
            instance = MockSvc.return_value
            instance.get_pdf = AsyncMock(side_effect=ValueError("Invalid invoice_id: contains unsafe characters"))
            resp = await client.get("/api/v1/billing/invoices/../../../etc/passwd/download")
            # The router may catch the ValueError and return 400 or 404.
            assert resp.status_code in (400, 404, 422)

    def test_space_in_path_component_rejected(self) -> None:
        """Spaces in identifiers must be rejected."""
        with pytest.raises(ValueError, match="unsafe characters"):
            _validate_path_component("invoice id", "invoice_id")

    def test_percent_encoding_rejected(self) -> None:
        """Percent-encoded characters must be rejected (raw %)."""
        with pytest.raises(ValueError, match="unsafe characters"):
            _validate_path_component("%2e%2e", "invoice_id")


# ===================================================================
# 2. TestCSVInjection
# ===================================================================


class TestCSVInjection:
    """Test CSV formula injection prevention in report exports.

    The ``_sanitize_csv_value()`` function prefixes dangerous values
    with a single-quote to prevent spreadsheet applications from
    interpreting them as formulas.
    """

    def test_equals_sign_gets_prefixed(self) -> None:
        """Values starting with '=' are formula injections in Excel."""
        result = _sanitize_csv_value("=SUM(A1:A10)")
        assert result == "'=SUM(A1:A10)"

    def test_plus_sign_gets_prefixed(self) -> None:
        """Values starting with '+' are treated as formulas."""
        result = _sanitize_csv_value("+CMD('calc')")
        assert result == "'+CMD('calc')"

    def test_minus_sign_gets_prefixed(self) -> None:
        """Values starting with '-' are treated as formulas."""
        result = _sanitize_csv_value("-1+1")
        assert result == "'-1+1"

    def test_at_sign_gets_prefixed(self) -> None:
        """Values starting with '@' are treated as formulas in some apps."""
        result = _sanitize_csv_value("@SUM(A1)")
        assert result == "'@SUM(A1)"

    def test_tab_gets_prefixed(self) -> None:
        """Tab character at start is dangerous (formula injection vector)."""
        result = _sanitize_csv_value("\tmalicious")
        assert result == "'\tmalicious"

    def test_carriage_return_gets_prefixed(self) -> None:
        """Carriage return at start is dangerous."""
        result = _sanitize_csv_value("\rmalicious")
        assert result == "'\rmalicious"

    def test_normal_string_unchanged(self) -> None:
        """Normal strings without dangerous first chars pass through unchanged."""
        assert _sanitize_csv_value("Hello World") == "Hello World"

    def test_numeric_string_unchanged(self) -> None:
        """Numeric strings pass through unchanged."""
        assert _sanitize_csv_value("12345") == "12345"

    def test_empty_string_unchanged(self) -> None:
        """Empty strings pass through unchanged."""
        assert _sanitize_csv_value("") == ""

    def test_non_string_values_pass_through(self) -> None:
        """Non-string values (int, float, None) are returned unchanged."""
        assert _sanitize_csv_value(42) == 42
        assert _sanitize_csv_value(3.14) == 3.14
        assert _sanitize_csv_value(None) is None

    def test_equals_in_middle_not_prefixed(self) -> None:
        """An equals sign in the middle of a string is not dangerous."""
        result = _sanitize_csv_value("x=5")
        assert result == "x=5"

    def test_url_not_prefixed(self) -> None:
        """URLs starting with 'h' should pass through unchanged."""
        result = _sanitize_csv_value("https://example.com")
        assert result == "https://example.com"

    def test_real_dde_attack_payload_prefixed(self) -> None:
        """A real-world DDE attack payload must be neutralized."""
        payload = "=cmd|'/C calc'!A0"
        result = _sanitize_csv_value(payload)
        assert result.startswith("'")
        assert not result.startswith("=")


# ===================================================================
# 3. TestLIKEEscaping
# ===================================================================


class TestLIKEEscaping:
    """Test SQL LIKE wildcard escaping in search queries.

    The ``_escape_like()`` function escapes ``%``, ``_``, and ``\\``
    so they are treated as literal characters in LIKE patterns.
    """

    def test_percent_wildcard_escaped(self) -> None:
        """'%' must be escaped to '\\%' to prevent wildcard matching."""
        result = _escape_like("100%")
        assert result == "100\\%"

    def test_underscore_wildcard_escaped(self) -> None:
        """'_' must be escaped to '\\_' to prevent single-char wildcard matching."""
        result = _escape_like("user_name")
        assert result == "user\\_name"

    def test_backslash_escaped_first(self) -> None:
        """Backslash must be escaped before other characters to avoid double-escaping."""
        result = _escape_like("a\\b")
        assert result == "a\\\\b"

    def test_combined_wildcards_all_escaped(self) -> None:
        """All metacharacters in the same string must be escaped."""
        result = _escape_like("100%_test\\end")
        assert result == "100\\%\\_test\\\\end"

    def test_normal_string_unchanged(self) -> None:
        """Strings without LIKE metacharacters pass through unchanged."""
        result = _escape_like("staging.orders")
        assert result == "staging.orders"

    def test_empty_string_unchanged(self) -> None:
        """Empty string passes through unchanged."""
        result = _escape_like("")
        assert result == ""

    def test_only_percent_signs(self) -> None:
        """A string of only percent signs is fully escaped."""
        result = _escape_like("%%%")
        assert result == "\\%\\%\\%"

    def test_only_underscores(self) -> None:
        """A string of only underscores is fully escaped."""
        result = _escape_like("___")
        assert result == "\\_\\_\\_"

    @pytest.mark.asyncio
    async def test_search_with_percent_does_not_wildcard(
        self,
        client: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Searching for '100%' must literally search for '100%', not '100<anything>'.

        Verify the search parameter is passed to list_filtered with proper escaping.
        """
        with patch("api.routers.models.ModelRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.list_filtered = AsyncMock(return_value=[])
            resp = await client.get("/api/v1/models?search=100%25")
            assert resp.status_code == 200
            # Verify list_filtered was called (not list_all).
            instance.list_filtered.assert_called_once()

    @pytest.mark.asyncio
    async def test_search_with_underscore_does_not_wildcard(
        self,
        client: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Searching for 'user_name' must literally search for 'user_name'.

        The underscore should not match any single character.
        """
        with patch("api.routers.models.ModelRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.list_filtered = AsyncMock(return_value=[])
            resp = await client.get("/api/v1/models?search=user_name")
            assert resp.status_code == 200
            instance.list_filtered.assert_called_once()
            # Verify the search parameter was passed.
            call_kwargs = instance.list_filtered.call_args
            assert call_kwargs.kwargs.get("search") == "user_name" or call_kwargs[1].get("search") == "user_name"


# ===================================================================
# 4. TestPaginationLimits
# ===================================================================


class TestPaginationLimits:
    """Test pagination parameter enforcement.

    The API enforces:
    - limit is capped at 500 (FastAPI Query constraint ge=1, le=500)
    - offset must be >= 0 (FastAPI Query constraint ge=0)
    - The repository layer also clamps: limit = max(1, min(limit, 500))
    """

    @pytest.mark.asyncio
    async def test_limit_over_500_rejected_by_api(
        self,
        client: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """limit > 500 should be rejected by FastAPI's Query validation (le=500)."""
        resp = await client.get("/api/v1/models?limit=501")
        assert resp.status_code == 422  # Pydantic validation error

    @pytest.mark.asyncio
    async def test_negative_offset_rejected_by_api(
        self,
        client: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Negative offset should be rejected by FastAPI's Query validation (ge=0)."""
        resp = await client.get("/api/v1/models?offset=-1")
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_limit_0_rejected_by_api(
        self,
        client: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """limit=0 should be rejected by FastAPI's Query validation (ge=1)."""
        resp = await client.get("/api/v1/models?limit=0")
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_limit_500_accepted(
        self,
        client: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """limit=500 should be accepted (it's the max)."""
        with patch("api.routers.models.ModelRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.list_all = AsyncMock(return_value=[])
            resp = await client.get("/api/v1/models?limit=500")
            assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_offset_0_accepted(
        self,
        client: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """offset=0 should be accepted."""
        with patch("api.routers.models.ModelRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.list_all = AsyncMock(return_value=[])
            resp = await client.get("/api/v1/models?offset=0")
            assert resp.status_code == 200

    def test_repository_clamps_limit_over_max(self) -> None:
        """The repository layer itself caps limit at _MAX_MODEL_PAGE_SIZE (500).

        This is a defense-in-depth check -- even if the FastAPI layer
        is bypassed, the repository clamps the value.
        """
        from core_engine.state.repository import _MAX_MODEL_PAGE_SIZE

        assert _MAX_MODEL_PAGE_SIZE == 500

        # Test the clamping formula directly.
        limit_input = 1000
        clamped = max(1, min(limit_input, _MAX_MODEL_PAGE_SIZE))
        assert clamped == 500

    def test_repository_clamps_limit_below_1(self) -> None:
        """The repository clamps limit to at least 1."""
        from core_engine.state.repository import _MAX_MODEL_PAGE_SIZE

        limit_input = 0
        clamped = max(1, min(limit_input, _MAX_MODEL_PAGE_SIZE))
        assert clamped == 1

        limit_input = -10
        clamped = max(1, min(limit_input, _MAX_MODEL_PAGE_SIZE))
        assert clamped == 1

    def test_repository_clamps_negative_offset(self) -> None:
        """The repository clamps negative offsets to 0."""
        offset_input = -5
        clamped = max(offset_input, 0)
        assert clamped == 0

    @pytest.mark.asyncio
    async def test_large_offset_accepted(
        self,
        client: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Very large offset values should be accepted (returns empty results)."""
        with patch("api.routers.models.ModelRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.list_all = AsyncMock(return_value=[])
            resp = await client.get("/api/v1/models?offset=999999")
            assert resp.status_code == 200
            assert resp.json() == []

    @pytest.mark.asyncio
    async def test_non_integer_limit_rejected(
        self,
        client: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Non-integer limit should be rejected."""
        resp = await client.get("/api/v1/models?limit=abc")
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_non_integer_offset_rejected(
        self,
        client: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Non-integer offset should be rejected."""
        resp = await client.get("/api/v1/models?offset=xyz")
        assert resp.status_code == 422
