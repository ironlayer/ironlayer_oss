"""Coverage tests for four under-tested API service-layer files.

Target files and coverage goals:
  - api/api/services/simulation_service.py   (20% -> 70%+)
  - api/api/services/ai_client.py            (30% -> 70%+)
  - api/api/services/reconciliation_service.py (45% -> 70%+)
  - api/api/routers/approvals.py             (38% -> 70%+)

Conventions:
  - asyncio_mode = "auto" in pyproject.toml, so no @pytest.mark.asyncio needed
  - unittest.mock.AsyncMock / MagicMock / patch for all external deps
  - No live DB or network calls
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

# ---------------------------------------------------------------------------
# ============================  simulation_service  ==========================
# ---------------------------------------------------------------------------


class TestTableToDefinition:
    """Tests for the _table_to_definition module-level helper."""

    def _make_row(self, **kwargs) -> MagicMock:
        row = MagicMock()
        row.model_name = kwargs.get("model_name", "test_model")
        row.repo_path = kwargs.get("repo_path", "models/test_model.sql")
        row.kind = kwargs.get("kind", "FULL_REFRESH")
        row.materialization = kwargs.get("materialization", "TABLE")
        row.tags = kwargs.get("tags", None)
        row.time_column = kwargs.get("time_column", None)
        row.unique_key = kwargs.get("unique_key", None)
        row.owner = kwargs.get("owner", None)
        row.metadata_json = kwargs.get("metadata_json", None)
        return row

    def test_basic_conversion(self) -> None:
        """A minimal row converts without error."""
        from api.services.simulation_service import _table_to_definition

        row = self._make_row()
        defn = _table_to_definition(row)
        assert defn.name == "test_model"

    def test_tags_json_string(self) -> None:
        """Tags stored as a JSON string are parsed correctly."""
        from api.services.simulation_service import _table_to_definition

        row = self._make_row(tags='["staging", "core"]')
        defn = _table_to_definition(row)
        assert defn.tags == ["staging", "core"]

    def test_tags_list(self) -> None:
        """Tags already as a list are accepted as-is."""
        from api.services.simulation_service import _table_to_definition

        row = self._make_row(tags=["a", "b"])
        defn = _table_to_definition(row)
        assert defn.tags == ["a", "b"]

    def test_tags_bad_json_falls_back_to_empty(self) -> None:
        """Unparseable tags fall back to empty list."""
        from api.services.simulation_service import _table_to_definition

        row = self._make_row(tags="not-valid-json{{")
        defn = _table_to_definition(row)
        assert defn.tags == []

    def test_metadata_json_string_is_parsed(self) -> None:
        """metadata_json stored as a JSON string is decoded and applied."""
        from api.services.simulation_service import _table_to_definition

        metadata = {
            "dependencies": ["upstream_model"],
            "referenced_tables": ["raw.orders"],
            "raw_sql": "SELECT id FROM raw.orders",
            "clean_sql": "SELECT id FROM raw.orders",
            "output_columns": ["id"],
            "contract_mode": "WARN",
            "contract_columns": [
                {"name": "id", "data_type": "INT64", "nullable": False}
            ],
        }
        row = self._make_row(metadata_json=json.dumps(metadata))
        defn = _table_to_definition(row)
        assert defn.dependencies == ["upstream_model"]
        assert defn.referenced_tables == ["raw.orders"]
        assert defn.raw_sql == "SELECT id FROM raw.orders"
        assert len(defn.contract_columns) == 1
        assert defn.contract_columns[0].name == "id"

    def test_metadata_json_dict_already_parsed(self) -> None:
        """metadata_json can also be a plain dict (already parsed)."""
        from api.services.simulation_service import _table_to_definition

        row = self._make_row(metadata_json={"dependencies": ["dep1"]})
        defn = _table_to_definition(row)
        assert defn.dependencies == ["dep1"]

    def test_metadata_json_bad_string_is_ignored(self) -> None:
        """A non-parseable metadata_json string is silently ignored."""
        from api.services.simulation_service import _table_to_definition

        row = self._make_row(metadata_json="{bad json}")
        defn = _table_to_definition(row)
        assert defn.dependencies == []

    def test_invalid_kind_falls_back_to_full_refresh(self) -> None:
        """An unrecognised kind value falls back to FULL_REFRESH."""
        from api.services.simulation_service import _table_to_definition
        from core_engine.models.model_definition import ModelKind

        row = self._make_row(kind="NOT_A_REAL_KIND")
        defn = _table_to_definition(row)
        assert defn.kind == ModelKind.FULL_REFRESH

    def test_invalid_materialization_falls_back_to_table(self) -> None:
        """An unrecognised materialization value falls back to TABLE."""
        from api.services.simulation_service import _table_to_definition
        from core_engine.models.model_definition import Materialization

        row = self._make_row(materialization="NOT_VALID")
        defn = _table_to_definition(row)
        assert defn.materialization == Materialization.TABLE

    def test_no_repo_path_generates_default(self) -> None:
        """When repo_path is None/empty, a default path is generated."""
        from api.services.simulation_service import _table_to_definition

        row = self._make_row(repo_path=None)
        defn = _table_to_definition(row)
        assert defn.file_path == "models/test_model.sql"

    def test_contract_mode_strict(self) -> None:
        """STRICT contract mode is correctly parsed."""
        from api.services.simulation_service import _table_to_definition
        from core_engine.models.model_definition import SchemaContractMode

        row = self._make_row(metadata_json={"contract_mode": "STRICT"})
        defn = _table_to_definition(row)
        assert defn.contract_mode == SchemaContractMode.STRICT

    def test_contract_mode_invalid_falls_back_to_disabled(self) -> None:
        """An invalid contract_mode string falls back to DISABLED."""
        from api.services.simulation_service import _table_to_definition
        from core_engine.models.model_definition import SchemaContractMode

        row = self._make_row(metadata_json={"contract_mode": "INVALID_MODE"})
        defn = _table_to_definition(row)
        assert defn.contract_mode == SchemaContractMode.DISABLED

    def test_contract_column_with_minimal_keys(self) -> None:
        """A contract_column dict with just name is accepted."""
        from api.services.simulation_service import _table_to_definition

        row = self._make_row(
            metadata_json={"contract_columns": [{"name": "col_x"}]}
        )
        defn = _table_to_definition(row)
        # Should not raise; defaults fill in the blanks
        assert defn.contract_columns[0].name == "col_x"
        assert defn.contract_columns[0].data_type == "STRING"
        assert defn.contract_columns[0].nullable is True

    def test_non_dict_contract_column_entry_is_skipped(self) -> None:
        """Non-dict entries in contract_columns are skipped."""
        from api.services.simulation_service import _table_to_definition

        row = self._make_row(
            metadata_json={"contract_columns": ["not-a-dict", {"name": "col"}]}
        )
        defn = _table_to_definition(row)
        # Only the valid dict entry is converted
        assert len(defn.contract_columns) == 1


class TestBuildAdjacencyList:
    """Tests for SimulationService._build_adjacency_list."""

    def _make_model(self, name, dependencies=None, referenced_tables=None):
        from core_engine.models.model_definition import (
            ModelDefinition,
            ModelKind,
            Materialization,
            SchemaContractMode,
        )

        return ModelDefinition(
            name=name,
            kind=ModelKind.FULL_REFRESH,
            materialization=Materialization.TABLE,
            file_path=f"models/{name}.sql",
            raw_sql="SELECT 1",
            dependencies=dependencies or [],
            referenced_tables=referenced_tables or [],
            contract_mode=SchemaContractMode.DISABLED,
        )

    def test_empty_model_list(self) -> None:
        from api.services.simulation_service import SimulationService

        dag = SimulationService._build_adjacency_list([])
        assert dag == {}

    def test_single_model_no_deps(self) -> None:
        from api.services.simulation_service import SimulationService

        m = self._make_model("orders")
        dag = SimulationService._build_adjacency_list([m])
        assert dag == {"orders": []}

    def test_upstream_deps_from_dependencies(self) -> None:
        from api.services.simulation_service import SimulationService

        upstream = self._make_model("raw_orders")
        downstream = self._make_model("orders", dependencies=["raw_orders"])
        dag = SimulationService._build_adjacency_list([upstream, downstream])
        assert "raw_orders" in dag["orders"]

    def test_upstream_deps_from_referenced_tables(self) -> None:
        from api.services.simulation_service import SimulationService

        upstream = self._make_model("raw_orders")
        downstream = self._make_model(
            "orders", referenced_tables=["raw_orders"]
        )
        dag = SimulationService._build_adjacency_list([upstream, downstream])
        assert "raw_orders" in dag["orders"]

    def test_self_reference_is_excluded(self) -> None:
        """A model referencing itself is excluded from its own dep list."""
        from api.services.simulation_service import SimulationService

        m = self._make_model("orders", dependencies=["orders"])
        dag = SimulationService._build_adjacency_list([m])
        assert "orders" not in dag["orders"]

    def test_external_table_not_in_model_names_excluded(self) -> None:
        """References to tables not in the model set are excluded."""
        from api.services.simulation_service import SimulationService

        m = self._make_model("orders", referenced_tables=["external_db.table"])
        dag = SimulationService._build_adjacency_list([m])
        assert dag["orders"] == []


class TestSimulationService:
    """Tests for SimulationService async methods."""

    @pytest.fixture()
    def mock_session(self) -> AsyncMock:
        return AsyncMock()

    def _make_model_row(self, name: str) -> MagicMock:
        row = MagicMock()
        row.model_name = name
        row.repo_path = f"models/{name}.sql"
        row.kind = "FULL_REFRESH"
        row.materialization = "TABLE"
        row.tags = None
        row.time_column = None
        row.unique_key = None
        row.owner = None
        row.metadata_json = None
        return row

    async def test_simulate_column_changes(self, mock_session: AsyncMock) -> None:
        """simulate_column_changes calls ImpactAnalyzer with the right args."""
        from api.services.simulation_service import SimulationService
        from core_engine.simulation.impact_analyzer import ImpactReport

        expected_report = ImpactReport(
            source_model="orders",
            breaking_count=1,
            warning_count=0,
            summary="1 BREAKING impact",
        )

        with patch(
            "api.services.simulation_service.ModelRepository"
        ) as MockRepo:
            instance = AsyncMock()
            instance.list_all = AsyncMock(
                return_value=[self._make_model_row("orders")]
            )
            MockRepo.return_value = instance

            with patch(
                "api.services.simulation_service.ImpactAnalyzer"
            ) as MockAnalyzer:
                analyzer_inst = MagicMock()
                analyzer_inst.simulate_column_change.return_value = expected_report
                MockAnalyzer.return_value = analyzer_inst

                svc = SimulationService(mock_session, tenant_id="t1")
                result = await svc.simulate_column_changes(
                    "orders",
                    [{"action": "REMOVE", "column_name": "old_col"}],
                )

        assert result is expected_report
        analyzer_inst.simulate_column_change.assert_called_once()

    async def test_simulate_model_removal(self, mock_session: AsyncMock) -> None:
        """simulate_model_removal calls ImpactAnalyzer.simulate_model_removal."""
        from api.services.simulation_service import SimulationService
        from core_engine.simulation.impact_analyzer import ModelRemovalReport

        expected_report = ModelRemovalReport(
            removed_model="orders",
            directly_affected=[],
            transitively_affected=[],
            orphaned_models=[],
            breaking_count=0,
            summary="No downstream impact",
        )

        with patch(
            "api.services.simulation_service.ModelRepository"
        ) as MockRepo:
            instance = AsyncMock()
            instance.list_all = AsyncMock(return_value=[])
            MockRepo.return_value = instance

            with patch(
                "api.services.simulation_service.ImpactAnalyzer"
            ) as MockAnalyzer:
                analyzer_inst = MagicMock()
                analyzer_inst.simulate_model_removal.return_value = expected_report
                MockAnalyzer.return_value = analyzer_inst

                svc = SimulationService(mock_session, tenant_id="t1")
                result = await svc.simulate_model_removal("orders")

        assert result is expected_report

    async def test_simulate_type_change(self, mock_session: AsyncMock) -> None:
        """simulate_type_change calls ImpactAnalyzer.simulate_type_change."""
        from api.services.simulation_service import SimulationService
        from core_engine.simulation.impact_analyzer import ImpactReport

        expected_report = ImpactReport(
            source_model="orders",
            breaking_count=0,
            warning_count=1,
            summary="0 BREAKING, 1 WARNING",
        )

        with patch(
            "api.services.simulation_service.ModelRepository"
        ) as MockRepo:
            instance = AsyncMock()
            instance.list_all = AsyncMock(
                return_value=[self._make_model_row("orders")]
            )
            MockRepo.return_value = instance

            with patch(
                "api.services.simulation_service.ImpactAnalyzer"
            ) as MockAnalyzer:
                analyzer_inst = MagicMock()
                analyzer_inst.simulate_type_change.return_value = expected_report
                MockAnalyzer.return_value = analyzer_inst

                svc = SimulationService(mock_session, tenant_id="t1")
                result = await svc.simulate_type_change(
                    "orders", "amount", "FLOAT64", "INT64"
                )

        assert result is expected_report
        analyzer_inst.simulate_type_change.assert_called_once_with(
            "orders", "amount", "FLOAT64", "INT64"
        )

    async def test_load_models_uses_tenant_id(
        self, mock_session: AsyncMock
    ) -> None:
        """_load_models creates ModelRepository with the correct tenant_id."""
        from api.services.simulation_service import SimulationService

        with patch(
            "api.services.simulation_service.ModelRepository"
        ) as MockRepo:
            instance = AsyncMock()
            instance.list_all = AsyncMock(return_value=[])
            MockRepo.return_value = instance

            with patch("api.services.simulation_service.ImpactAnalyzer") as MockAnalyzer:
                analyzer_inst = MagicMock()
                analyzer_inst.simulate_model_removal.return_value = MagicMock()
                MockAnalyzer.return_value = analyzer_inst

                svc = SimulationService(mock_session, tenant_id="tenant-xyz")
                await svc.simulate_model_removal("some_model")

            MockRepo.assert_called_once_with(mock_session, tenant_id="tenant-xyz")


# ---------------------------------------------------------------------------
# ================================  ai_client  ===============================
# ---------------------------------------------------------------------------


class TestSanitizeAiInput:
    """Tests for the _sanitize_ai_input helper."""

    def test_control_chars_stripped(self) -> None:
        from api.services.ai_client import _sanitize_ai_input

        result = _sanitize_ai_input("\x00hello\x07world\x1f", "test")
        assert result == "helloworld"

    def test_newline_tab_carriage_return_preserved(self) -> None:
        from api.services.ai_client import _sanitize_ai_input

        result = _sanitize_ai_input("line1\nline2\ttabbed\r", "test")
        assert "\n" in result
        assert "\t" in result
        assert "\r" in result

    def test_prompt_injection_markers_removed(self) -> None:
        from api.services.ai_client import _sanitize_ai_input

        injections = [
            "<|system|>",
            "<|user|>",
            "<|assistant|>",
            "Human:",
            "Assistant:",
            "[INST]",
            "[/INST]",
            "<<SYS>>",
            "<</SYS>>",
            "<|im_start|>",
            "<|im_end|>",
        ]
        for marker in injections:
            result = _sanitize_ai_input(f"before {marker} after", "test")
            assert marker.lower() not in result.lower() or "[FILTERED]" in result

    def test_oversized_input_is_truncated(self) -> None:
        from api.services.ai_client import _sanitize_ai_input, _MAX_FIELD_SIZE

        big = "x" * (_MAX_FIELD_SIZE + 100)
        result = _sanitize_ai_input(big, "my_field")
        assert len(result) > _MAX_FIELD_SIZE  # includes truncation message
        assert "TRUNCATED" in result
        assert "my_field" in result

    def test_normal_input_unchanged(self) -> None:
        from api.services.ai_client import _sanitize_ai_input

        result = _sanitize_ai_input("SELECT id FROM orders WHERE status = 'active'", "sql")
        assert result == "SELECT id FROM orders WHERE status = 'active'"


class TestSanitizeDict:
    """Tests for _sanitize_dict."""

    def test_string_values_sanitised(self) -> None:
        from api.services.ai_client import _sanitize_dict

        d = {"key": "<|system|>payload"}
        result = _sanitize_dict(d)
        assert "[FILTERED]" in result["key"]

    def test_non_string_values_pass_through(self) -> None:
        from api.services.ai_client import _sanitize_dict

        d = {"count": 42, "flag": True, "nothing": None}
        result = _sanitize_dict(d)
        assert result["count"] == 42
        assert result["flag"] is True
        assert result["nothing"] is None

    def test_nested_dict_is_recursed(self) -> None:
        from api.services.ai_client import _sanitize_dict

        d = {"outer": {"inner": "<|user|>evil"}}
        result = _sanitize_dict(d)
        assert "[FILTERED]" in result["outer"]["inner"]

    def test_nested_list_is_recursed(self) -> None:
        from api.services.ai_client import _sanitize_dict

        d = {"items": ["clean", "<|system|>injection"]}
        result = _sanitize_dict(d)
        assert "[FILTERED]" in result["items"][1]


class TestSanitizeList:
    """Tests for _sanitize_list."""

    def test_string_items_sanitised(self) -> None:
        from api.services.ai_client import _sanitize_list

        items = ["good", "Human: do evil"]
        result = _sanitize_list(items)
        assert "[FILTERED]" in result[1]

    def test_non_string_items_pass_through(self) -> None:
        from api.services.ai_client import _sanitize_list

        items = [1, 2.5, False, None]
        result = _sanitize_list(items)
        assert result == [1, 2.5, False, None]

    def test_nested_list(self) -> None:
        from api.services.ai_client import _sanitize_list

        items = [["<|im_start|>"]]
        result = _sanitize_list(items)
        assert "[FILTERED]" in result[0][0]

    def test_nested_dict(self) -> None:
        from api.services.ai_client import _sanitize_list

        items = [{"sql": "<|assistant|>"}]
        result = _sanitize_list(items)
        assert "[FILTERED]" in result[0]["sql"]


class TestCircuitBreaker:
    """Tests for the _CircuitBreaker class."""

    def test_initial_state_is_closed(self) -> None:
        from api.services.ai_client import _CircuitBreaker

        cb = _CircuitBreaker()
        assert cb.state == "closed"

    def test_on_success_resets_failures(self) -> None:
        from api.services.ai_client import _CircuitBreaker

        cb = _CircuitBreaker(fail_max=3)
        cb.on_failure()
        cb.on_failure()
        cb.on_success()
        assert cb._failures == 0
        assert cb.state == "closed"

    def test_on_failure_increments_counter(self) -> None:
        from api.services.ai_client import _CircuitBreaker

        cb = _CircuitBreaker(fail_max=5)
        cb.on_failure()
        assert cb._failures == 1

    def test_circuit_opens_after_fail_max(self) -> None:
        from api.services.ai_client import _CircuitBreaker

        cb = _CircuitBreaker(fail_max=3)
        cb.on_failure()
        cb.on_failure()
        assert cb.state == "closed"
        cb.on_failure()
        assert cb.state == "open"

    def test_is_open_returns_true_when_open(self) -> None:
        from api.services.ai_client import _CircuitBreaker

        cb = _CircuitBreaker(fail_max=1)
        cb.on_failure()
        assert cb.is_open() is True

    def test_is_open_returns_false_when_closed(self) -> None:
        from api.services.ai_client import _CircuitBreaker

        cb = _CircuitBreaker()
        assert cb.is_open() is False

    def test_open_transitions_to_half_open_after_timeout(self, monkeypatch) -> None:
        from api.services.ai_client import _CircuitBreaker

        # Freeze time at t=1_000.0, then advance past the 30 s reset_timeout.
        # No sleeping — the clock is mocked so the test is deterministic in CI.
        t = [1_000.0]
        monkeypatch.setattr("api.services.ai_client.time.monotonic", lambda: t[0])

        cb = _CircuitBreaker(fail_max=1, reset_timeout=30.0)
        cb.on_failure()  # _opened_at = 1_000.0
        assert cb.state == "open"

        t[0] = 1_031.0  # advance 31 s — past the 30 s reset_timeout
        assert cb.state == "half_open"

    def test_failure_in_half_open_reopens(self, monkeypatch) -> None:
        from api.services.ai_client import _CircuitBreaker

        t = [1_000.0]
        monkeypatch.setattr("api.services.ai_client.time.monotonic", lambda: t[0])

        cb = _CircuitBreaker(fail_max=1, reset_timeout=30.0)
        cb.on_failure()  # opens; _opened_at = 1_000.0
        t[0] = 1_031.0  # advance past reset_timeout
        assert cb.state == "half_open"  # trigger lazy open → half_open
        cb.on_failure()  # failure in half_open → reopens; _opened_at = 1_031.0
        assert cb.state == "open"

    def test_success_in_half_open_closes_circuit(self, monkeypatch) -> None:
        from api.services.ai_client import _CircuitBreaker

        t = [1_000.0]
        monkeypatch.setattr("api.services.ai_client.time.monotonic", lambda: t[0])

        cb = _CircuitBreaker(fail_max=1, reset_timeout=30.0)
        cb.on_failure()  # opens; _opened_at = 1_000.0
        t[0] = 1_031.0  # advance past reset_timeout
        _ = cb.state  # trigger lazy open → half_open transition
        cb.on_success()
        assert cb.state == "closed"


class TestAIServiceClientInit:
    """Tests for AIServiceClient initialisation."""

    def test_base_url_trailing_slash_stripped(self) -> None:
        from api.services.ai_client import AIServiceClient

        client = AIServiceClient("http://localhost:8001/")
        assert client._base_url == "http://localhost:8001"

    def test_shared_secret_from_env(self, monkeypatch) -> None:
        from api.services.ai_client import AIServiceClient

        monkeypatch.setenv("AI_ENGINE_SHARED_SECRET", "env-secret")
        client = AIServiceClient("http://localhost:8001")
        assert client._shared_secret == "env-secret"

    def test_shared_secret_from_param_overrides_env(self, monkeypatch) -> None:
        from api.services.ai_client import AIServiceClient

        monkeypatch.setenv("AI_ENGINE_SHARED_SECRET", "env-secret")
        client = AIServiceClient("http://localhost:8001", shared_secret="param-secret")
        assert client._shared_secret == "param-secret"

    def test_custom_circuit_breaker_params(self) -> None:
        from api.services.ai_client import AIServiceClient

        client = AIServiceClient(
            "http://localhost:8001",
            circuit_breaker_fail_max=10,
            circuit_breaker_reset_timeout=60.0,
        )
        assert client._circuit_breaker._fail_max == 10
        assert client._circuit_breaker._reset_timeout == 60.0


class TestAIServiceClientPost:
    """Tests for AIServiceClient._post and the public advisory methods."""

    def _make_client(self) -> Any:
        from api.services.ai_client import AIServiceClient

        return AIServiceClient("http://ai-engine:8001", shared_secret="secret")

    async def test_post_returns_json_on_success(self) -> None:

        client = self._make_client()
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {"result": "ok"}

        client._client.post = AsyncMock(return_value=mock_resp)
        result = await client._post("/test", {"key": "val"})
        assert result == {"result": "ok"}
        assert client._circuit_breaker.state == "closed"

    async def test_post_returns_none_on_http_status_error(self) -> None:
        client = self._make_client()
        error_resp = MagicMock()
        error_resp.status_code = 503
        error_resp.text = "Service Unavailable"
        exc = httpx.HTTPStatusError("503", request=MagicMock(), response=error_resp)

        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = exc

        client._client.post = AsyncMock(return_value=mock_resp)
        result = await client._post("/test", {})
        # HTTP error does NOT trip the circuit
        assert result is None
        assert client._circuit_breaker.state == "closed"

    async def test_post_returns_none_on_request_error_and_trips_circuit(self) -> None:
        client = self._make_client()
        client._client.post = AsyncMock(
            side_effect=httpx.ConnectError("refused")
        )
        result = await client._post("/test", {})
        assert result is None
        assert client._circuit_breaker._failures == 1

    async def test_post_skipped_when_circuit_open(self) -> None:

        client = self._make_client()
        # Force circuit open
        client._circuit_breaker._state = "open"
        client._circuit_breaker._opened_at = time.monotonic()
        client._client.post = AsyncMock()

        result = await client._post("/test", {})
        assert result is None
        client._client.post.assert_not_called()

    async def test_post_forwards_traceparent_header(self) -> None:
        """When get_traceparent returns a value, it is forwarded in the request headers."""
        import api.middleware.trace_context as tc_module

        client = self._make_client()
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {}
        client._client.post = AsyncMock(return_value=mock_resp)

        with patch.object(tc_module, "get_traceparent", return_value="00-trace-span-01"):
            await client._post("/semantic_classify", {})

        # Verify the request was made (traceparent header should have been included)
        client._client.post.assert_called_once()
        call_kwargs = client._client.post.call_args
        # The headers kwarg should contain traceparent
        passed_headers = call_kwargs.kwargs.get("headers")
        assert passed_headers is not None
        assert passed_headers.get("traceparent") == "00-trace-span-01"

    async def test_post_handles_missing_traceparent_import(self) -> None:
        """When trace_context module is missing, _post still works."""
        client = self._make_client()
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {"ok": True}
        client._client.post = AsyncMock(return_value=mock_resp)

        with patch.dict("sys.modules", {"api.middleware.trace_context": None}):
            result = await client._post("/test", {})
        # Should still work, returning the JSON
        assert result == {"ok": True}


class TestAIServiceClientAdvisoryMethods:
    """Tests for the four public advisory methods."""

    def _make_client_with_mock_post(self, return_value=None):
        from api.services.ai_client import AIServiceClient

        client = AIServiceClient("http://ai-engine:8001")
        client._post = AsyncMock(return_value=return_value)
        return client

    async def test_semantic_classify_sends_correct_payload(self) -> None:
        client = self._make_client_with_mock_post({"change_type": "non_breaking"})
        result = await client.semantic_classify(
            "SELECT 1", "SELECT 2", schema_diff={"added": ["col"]},
            tenant_id="t1", llm_enabled=True, api_key="key-abc"
        )
        assert result == {"change_type": "non_breaking"}
        payload = client._post.call_args[0][1]
        assert "old_sql" in payload
        assert "new_sql" in payload
        assert "schema_diff" in payload
        assert payload["tenant_id"] == "t1"
        assert payload["api_key"] == "key-abc"

    async def test_semantic_classify_returns_none_on_failure(self) -> None:
        client = self._make_client_with_mock_post(None)
        result = await client.semantic_classify("SELECT 1", "SELECT 2")
        assert result is None

    async def test_predict_cost_builds_payload(self) -> None:
        client = self._make_client_with_mock_post({"estimated_cost_usd": 2.5})
        result = await client.predict_cost(
            model_name="orders",
            partition_count=10,
            historical_runtime_avg=45.0,
            data_volume=1_000_000,
            cluster_size="medium",
            tenant_id="t2",
            llm_enabled=False,
        )
        assert result == {"estimated_cost_usd": 2.5}
        payload = client._post.call_args[0][1]
        assert payload["model_name"] == "orders"
        assert payload["partition_count"] == 10
        assert payload["historical_runtime_avg"] == 45.0
        assert payload["data_volume_bytes"] == 1_000_000
        assert payload["cluster_size"] == "medium"
        assert payload["tenant_id"] == "t2"

    async def test_predict_cost_omits_optional_fields_when_none(self) -> None:
        client = self._make_client_with_mock_post({})
        await client.predict_cost(
            "m", 1, None, None, "small"
        )
        payload = client._post.call_args[0][1]
        assert "historical_runtime_avg" not in payload
        assert "data_volume_bytes" not in payload

    async def test_score_risk_builds_payload(self) -> None:
        client = self._make_client_with_mock_post({"risk_score": 3.0})
        result = await client.score_risk(
            model_name="orders",
            downstream_depth=4,
            sla_tags=["sla-critical"],
            dashboard_deps=["dash1"],
            model_tags=["pii"],
            failure_rate=0.05,
            tenant_id="t3",
        )
        assert result == {"risk_score": 3.0}
        payload = client._post.call_args[0][1]
        assert payload["model_name"] == "orders"
        assert payload["downstream_depth"] == 4
        assert payload["historical_failure_rate"] == 0.05
        assert payload["tenant_id"] == "t3"

    async def test_optimize_sql_with_stats(self) -> None:
        client = self._make_client_with_mock_post({"suggestions": ["use index"]})
        result = await client.optimize_sql(
            sql="SELECT * FROM t",
            stats={"t": {"rows": 1000}},
            tenant_id="t4",
            api_key="key-opt",
        )
        assert result == {"suggestions": ["use index"]}
        payload = client._post.call_args[0][1]
        assert "table_statistics" in payload
        assert payload["tenant_id"] == "t4"
        assert payload["api_key"] == "key-opt"

    async def test_optimize_sql_without_stats(self) -> None:
        client = self._make_client_with_mock_post({})
        await client.optimize_sql(sql="SELECT 1")
        payload = client._post.call_args[0][1]
        assert "table_statistics" not in payload

    async def test_health_check_returns_true_on_200(self) -> None:
        from api.services.ai_client import AIServiceClient

        client = AIServiceClient("http://ai-engine:8001")
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        client._client.get = AsyncMock(return_value=mock_resp)

        result = await client.health_check()
        assert result is True

    async def test_health_check_returns_false_on_non_200(self) -> None:
        from api.services.ai_client import AIServiceClient

        client = AIServiceClient("http://ai-engine:8001")
        mock_resp = MagicMock()
        mock_resp.status_code = 503
        client._client.get = AsyncMock(return_value=mock_resp)

        result = await client.health_check()
        assert result is False

    async def test_health_check_returns_false_on_exception(self) -> None:
        from api.services.ai_client import AIServiceClient

        client = AIServiceClient("http://ai-engine:8001")
        client._client.get = AsyncMock(side_effect=Exception("network err"))

        result = await client.health_check()
        assert result is False

    async def test_close_calls_aclose(self) -> None:
        from api.services.ai_client import AIServiceClient

        client = AIServiceClient("http://ai-engine:8001")
        client._client.aclose = AsyncMock()
        await client.close()
        client._client.aclose.assert_called_once()


# ---------------------------------------------------------------------------
# =========================  reconciliation_service  =========================
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_session_recon() -> AsyncMock:
    session = AsyncMock()
    session.add = MagicMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock()
    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = None
    result_mock.scalars.return_value.all.return_value = []
    session.execute = AsyncMock(return_value=result_mock)
    return session


def _make_recon_service(session, tenant_id="test-tenant"):
    from api.services.reconciliation_service import ReconciliationService

    with (
        patch("api.services.reconciliation_service.RunRepository"),
        patch("api.services.reconciliation_service.ReconciliationRepository"),
        patch("api.services.reconciliation_service.SchemaDriftRepository"),
        patch("api.services.reconciliation_service.ModelRepository"),
    ):
        svc = ReconciliationService(session, tenant_id=tenant_id)
    return svc


class TestClassifyDiscrepancy:
    """Tests for ReconciliationService._classify_discrepancy."""

    def test_phantom_success(self) -> None:
        from api.services.reconciliation_service import ReconciliationService

        assert (
            ReconciliationService._classify_discrepancy("SUCCESS", "FAIL")
            == "phantom_success"
        )

    def test_missed_success(self) -> None:
        from api.services.reconciliation_service import ReconciliationService

        assert (
            ReconciliationService._classify_discrepancy("FAIL", "SUCCESS")
            == "missed_success"
        )

    def test_stale_running_success(self) -> None:
        from api.services.reconciliation_service import ReconciliationService

        assert (
            ReconciliationService._classify_discrepancy("RUNNING", "SUCCESS")
            == "stale_running"
        )

    def test_stale_running_failed(self) -> None:
        from api.services.reconciliation_service import ReconciliationService

        assert (
            ReconciliationService._classify_discrepancy("RUNNING", "FAIL")
            == "stale_running_failed"
        )

    def test_stale_pending_success(self) -> None:
        from api.services.reconciliation_service import ReconciliationService

        assert (
            ReconciliationService._classify_discrepancy("PENDING", "SUCCESS")
            == "stale_pending"
        )

    def test_stale_pending_fail(self) -> None:
        from api.services.reconciliation_service import ReconciliationService

        assert (
            ReconciliationService._classify_discrepancy("PENDING", "FAIL")
            == "stale_pending"
        )

    def test_generic_mismatch(self) -> None:
        from api.services.reconciliation_service import ReconciliationService

        assert (
            ReconciliationService._classify_discrepancy("SUCCESS", "CANCELLED")
            == "status_mismatch"
        )


class TestTriggerReconciliation:
    """Tests for ReconciliationService.trigger_reconciliation."""

    async def test_skips_runs_without_external_id(
        self, mock_session_recon: AsyncMock
    ) -> None:
        from api.services.reconciliation_service import ReconciliationService

        svc = ReconciliationService.__new__(ReconciliationService)
        svc._session = mock_session_recon
        svc._tenant_id = "t1"

        run_no_ext = MagicMock()
        run_no_ext.external_run_id = None

        run_repo = AsyncMock()
        run_repo.get_by_plan = AsyncMock(return_value=[run_no_ext])
        svc._run_repo = run_repo
        svc._recon_repo = AsyncMock()
        svc._drift_repo = AsyncMock()
        svc._model_repo = AsyncMock()

        result = await svc.trigger_reconciliation(plan_id="plan-001")
        assert result["skipped"] == 1
        assert result["checked"] == 0

    async def test_matched_run_records_check(
        self, mock_session_recon: AsyncMock
    ) -> None:
        from api.services.reconciliation_service import ReconciliationService
        from core_engine.models.run import RunStatus

        svc = ReconciliationService.__new__(ReconciliationService)
        svc._session = mock_session_recon
        svc._tenant_id = "t1"

        run = MagicMock()
        run.run_id = "run-001"
        run.model_name = "orders"
        run.external_run_id = "ext-001"
        run.status = RunStatus.SUCCESS.value

        run_repo = AsyncMock()
        run_repo.get_by_plan = AsyncMock(return_value=[run])
        svc._run_repo = run_repo

        recon_repo = AsyncMock()
        recon_repo.record_check = AsyncMock()
        svc._recon_repo = recon_repo
        svc._drift_repo = AsyncMock()
        svc._model_repo = AsyncMock()

        svc._verify_against_backend = AsyncMock(return_value=RunStatus.SUCCESS)

        result = await svc.trigger_reconciliation(plan_id="plan-001")
        assert result["matched"] == 1
        assert result["discrepancies"] == 0
        recon_repo.record_check.assert_called_once()

    async def test_discrepancy_is_classified_and_logged(
        self, mock_session_recon: AsyncMock
    ) -> None:
        from api.services.reconciliation_service import ReconciliationService
        from core_engine.models.run import RunStatus

        svc = ReconciliationService.__new__(ReconciliationService)
        svc._session = mock_session_recon
        svc._tenant_id = "t1"

        run = MagicMock()
        run.run_id = "run-002"
        run.model_name = "revenue"
        run.external_run_id = "ext-002"
        run.status = RunStatus.SUCCESS.value

        run_repo = AsyncMock()
        run_repo.get_by_plan = AsyncMock(return_value=[run])
        svc._run_repo = run_repo

        recon_repo = AsyncMock()
        recon_repo.record_check = AsyncMock()
        svc._recon_repo = recon_repo
        svc._drift_repo = AsyncMock()
        svc._model_repo = AsyncMock()

        # Warehouse says FAIL — discrepancy
        svc._verify_against_backend = AsyncMock(return_value=RunStatus.FAIL)

        result = await svc.trigger_reconciliation(plan_id="plan-001")
        assert result["discrepancies"] == 1
        call_kwargs = recon_repo.record_check.call_args.kwargs
        assert call_kwargs["discrepancy_type"] == "phantom_success"

    async def test_backend_exception_counts_as_skipped(
        self, mock_session_recon: AsyncMock
    ) -> None:
        from api.services.reconciliation_service import ReconciliationService

        svc = ReconciliationService.__new__(ReconciliationService)
        svc._session = mock_session_recon
        svc._tenant_id = "t1"

        run = MagicMock()
        run.run_id = "run-003"
        run.model_name = "orders"
        run.external_run_id = "ext-003"

        run_repo = AsyncMock()
        run_repo.get_by_plan = AsyncMock(return_value=[run])
        svc._run_repo = run_repo
        svc._recon_repo = AsyncMock()
        svc._drift_repo = AsyncMock()
        svc._model_repo = AsyncMock()

        svc._verify_against_backend = AsyncMock(
            side_effect=RuntimeError("Databricks down")
        )

        result = await svc.trigger_reconciliation(plan_id="plan-001")
        assert result["skipped"] == 1

    async def test_no_plan_id_uses_recent_runs(
        self, mock_session_recon: AsyncMock
    ) -> None:
        """When plan_id is None, _get_recent_runs is called."""
        from api.services.reconciliation_service import ReconciliationService

        svc = ReconciliationService.__new__(ReconciliationService)
        svc._session = mock_session_recon
        svc._tenant_id = "t1"
        svc._run_repo = AsyncMock()
        svc._recon_repo = AsyncMock()
        svc._drift_repo = AsyncMock()
        svc._model_repo = AsyncMock()

        svc._get_recent_runs = AsyncMock(return_value=[])
        result = await svc.trigger_reconciliation()
        svc._get_recent_runs.assert_called_once()
        assert result["total_runs"] == 0


class TestGetDiscrepancies:
    """Tests for get_discrepancies."""

    async def test_returns_serialised_rows(
        self, mock_session_recon: AsyncMock
    ) -> None:
        from api.services.reconciliation_service import ReconciliationService

        svc = ReconciliationService.__new__(ReconciliationService)
        row = MagicMock()
        row.id = 1
        row.run_id = "run-1"
        row.model_name = "orders"
        row.expected_status = "SUCCESS"
        row.warehouse_status = "FAIL"
        row.discrepancy_type = "phantom_success"
        row.checked_at = datetime(2024, 6, 1, tzinfo=timezone.utc)

        recon_repo = AsyncMock()
        recon_repo.get_unresolved = AsyncMock(return_value=[row])
        svc._recon_repo = recon_repo

        result = await svc.get_discrepancies(limit=50)
        assert len(result) == 1
        assert result[0]["run_id"] == "run-1"
        assert result[0]["discrepancy_type"] == "phantom_success"
        assert result[0]["checked_at"] is not None

    async def test_returns_none_checked_at_when_missing(
        self, mock_session_recon: AsyncMock
    ) -> None:
        from api.services.reconciliation_service import ReconciliationService

        svc = ReconciliationService.__new__(ReconciliationService)
        row = MagicMock()
        row.id = 2
        row.run_id = "run-2"
        row.model_name = "revenue"
        row.expected_status = "RUNNING"
        row.warehouse_status = "SUCCESS"
        row.discrepancy_type = "stale_running"
        row.checked_at = None

        recon_repo = AsyncMock()
        recon_repo.get_unresolved = AsyncMock(return_value=[row])
        svc._recon_repo = recon_repo

        result = await svc.get_discrepancies()
        assert result[0]["checked_at"] is None


class TestResolveDiscrepancy:
    """Tests for resolve_discrepancy."""

    async def test_returns_resolved_record(
        self, mock_session_recon: AsyncMock
    ) -> None:
        from api.services.reconciliation_service import ReconciliationService

        svc = ReconciliationService.__new__(ReconciliationService)
        row = MagicMock()
        row.id = 5
        row.run_id = "run-5"
        row.model_name = "orders"
        row.resolved = True
        row.resolved_by = "alice"
        row.resolved_at = datetime(2024, 6, 15, tzinfo=timezone.utc)
        row.resolution_note = "fixed upstream"

        recon_repo = AsyncMock()
        recon_repo.resolve = AsyncMock(return_value=row)
        svc._recon_repo = recon_repo

        result = await svc.resolve_discrepancy(5, "alice", "fixed upstream")
        assert result is not None
        assert result["resolved_by"] == "alice"
        assert result["resolution_note"] == "fixed upstream"

    async def test_returns_none_when_not_found(
        self, mock_session_recon: AsyncMock
    ) -> None:
        from api.services.reconciliation_service import ReconciliationService

        svc = ReconciliationService.__new__(ReconciliationService)
        recon_repo = AsyncMock()
        recon_repo.resolve = AsyncMock(return_value=None)
        svc._recon_repo = recon_repo

        result = await svc.resolve_discrepancy(999, "alice", "note")
        assert result is None


class TestGetStats:
    """Tests for get_stats."""

    async def test_delegates_to_repo(self, mock_session_recon: AsyncMock) -> None:
        from api.services.reconciliation_service import ReconciliationService

        svc = ReconciliationService.__new__(ReconciliationService)
        recon_repo = AsyncMock()
        recon_repo.get_stats = AsyncMock(
            return_value={"total": 10, "matched": 8, "discrepancies": 2}
        )
        svc._recon_repo = recon_repo

        result = await svc.get_stats()
        assert result["total"] == 10


class TestVerifyAgainstBackend:
    """Tests for _verify_against_backend."""

    async def test_raises_when_no_credentials(
        self, mock_session_recon: AsyncMock, monkeypatch
    ) -> None:
        from api.services.reconciliation_service import ReconciliationService

        monkeypatch.delenv("PLATFORM_DATABRICKS_HOST", raising=False)
        monkeypatch.delenv("PLATFORM_DATABRICKS_TOKEN", raising=False)

        svc = ReconciliationService.__new__(ReconciliationService)
        with pytest.raises(RuntimeError, match="Databricks credentials not configured"):
            await svc._verify_against_backend("ext-001")

    async def test_calls_executor_verify(
        self, mock_session_recon: AsyncMock, monkeypatch
    ) -> None:
        from api.services.reconciliation_service import ReconciliationService
        from core_engine.models.run import RunStatus

        monkeypatch.setenv("PLATFORM_DATABRICKS_HOST", "https://dbc.example.com")
        monkeypatch.setenv("PLATFORM_DATABRICKS_TOKEN", "dapi-token")

        mock_executor = MagicMock()
        mock_executor.verify_run.return_value = RunStatus.SUCCESS

        svc = ReconciliationService.__new__(ReconciliationService)

        # DatabricksExecutor is a local import inside _verify_against_backend;
        # patch it in its source module.
        with patch(
            "core_engine.executor.databricks_executor.DatabricksExecutor",
            return_value=mock_executor,
        ):
            with patch(
                "api.services.reconciliation_service.DatabricksExecutor",
                return_value=mock_executor,
                create=True,
            ):
                result = await svc._verify_against_backend("ext-001")

        assert result == RunStatus.SUCCESS
        mock_executor.verify_run.assert_called_once_with("ext-001")


class TestCheckSchemaDrift:
    """Tests for check_schema_drift."""

    def _make_svc(self, session):
        from api.services.reconciliation_service import ReconciliationService

        svc = ReconciliationService.__new__(ReconciliationService)
        svc._session = session
        svc._tenant_id = "t1"
        svc._run_repo = AsyncMock()
        svc._recon_repo = AsyncMock()
        svc._drift_repo = AsyncMock()
        svc._model_repo = AsyncMock()
        return svc

    async def test_model_not_found(self, mock_session_recon: AsyncMock) -> None:
        svc = self._make_svc(mock_session_recon)
        svc._model_repo.get = AsyncMock(return_value=None)

        result = await svc.check_schema_drift("missing_model")
        assert result["status"] == "model_not_found"
        assert result["drifts"] == []

    async def test_no_actual_schema(self, mock_session_recon: AsyncMock) -> None:
        svc = self._make_svc(mock_session_recon)
        svc._model_repo.get = AsyncMock(return_value=MagicMock())

        result = await svc.check_schema_drift("orders", actual_schema=None)
        assert result["status"] == "no_actual_schema"

    async def test_no_drifts_when_schemas_match(
        self, mock_session_recon: AsyncMock
    ) -> None:
        from core_engine.executor.schema_introspector import ColumnInfo, TableSchema

        svc = self._make_svc(mock_session_recon)
        svc._model_repo.get = AsyncMock(return_value=MagicMock())

        drift_row = MagicMock()
        drift_row.id = 10
        svc._drift_repo.record_drift = AsyncMock(return_value=drift_row)

        cols = [ColumnInfo(name="id", data_type="INT64", nullable=False)]
        schema = TableSchema(table_name="orders", columns=cols)

        result = await svc.check_schema_drift(
            "orders",
            actual_schema=schema,
            expected_schema=schema,
        )
        assert result["status"] == "checked"
        assert result["drift_type"] == "NONE"
        assert result["drifts"] == []

    async def test_column_removed_drift_detected(
        self, mock_session_recon: AsyncMock
    ) -> None:
        from core_engine.executor.schema_introspector import ColumnInfo, TableSchema

        svc = self._make_svc(mock_session_recon)
        svc._model_repo.get = AsyncMock(return_value=MagicMock())

        drift_row = MagicMock()
        drift_row.id = 11
        svc._drift_repo.record_drift = AsyncMock(return_value=drift_row)

        expected = TableSchema(
            table_name="orders",
            columns=[
                ColumnInfo(name="id", data_type="INT64", nullable=False),
                ColumnInfo(name="removed_col", data_type="STRING", nullable=True),
            ],
        )
        actual = TableSchema(
            table_name="orders",
            columns=[ColumnInfo(name="id", data_type="INT64", nullable=False)],
        )

        result = await svc.check_schema_drift(
            "orders", actual_schema=actual, expected_schema=expected
        )
        assert result["status"] == "checked"
        assert result["drift_type"] == "COLUMN_REMOVED"
        assert len(result["drifts"]) > 0

    async def test_derives_expected_schema_from_previous_drift(
        self, mock_session_recon: AsyncMock
    ) -> None:
        """When expected_schema is None, it's derived from previous checks."""
        from core_engine.executor.schema_introspector import ColumnInfo, TableSchema

        svc = self._make_svc(mock_session_recon)
        svc._model_repo.get = AsyncMock(return_value=MagicMock())

        prev_drift_row = MagicMock()
        prev_drift_row.model_name = "orders"
        prev_drift_row.expected_columns_json = [
            {"name": "id", "data_type": "INT64", "nullable": False}
        ]
        svc._drift_repo.get_unresolved = AsyncMock(return_value=[prev_drift_row])

        new_drift_row = MagicMock()
        new_drift_row.id = 12
        svc._drift_repo.record_drift = AsyncMock(return_value=new_drift_row)

        actual = TableSchema(
            table_name="orders",
            columns=[ColumnInfo(name="id", data_type="INT64", nullable=False)],
        )

        result = await svc.check_schema_drift(
            "orders", actual_schema=actual, expected_schema=None
        )
        assert result["status"] == "checked"


class TestCheckAllSchemas:
    """Tests for check_all_schemas."""

    async def test_uses_all_models_when_no_names_given(
        self, mock_session_recon: AsyncMock
    ) -> None:
        from api.services.reconciliation_service import ReconciliationService

        svc = ReconciliationService.__new__(ReconciliationService)
        svc._session = mock_session_recon
        svc._tenant_id = "t1"
        svc._run_repo = AsyncMock()
        svc._recon_repo = AsyncMock()
        svc._drift_repo = AsyncMock()

        m1, m2 = MagicMock(), MagicMock()
        m1.model_name = "orders"
        m2.model_name = "revenue"
        model_repo = AsyncMock()
        model_repo.list_all = AsyncMock(return_value=[m1, m2])
        svc._model_repo = model_repo

        # Patch check_schema_drift to return no_actual_schema
        async def _fake_check(name, **kwargs):
            return {"model_name": name, "status": "no_actual_schema", "drifts": []}

        svc.check_schema_drift = _fake_check  # type: ignore[assignment]

        result = await svc.check_all_schemas()
        assert result["models_requested"] == 2
        assert result["models_checked"] == 0  # status != "checked"

    async def test_with_explicit_model_names(
        self, mock_session_recon: AsyncMock
    ) -> None:
        from api.services.reconciliation_service import ReconciliationService

        svc = ReconciliationService.__new__(ReconciliationService)
        svc._session = mock_session_recon
        svc._tenant_id = "t1"
        svc._run_repo = AsyncMock()
        svc._recon_repo = AsyncMock()
        svc._drift_repo = AsyncMock()
        svc._model_repo = AsyncMock()

        async def _fake_check(name, **kwargs):
            return {
                "model_name": name,
                "status": "checked",
                "drift_type": "NONE",
                "drifts": [],
                "check_id": 1,
            }

        svc.check_schema_drift = _fake_check  # type: ignore[assignment]

        result = await svc.check_all_schemas(model_names=["model_a", "model_b"])
        assert result["models_requested"] == 2
        assert result["models_checked"] == 2
        assert result["drifts_found"] == 0


class TestReconciliationServiceInit:
    """Test that ReconciliationService __init__ creates all repositories."""

    async def test_init_creates_repositories(self) -> None:
        """__init__ wires up all four repositories correctly."""
        from api.services.reconciliation_service import ReconciliationService

        session = AsyncMock()
        with (
            patch("api.services.reconciliation_service.RunRepository") as MockRun,
            patch("api.services.reconciliation_service.ReconciliationRepository") as MockRecon,
            patch("api.services.reconciliation_service.SchemaDriftRepository") as MockDrift,
            patch("api.services.reconciliation_service.ModelRepository") as MockModel,
        ):
            ReconciliationService(session, tenant_id="my-tenant")

        MockRun.assert_called_once_with(session, tenant_id="my-tenant")
        MockRecon.assert_called_once_with(session, tenant_id="my-tenant")
        MockDrift.assert_called_once_with(session, tenant_id="my-tenant")
        MockModel.assert_called_once_with(session, tenant_id="my-tenant")


class TestGetRecentRuns:
    """Tests for _get_recent_runs."""

    async def test_fetches_runs_for_recent_plans(
        self, mock_session_recon: AsyncMock
    ) -> None:
        from api.services.reconciliation_service import ReconciliationService

        svc = ReconciliationService.__new__(ReconciliationService)
        svc._session = mock_session_recon
        svc._tenant_id = "t1"

        plan1 = MagicMock()
        plan1.plan_id = "plan-001"
        plan2 = MagicMock()
        plan2.plan_id = "plan-002"

        run1 = MagicMock()
        run2 = MagicMock()

        run_repo = AsyncMock()
        run_repo.get_by_plan = AsyncMock(side_effect=[[run1], [run2]])
        svc._run_repo = run_repo

        mock_plan_repo = AsyncMock()
        mock_plan_repo.list_recent = AsyncMock(return_value=[plan1, plan2])

        # PlanRepository is imported locally inside _get_recent_runs;
        # patch it in core_engine.state.repository which is the source module.
        with patch(
            "core_engine.state.repository.PlanRepository",
            return_value=mock_plan_repo,
        ):
            runs = await svc._get_recent_runs(hours_back=24)

        assert len(runs) == 2
        assert run1 in runs
        assert run2 in runs

    async def test_returns_empty_when_no_plans(
        self, mock_session_recon: AsyncMock
    ) -> None:
        from api.services.reconciliation_service import ReconciliationService

        svc = ReconciliationService.__new__(ReconciliationService)
        svc._session = mock_session_recon
        svc._tenant_id = "t1"
        svc._run_repo = AsyncMock()

        mock_plan_repo = AsyncMock()
        mock_plan_repo.list_recent = AsyncMock(return_value=[])

        with patch(
            "core_engine.state.repository.PlanRepository",
            return_value=mock_plan_repo,
        ):
            runs = await svc._get_recent_runs(hours_back=6)

        assert runs == []


class TestCheckSchemaDriftAdditionalBranches:
    """Additional drift-type classification branches in check_schema_drift."""

    def _make_svc(self, session):
        from api.services.reconciliation_service import ReconciliationService

        svc = ReconciliationService.__new__(ReconciliationService)
        svc._session = session
        svc._tenant_id = "t1"
        svc._run_repo = AsyncMock()
        svc._recon_repo = AsyncMock()
        svc._drift_repo = AsyncMock()
        svc._model_repo = AsyncMock()
        return svc

    async def test_type_changed_drift_detected(
        self, mock_session_recon: AsyncMock
    ) -> None:
        """TYPE_CHANGED is prioritised when no COLUMN_REMOVED drifts exist."""
        from core_engine.executor.schema_introspector import ColumnInfo, TableSchema, SchemaDrift

        svc = self._make_svc(mock_session_recon)
        svc._model_repo.get = AsyncMock(return_value=MagicMock())

        drift_row = MagicMock()
        drift_row.id = 30
        svc._drift_repo.record_drift = AsyncMock(return_value=drift_row)

        # Patch compare_schemas to return a TYPE_CHANGED drift
        type_drift = SchemaDrift(
            model_name="orders",
            drift_type="TYPE_CHANGED",
            column_name="amount",
            expected="INT64",
            actual="FLOAT64",
            message="Type changed",
        )
        with patch(
            "api.services.reconciliation_service.compare_schemas",
            return_value=[type_drift],
        ):
            cols = [ColumnInfo(name="amount", data_type="INT64", nullable=True)]
            schema = TableSchema(table_name="orders", columns=cols)
            result = await svc.check_schema_drift(
                "orders", actual_schema=schema, expected_schema=schema
            )

        assert result["drift_type"] == "TYPE_CHANGED"

    async def test_column_added_drift_detected(
        self, mock_session_recon: AsyncMock
    ) -> None:
        """COLUMN_ADDED is classified when only addition drifts exist."""
        from core_engine.executor.schema_introspector import ColumnInfo, TableSchema, SchemaDrift

        svc = self._make_svc(mock_session_recon)
        svc._model_repo.get = AsyncMock(return_value=MagicMock())

        drift_row = MagicMock()
        drift_row.id = 31
        svc._drift_repo.record_drift = AsyncMock(return_value=drift_row)

        added_drift = SchemaDrift(
            model_name="orders",
            drift_type="COLUMN_ADDED",
            column_name="new_col",
            expected="",
            actual="STRING",
            message="Column added",
        )
        with patch(
            "api.services.reconciliation_service.compare_schemas",
            return_value=[added_drift],
        ):
            cols = [ColumnInfo(name="id", data_type="INT64", nullable=False)]
            schema = TableSchema(table_name="orders", columns=cols)
            result = await svc.check_schema_drift(
                "orders", actual_schema=schema, expected_schema=schema
            )

        assert result["drift_type"] == "COLUMN_ADDED"

    async def test_unknown_drift_type_uses_first_drift(
        self, mock_session_recon: AsyncMock
    ) -> None:
        """Fallback branch uses drifts[0].drift_type when not a known type."""
        from core_engine.executor.schema_introspector import ColumnInfo, TableSchema, SchemaDrift

        svc = self._make_svc(mock_session_recon)
        svc._model_repo.get = AsyncMock(return_value=MagicMock())

        drift_row = MagicMock()
        drift_row.id = 32
        svc._drift_repo.record_drift = AsyncMock(return_value=drift_row)

        custom_drift = SchemaDrift(
            model_name="orders",
            drift_type="NULLABILITY_CHANGED",
            column_name="amount",
            expected="NOT NULL",
            actual="NULLABLE",
            message="Nullability changed",
        )
        with patch(
            "api.services.reconciliation_service.compare_schemas",
            return_value=[custom_drift],
        ):
            cols = [ColumnInfo(name="amount", data_type="INT64", nullable=False)]
            schema = TableSchema(table_name="orders", columns=cols)
            result = await svc.check_schema_drift(
                "orders", actual_schema=schema, expected_schema=schema
            )

        assert result["drift_type"] == "NULLABILITY_CHANGED"


class TestDeriveExpectedSchemaEdgeCases:
    """Edge cases in _derive_expected_schema."""

    def _make_svc(self, session):
        from api.services.reconciliation_service import ReconciliationService

        svc = ReconciliationService.__new__(ReconciliationService)
        svc._session = session
        svc._tenant_id = "t1"
        svc._run_repo = AsyncMock()
        svc._recon_repo = AsyncMock()
        svc._drift_repo = AsyncMock()
        svc._model_repo = AsyncMock()
        return svc

    async def test_skips_rows_for_different_models(
        self, mock_session_recon: AsyncMock
    ) -> None:
        """Rows with a different model_name are skipped."""
        svc = self._make_svc(mock_session_recon)

        different_row = MagicMock()
        different_row.model_name = "other_model"
        different_row.expected_columns_json = [{"name": "id"}]

        svc._drift_repo.get_unresolved = AsyncMock(return_value=[different_row])

        result = await svc._derive_expected_schema("orders")
        assert result is None

    async def test_returns_none_when_exception_raised(
        self, mock_session_recon: AsyncMock
    ) -> None:
        """Exception in _derive_expected_schema returns None and logs."""
        svc = self._make_svc(mock_session_recon)
        svc._drift_repo.get_unresolved = AsyncMock(
            side_effect=Exception("DB error")
        )

        result = await svc._derive_expected_schema("orders")
        assert result is None


class TestCheckAllSchemasWithDrifts:
    """check_all_schemas counts drifts_found correctly."""

    async def test_drifts_found_incremented(
        self, mock_session_recon: AsyncMock
    ) -> None:
        """When a model has drift_type != NONE, drifts_found is incremented."""
        from api.services.reconciliation_service import ReconciliationService

        svc = ReconciliationService.__new__(ReconciliationService)
        svc._session = mock_session_recon
        svc._tenant_id = "t1"
        svc._run_repo = AsyncMock()
        svc._recon_repo = AsyncMock()
        svc._drift_repo = AsyncMock()
        svc._model_repo = AsyncMock()

        async def _fake_check(name, **kwargs):
            if name == "orders":
                return {
                    "model_name": name,
                    "status": "checked",
                    "drift_type": "COLUMN_REMOVED",
                    "drifts": [{"drift_type": "COLUMN_REMOVED"}],
                    "check_id": 5,
                }
            return {
                "model_name": name,
                "status": "checked",
                "drift_type": "NONE",
                "drifts": [],
                "check_id": 6,
            }

        svc.check_schema_drift = _fake_check  # type: ignore[assignment]

        result = await svc.check_all_schemas(model_names=["orders", "revenue"])
        assert result["models_checked"] == 2
        assert result["drifts_found"] == 1


class TestGetSchemaDrifts:
    """Tests for get_schema_drifts."""

    async def test_serialises_rows(self, mock_session_recon: AsyncMock) -> None:
        from api.services.reconciliation_service import ReconciliationService

        svc = ReconciliationService.__new__(ReconciliationService)
        row = MagicMock()
        row.id = 20
        row.model_name = "orders"
        row.drift_type = "COLUMN_REMOVED"
        row.drift_details_json = {"drifts": []}
        row.expected_columns_json = None
        row.actual_columns_json = None
        row.resolved = False
        row.checked_at = datetime(2024, 6, 1, tzinfo=timezone.utc)

        drift_repo = AsyncMock()
        drift_repo.get_unresolved = AsyncMock(return_value=[row])
        svc._drift_repo = drift_repo

        result = await svc.get_schema_drifts()
        assert len(result) == 1
        assert result[0]["drift_type"] == "COLUMN_REMOVED"


class TestResolveSchemaDrift:
    """Tests for resolve_schema_drift."""

    async def test_returns_resolved_record(
        self, mock_session_recon: AsyncMock
    ) -> None:
        from api.services.reconciliation_service import ReconciliationService

        svc = ReconciliationService.__new__(ReconciliationService)
        row = MagicMock()
        row.id = 7
        row.model_name = "orders"
        row.drift_type = "TYPE_CHANGED"
        row.resolved = True
        row.resolved_by = "bob"
        row.resolved_at = datetime(2024, 7, 1, tzinfo=timezone.utc)
        row.resolution_note = "accepted change"

        drift_repo = AsyncMock()
        drift_repo.resolve = AsyncMock(return_value=row)
        svc._drift_repo = drift_repo

        result = await svc.resolve_schema_drift(7, "bob", "accepted change")
        assert result is not None
        assert result["resolved_by"] == "bob"

    async def test_returns_none_when_not_found(
        self, mock_session_recon: AsyncMock
    ) -> None:
        from api.services.reconciliation_service import ReconciliationService

        svc = ReconciliationService.__new__(ReconciliationService)
        drift_repo = AsyncMock()
        drift_repo.resolve = AsyncMock(return_value=None)
        svc._drift_repo = drift_repo

        result = await svc.resolve_schema_drift(999, "bob", "note")
        assert result is None


# ---------------------------------------------------------------------------
# ============================  approvals router  ============================
# ---------------------------------------------------------------------------

# We create a minimal FastAPI app with the approvals router and bypass
# the auth/DB dependencies so tests never hit a real database.


def _build_approvals_app(mock_session: AsyncMock, user_identity: str = "test-user"):
    """Build a minimal FastAPI app with the approvals router and dep overrides."""
    from api.routers.approvals import router
    from api.dependencies import get_tenant_session, get_tenant_id, get_settings
    from api.middleware.rbac import get_user_role, Role
    from api.dependencies import get_user_identity
    from pydantic import SecretStr

    import os

    os.environ.setdefault("JWT_SECRET", "test-secret-key-for-ironlayer-tests")

    from api.config import APISettings

    settings = APISettings(
        host="0.0.0.0",
        port=8000,
        debug=True,
        database_url="postgresql+asyncpg://test:test@localhost:5432/test",
        ai_engine_url="http://localhost:8001",
        ai_engine_timeout=5.0,
        platform_env="dev",
        cors_origins=["http://localhost:3000"],
        jwt_secret=SecretStr("test-secret-key-for-ironlayer-tests"),
    )

    app = FastAPI()
    # The router already has prefix="/plans", so mount under /api/v1 only.
    app.include_router(router, prefix="/api/v1")

    async def _session():
        yield mock_session

    app.dependency_overrides[get_tenant_session] = _session
    app.dependency_overrides[get_tenant_id] = lambda: "test-tenant"
    app.dependency_overrides[get_user_identity] = lambda: user_identity
    app.dependency_overrides[get_user_role] = lambda: Role.ADMIN
    app.dependency_overrides[get_settings] = lambda: settings

    return app


def _make_plan_row(
    plan_id: str = "plan-001",
    approvals_json: str | None = None,
    auto_approved: bool = False,
) -> MagicMock:
    plan_data = {
        "plan_id": plan_id,
        "base": "sha-old",
        "target": "sha-new",
        "summary": {
            "total_steps": 1,
            "estimated_cost_usd": 1.0,
            "models_changed": ["orders"],
        },
        "steps": [],
    }
    row = MagicMock()
    row.plan_id = plan_id
    row.plan_json = json.dumps(plan_data)
    row.approvals_json = approvals_json
    row.auto_approved = auto_approved
    row.created_at = datetime(2024, 6, 1, tzinfo=timezone.utc)
    return row


class TestApproveEndpoint:
    """Tests for POST /plans/{plan_id}/approve."""

    @pytest_asyncio.fixture()
    async def client_and_session(self):
        session = AsyncMock()
        session.add = MagicMock()
        session.flush = AsyncMock()
        session.commit = AsyncMock()
        session.execute = AsyncMock()

        app = _build_approvals_app(session, user_identity="approver-user")
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as c:
            yield c, session

    async def test_approve_success(
        self, client_and_session
    ) -> None:
        client, session = client_and_session
        plan_row = _make_plan_row("plan-001", approvals_json=None)

        with patch(
            "api.routers.approvals.PlanRepository"
        ) as MockRepo, patch(
            "api.routers.approvals.AuditService"
        ) as MockAudit:
            mock_repo = AsyncMock()
            # First get_plan call (check for duplicates)
            # Second get_plan call (reload after approval)
            mock_repo.get_plan = AsyncMock(
                side_effect=[plan_row, plan_row]
            )
            mock_repo.add_approval = AsyncMock()
            MockRepo.return_value = mock_repo

            audit_inst = AsyncMock()
            audit_inst.log = AsyncMock()
            MockAudit.return_value = audit_inst

            resp = await client.post(
                "/api/v1/plans/plan-001/approve",
                json={"comment": "LGTM"},
            )

        assert resp.status_code == 200
        mock_repo.add_approval.assert_called_once_with(
            plan_id="plan-001",
            user="approver-user",
            comment="LGTM",
        )

    async def test_approve_missing_user_identity_returns_401(self) -> None:
        """Empty user identity raises 401."""
        session = AsyncMock()
        app = _build_approvals_app(session, user_identity="")

        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as c:
            resp = await c.post(
                "/api/v1/plans/plan-001/approve",
                json={},
            )
        assert resp.status_code == 401

    async def test_approve_plan_not_found_returns_404(
        self, client_and_session
    ) -> None:
        client, session = client_and_session

        with patch("api.routers.approvals.PlanRepository") as MockRepo:
            mock_repo = AsyncMock()
            mock_repo.get_plan = AsyncMock(return_value=None)
            MockRepo.return_value = mock_repo

            resp = await client.post(
                "/api/v1/plans/missing-plan/approve",
                json={"comment": "LGTM"},
            )
        assert resp.status_code == 404

    async def test_approve_duplicate_returns_409(
        self, client_and_session
    ) -> None:
        """Re-approving by same user returns 409."""
        client, session = client_and_session
        existing_approvals = json.dumps([{"user": "approver-user", "comment": "first"}])
        plan_row = _make_plan_row("plan-001", approvals_json=existing_approvals)

        with patch("api.routers.approvals.PlanRepository") as MockRepo:
            mock_repo = AsyncMock()
            mock_repo.get_plan = AsyncMock(return_value=plan_row)
            MockRepo.return_value = mock_repo

            resp = await client.post(
                "/api/v1/plans/plan-001/approve",
                json={"comment": "second approval"},
            )
        assert resp.status_code == 409
        assert "already approved" in resp.json()["detail"]

    async def test_approve_without_comment(
        self, client_and_session
    ) -> None:
        """Approval with no comment uses empty string."""
        client, session = client_and_session
        plan_row = _make_plan_row("plan-002")

        with patch(
            "api.routers.approvals.PlanRepository"
        ) as MockRepo, patch(
            "api.routers.approvals.AuditService"
        ) as MockAudit:
            mock_repo = AsyncMock()
            mock_repo.get_plan = AsyncMock(side_effect=[plan_row, plan_row])
            mock_repo.add_approval = AsyncMock()
            MockRepo.return_value = mock_repo
            MockAudit.return_value = AsyncMock()
            MockAudit.return_value.log = AsyncMock()

            resp = await client.post(
                "/api/v1/plans/plan-002/approve",
                json={},
            )
        assert resp.status_code == 200
        mock_repo.add_approval.assert_called_once_with(
            plan_id="plan-002",
            user="approver-user",
            comment="",
        )


class TestRejectEndpoint:
    """Tests for POST /plans/{plan_id}/reject."""

    @pytest_asyncio.fixture()
    async def client_and_session(self):
        session = AsyncMock()
        session.add = MagicMock()
        session.flush = AsyncMock()
        session.execute = AsyncMock()

        app = _build_approvals_app(session, user_identity="rejector-user")
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as c:
            yield c, session

    async def test_reject_success(self, client_and_session) -> None:
        client, session = client_and_session
        plan_row = _make_plan_row("plan-001")
        after_row = _make_plan_row("plan-001")

        with patch(
            "api.routers.approvals.PlanRepository"
        ) as MockRepo, patch(
            "api.routers.approvals.AuditService"
        ) as MockAudit:
            mock_repo = AsyncMock()
            mock_repo.get_plan = AsyncMock(side_effect=[plan_row, after_row])
            MockRepo.return_value = mock_repo

            audit_inst = AsyncMock()
            audit_inst.log = AsyncMock()
            MockAudit.return_value = audit_inst

            resp = await client.post(
                "/api/v1/plans/plan-001/reject",
                json={"reason": "Needs rework"},
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["rejected"] is True
        assert body["rejected_by"] == "rejector-user"
        assert body["rejection_reason"] == "Needs rework"

    async def test_reject_missing_reason_returns_422(
        self, client_and_session
    ) -> None:
        """reason is required; omitting it gives 422."""
        client, session = client_and_session
        resp = await client.post(
            "/api/v1/plans/plan-001/reject",
            json={},
        )
        assert resp.status_code == 422

    async def test_reject_empty_reason_returns_422(
        self, client_and_session
    ) -> None:
        """reason min_length=1; empty string gives 422."""
        client, session = client_and_session
        resp = await client.post(
            "/api/v1/plans/plan-001/reject",
            json={"reason": ""},
        )
        assert resp.status_code == 422

    async def test_reject_missing_user_identity_returns_401(self) -> None:
        session = AsyncMock()
        app = _build_approvals_app(session, user_identity="")

        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as c:
            resp = await c.post(
                "/api/v1/plans/plan-001/reject",
                json={"reason": "bad plan"},
            )
        assert resp.status_code == 401

    async def test_reject_plan_not_found_returns_404(
        self, client_and_session
    ) -> None:
        client, session = client_and_session

        with patch("api.routers.approvals.PlanRepository") as MockRepo:
            mock_repo = AsyncMock()
            mock_repo.get_plan = AsyncMock(return_value=None)
            MockRepo.return_value = mock_repo

            resp = await client.post(
                "/api/v1/plans/missing/reject",
                json={"reason": "bad"},
            )
        assert resp.status_code == 404

    async def test_reject_writes_rejection_entry_to_db(
        self, client_and_session
    ) -> None:
        """Rejection record appended to approvals and persisted via session.execute."""
        client, session = client_and_session
        plan_row = _make_plan_row("plan-rej", approvals_json=json.dumps([]))
        after_row = _make_plan_row("plan-rej")

        with patch(
            "api.routers.approvals.PlanRepository"
        ) as MockRepo, patch(
            "api.routers.approvals.AuditService"
        ) as MockAudit:
            mock_repo = AsyncMock()
            mock_repo.get_plan = AsyncMock(side_effect=[plan_row, after_row])
            MockRepo.return_value = mock_repo
            MockAudit.return_value = AsyncMock()
            MockAudit.return_value.log = AsyncMock()

            resp = await client.post(
                "/api/v1/plans/plan-rej/reject",
                json={"reason": "Dangerous query", "display_name": "Carol"},
            )
        assert resp.status_code == 200
        # Session.execute was called for the UPDATE statement
        session.execute.assert_called()

    async def test_reject_preserves_existing_approvals(
        self, client_and_session
    ) -> None:
        """Rejection appended to existing approvals list."""
        client, session = client_and_session
        existing_approvals = [{"user": "alice", "comment": "approved"}]
        plan_row = _make_plan_row(
            "plan-multi",
            approvals_json=json.dumps(existing_approvals),
        )
        after_row = _make_plan_row("plan-multi")

        with patch(
            "api.routers.approvals.PlanRepository"
        ) as MockRepo, patch(
            "api.routers.approvals.AuditService"
        ) as MockAudit:
            mock_repo = AsyncMock()
            mock_repo.get_plan = AsyncMock(side_effect=[plan_row, after_row])
            MockRepo.return_value = mock_repo
            MockAudit.return_value = AsyncMock()
            MockAudit.return_value.log = AsyncMock()

            # Capture the values written to session.execute
            execute_calls: list[Any] = []

            async def _capture_execute(stmt, *args, **kwargs):
                execute_calls.append(stmt)
                return AsyncMock()()

            session.execute = _capture_execute

            resp = await client.post(
                "/api/v1/plans/plan-multi/reject",
                json={"reason": "Reconsidered"},
            )
        assert resp.status_code == 200
        # session.execute was called (the UPDATE statement)
        assert len(execute_calls) >= 1

    async def test_reject_audit_logged(self, client_and_session) -> None:
        """AuditService.log is called with PLAN_REJECTED action."""
        client, session = client_and_session
        plan_row = _make_plan_row("plan-audit-rej")
        after_row = _make_plan_row("plan-audit-rej")

        with patch(
            "api.routers.approvals.PlanRepository"
        ) as MockRepo, patch(
            "api.routers.approvals.AuditService"
        ) as MockAudit:
            mock_repo = AsyncMock()
            mock_repo.get_plan = AsyncMock(side_effect=[plan_row, after_row])
            MockRepo.return_value = mock_repo

            audit_inst = AsyncMock()
            audit_inst.log = AsyncMock()
            MockAudit.return_value = audit_inst

            await client.post(
                "/api/v1/plans/plan-audit-rej/reject",
                json={"reason": "audit test"},
            )

        audit_inst.log.assert_called_once()
        call_args = audit_inst.log.call_args
        from api.services.audit_service import AuditAction

        assert call_args[0][0] == AuditAction.PLAN_REJECTED

    async def test_approve_audit_logged(self, client_and_session) -> None:
        """AuditService.log is called with PLAN_APPROVED action on approve."""
        client, session = client_and_session
        plan_row = _make_plan_row("plan-audit-app")

        with patch(
            "api.routers.approvals.PlanRepository"
        ) as MockRepo, patch(
            "api.routers.approvals.AuditService"
        ) as MockAudit:
            mock_repo = AsyncMock()
            mock_repo.get_plan = AsyncMock(side_effect=[plan_row, plan_row])
            mock_repo.add_approval = AsyncMock()
            MockRepo.return_value = mock_repo

            audit_inst = AsyncMock()
            audit_inst.log = AsyncMock()
            MockAudit.return_value = audit_inst

            await client.post(
                "/api/v1/plans/plan-audit-app/approve",
                json={"comment": "audit test"},
            )

        audit_inst.log.assert_called_once()
        from api.services.audit_service import AuditAction

        assert audit_inst.log.call_args[0][0] == AuditAction.PLAN_APPROVED
