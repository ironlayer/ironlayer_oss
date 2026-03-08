"""Tests for per-tenant configuration (F7)."""

from __future__ import annotations

import pytest

from ai_engine.models.requests import (
    CostPredictRequest,
    OptimizeSQLRequest,
    RiskScoreRequest,
    SemanticClassifyRequest,
)


class TestRequestModelsLLMEnabled:
    """Verify llm_enabled and tenant_id fields on request models."""

    def test_semantic_classify_defaults(self) -> None:
        req = SemanticClassifyRequest(old_sql="SELECT 1", new_sql="SELECT 2")
        assert req.llm_enabled is True
        assert req.tenant_id is None

    def test_semantic_classify_opt_out(self) -> None:
        req = SemanticClassifyRequest(
            old_sql="SELECT 1",
            new_sql="SELECT 2",
            llm_enabled=False,
            tenant_id="acme",
        )
        assert req.llm_enabled is False
        assert req.tenant_id == "acme"

    def test_cost_predict_defaults(self) -> None:
        req = CostPredictRequest(
            model_name="test_model",
            partition_count=5,
            cluster_size="small",
        )
        assert req.llm_enabled is True
        assert req.tenant_id is None

    def test_cost_predict_opt_out(self) -> None:
        req = CostPredictRequest(
            model_name="test_model",
            partition_count=5,
            cluster_size="small",
            llm_enabled=False,
            tenant_id="acme",
        )
        assert req.llm_enabled is False

    def test_risk_score_defaults(self) -> None:
        req = RiskScoreRequest(
            model_name="test_model",
            downstream_depth=2,
            historical_failure_rate=0.1,
        )
        assert req.llm_enabled is True
        assert req.tenant_id is None

    def test_optimize_sql_defaults(self) -> None:
        req = OptimizeSQLRequest(sql="SELECT * FROM t")
        assert req.llm_enabled is True
        assert req.tenant_id is None

    def test_optimize_sql_opt_out(self) -> None:
        req = OptimizeSQLRequest(
            sql="SELECT * FROM t",
            llm_enabled=False,
            tenant_id="acme",
        )
        assert req.llm_enabled is False
        assert req.tenant_id == "acme"


class TestLLMClientLLMEnabled:
    """Verify LLMClient respects per-request llm_enabled flag."""

    def test_classify_change_skips_when_disabled(self) -> None:
        """When llm_enabled=False, classify_change returns None."""
        from unittest.mock import MagicMock

        from ai_engine.engines.llm_client import LLMClient

        settings = MagicMock()
        settings.llm_enabled = True
        settings.llm_model = "test"
        settings.llm_max_tokens = 100
        settings.llm_timeout = 5.0
        settings.llm_api_key = None

        client = LLMClient.__new__(LLMClient)
        client._enabled = True
        client._model = "test"
        client._max_tokens = 100
        client._timeout = 5.0
        client._client = MagicMock()

        # Should return None when llm_enabled=False
        result = client.classify_change("old", "new", llm_enabled=False)
        assert result is None

    def test_suggest_optimization_skips_when_disabled(self) -> None:
        """When llm_enabled=False, suggest_optimization returns None."""
        from unittest.mock import MagicMock

        from ai_engine.engines.llm_client import LLMClient

        client = LLMClient.__new__(LLMClient)
        client._enabled = True
        client._model = "test"
        client._max_tokens = 100
        client._timeout = 5.0
        client._client = MagicMock()

        result = client.suggest_optimization("SELECT 1", llm_enabled=False)
        assert result is None


class TestTenantConfigTable:
    """Verify TenantConfigTable ORM definition."""

    def test_table_name(self) -> None:
        from core_engine.state.tables import TenantConfigTable

        assert TenantConfigTable.__tablename__ == "tenant_config"

    def test_primary_key_is_tenant_id(self) -> None:
        from core_engine.state.tables import TenantConfigTable

        pk_cols = [c.name for c in TenantConfigTable.__table__.columns if c.primary_key]
        assert pk_cols == ["tenant_id"]

    def test_llm_enabled_column_exists(self) -> None:
        from core_engine.state.tables import TenantConfigTable

        col_names = [c.name for c in TenantConfigTable.__table__.columns]
        assert "llm_enabled" in col_names
        assert "created_at" in col_names
        assert "updated_at" in col_names
        assert "updated_by" in col_names
        assert "deactivated_at" in col_names


class TestTokenRevocationTable:
    """Verify TokenRevocationTable ORM definition."""

    def test_table_name(self) -> None:
        from core_engine.state.tables import TokenRevocationTable

        assert TokenRevocationTable.__tablename__ == "token_revocations"

    def test_unique_constraint(self) -> None:
        from core_engine.state.tables import TokenRevocationTable

        constraints = [c.name for c in TokenRevocationTable.__table__.constraints if hasattr(c, "name") and c.name]
        assert "uq_token_revocations_tenant_jti" in constraints

    def test_columns(self) -> None:
        from core_engine.state.tables import TokenRevocationTable

        col_names = [c.name for c in TokenRevocationTable.__table__.columns]
        assert "jti" in col_names
        assert "tenant_id" in col_names
        assert "revoked_at" in col_names
        assert "reason" in col_names
        assert "expires_at" in col_names
