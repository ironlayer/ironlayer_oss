"""Tests for token revocation (C2)."""

from __future__ import annotations

import pytest
from pydantic import SecretStr

from api.security import TokenClaims, TokenConfig, TokenManager

_TEST_SECRET = SecretStr("test-secret-key")


class TestJtiOnTokenClaims:
    """Verify jti field on TokenClaims."""

    def test_jti_auto_generated(self) -> None:
        claims = TokenClaims(sub="user1", tenant_id="t1")
        assert claims.jti is not None
        assert isinstance(claims.jti, str)
        assert len(claims.jti) == 32

    def test_jti_unique_per_instance(self) -> None:
        c1 = TokenClaims(sub="user1", tenant_id="t1")
        c2 = TokenClaims(sub="user1", tenant_id="t1")
        assert c1.jti != c2.jti

    def test_jti_explicit_value(self) -> None:
        claims = TokenClaims(sub="user1", tenant_id="t1", jti="custom-jti-123")
        assert claims.jti == "custom-jti-123"


class TestTokenRoundTripWithJti:
    """Verify that jti survives token generation and validation."""

    def test_dev_token_preserves_jti(self) -> None:
        mgr = TokenManager(TokenConfig(jwt_secret=_TEST_SECRET))
        token = mgr.generate_token("user1", "t1")
        claims = mgr.validate_token(token)
        assert claims.jti is not None
        assert len(claims.jti) == 32

    def test_different_tokens_different_jtis(self) -> None:
        mgr = TokenManager(TokenConfig(jwt_secret=_TEST_SECRET))
        t1 = mgr.generate_token("user1", "t1")
        t2 = mgr.generate_token("user1", "t1")
        c1 = mgr.validate_token(t1)
        c2 = mgr.validate_token(t2)
        assert c1.jti != c2.jti


class TestOIDCJtiMapping:
    """Verify OIDC _map_claims extracts jti."""

    def test_oidc_jti_extracted(self) -> None:
        from api.security import OIDCProvider

        payload = {
            "sub": "user1",
            "tenant_id": "t1",
            "jti": "oidc-jti-abc",
            "iat": 1000000,
            "exp": 9999999,
        }
        claims = OIDCProvider._map_claims(payload)
        assert claims.jti == "oidc-jti-abc"

    def test_oidc_jti_missing_generates_uuid(self) -> None:
        from api.security import OIDCProvider

        payload = {
            "sub": "user1",
            "tenant_id": "t1",
            "iat": 1000000,
            "exp": 9999999,
        }
        claims = OIDCProvider._map_claims(payload)
        assert claims.jti is not None
        assert len(claims.jti) == 32

    def test_oidc_identity_kind_extracted(self) -> None:
        from api.security import OIDCProvider

        payload = {
            "sub": "svc1",
            "tenant_id": "t1",
            "identity_kind": "service",
            "iat": 1000000,
            "exp": 9999999,
        }
        claims = OIDCProvider._map_claims(payload)
        assert claims.identity_kind == "service"

    def test_oidc_identity_kind_defaults_to_user(self) -> None:
        from api.security import OIDCProvider

        payload = {
            "sub": "user1",
            "tenant_id": "t1",
            "iat": 1000000,
            "exp": 9999999,
        }
        claims = OIDCProvider._map_claims(payload)
        assert claims.identity_kind == "user"
