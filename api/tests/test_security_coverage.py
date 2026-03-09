"""Comprehensive tests for api/api/security.py.

Covers areas previously at 0% coverage:
- CredentialVault: encrypt/decrypt round-trip, corrupted ciphertext
- TokenManager JWT mode: generate/validate happy path, expired, wrong secret
- TokenManager dev mode: all tokens valid, expired, tampered
- JWT secret rotation: jwt_secret_previous fallback
- _validate_url_safe: private IPs, metadata endpoints, safe URLs, DNS errors
- OIDCProvider: _fetch_discovery, _fetch_jwks, _get_signing_key, _map_claims
- TokenManager OIDC: validate_token_async, _validate_oidc_token
- TokenManager generate_token: dev/jwt/oidc modes
- TokenManager generate_refresh_token
- TokenConfig.resolve_kms_provider
- AzureKeyVaultProvider._parse_key_uri
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import time
import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import SecretStr

from api.security import (
    AuthMode,
    CredentialVault,
    KmsProvider,
    OIDCProvider,
    TokenClaims,
    TokenConfig,
    TokenManager,
    _validate_url_safe,
)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_JWT_SECRET = "super-secret-jwt-key-for-tests-32chars"
_JWT_SECRET_ALT = "different-secret-jwt-key-for-tests!!"


def _make_token_config(
    auth_mode: AuthMode = AuthMode.JWT,
    jwt_secret: str = _JWT_SECRET,
    jwt_secret_previous: str | None = None,
    oidc_issuer_url: str | None = None,
    oidc_audience: str | None = None,
    token_ttl_seconds: int = 3600,
) -> TokenConfig:
    return TokenConfig(
        auth_mode=auth_mode,
        jwt_secret=SecretStr(jwt_secret),
        jwt_secret_previous=SecretStr(jwt_secret_previous) if jwt_secret_previous else None,
        oidc_issuer_url=oidc_issuer_url,
        oidc_audience=oidc_audience,
        token_ttl_seconds=token_ttl_seconds,
    )


def _make_dev_token_raw(
    secret: str = _JWT_SECRET,
    sub: str = "user-1",
    tenant_id: str = "tenant-abc",
    exp_offset: float = 3600.0,
) -> str:
    """Produce a raw dev-mode HMAC token without using the class under test."""
    now = time.time()
    payload = {
        "sub": sub,
        "tenant_id": tenant_id,
        "iss": "ironlayer",
        "iat": now,
        "exp": now + exp_offset,
        "scopes": ["read", "write"],
        "jti": uuid.uuid4().hex,
        "identity_kind": "user",
        "role": None,
    }
    payload_json = json.dumps(payload)
    sig = hmac.new(
        secret.encode("utf-8"),
        payload_json.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    b64 = base64.urlsafe_b64encode(payload_json.encode("utf-8")).decode("ascii")
    return f"bmdev.{b64}.{sig}"


# ===========================================================================
# 1. CredentialVault
# ===========================================================================


class TestCredentialVault:
    """Tests for CredentialVault encrypt/decrypt."""

    def test_encrypt_returns_non_empty_string(self) -> None:
        vault = CredentialVault("my-secret-key")
        ct = vault.encrypt("dapitoken12345")
        assert isinstance(ct, str)
        assert len(ct) > 0

    def test_encrypt_decrypt_round_trip(self) -> None:
        vault = CredentialVault("my-secret-key")
        plaintext = "dapi-super-secret-databricks-token"
        ct = vault.encrypt(plaintext)
        assert vault.decrypt(ct) == plaintext

    def test_different_plaintexts_produce_different_ciphertexts(self) -> None:
        vault = CredentialVault("my-secret-key")
        ct1 = vault.encrypt("token-a")
        ct2 = vault.encrypt("token-b")
        assert ct1 != ct2

    def test_same_plaintext_produces_different_ciphertexts_fernet_nonce(self) -> None:
        """Fernet uses a random IV so each encryption is unique."""
        vault = CredentialVault("my-secret-key")
        ct1 = vault.encrypt("hello")
        ct2 = vault.encrypt("hello")
        assert ct1 != ct2

    def test_decrypt_with_wrong_key_raises(self) -> None:
        vault_a = CredentialVault("secret-key-alpha")
        vault_b = CredentialVault("secret-key-beta")
        ct = vault_a.encrypt("sensitive-value")
        with pytest.raises(Exception):
            vault_b.decrypt(ct)

    def test_decrypt_corrupted_ciphertext_raises(self) -> None:
        vault = CredentialVault("my-secret-key")
        with pytest.raises(Exception):
            vault.decrypt("this-is-not-valid-fernet-ciphertext")

    def test_encrypt_empty_string(self) -> None:
        vault = CredentialVault("my-secret-key")
        ct = vault.encrypt("")
        assert vault.decrypt(ct) == ""

    def test_encrypt_unicode_content(self) -> None:
        vault = CredentialVault("my-secret-key")
        value = "dapi-token-with-unicode-\u00e9\u00e0\u00fc"
        ct = vault.encrypt(value)
        assert vault.decrypt(ct) == value

    def test_different_secrets_same_plaintext(self) -> None:
        v1 = CredentialVault("secret-one-for-testing")
        v2 = CredentialVault("secret-two-for-testing")
        ct1 = v1.encrypt("value")
        ct2 = v2.encrypt("value")
        # Different keys -> different ciphertexts
        assert ct1 != ct2
        # Cross-decryption must fail
        with pytest.raises(Exception):
            v1.decrypt(ct2)


class TestCredentialVaultV2Salt:
    """BL-070: per-credential random salt (v2 envelope format).

    Verifies that:
    - New ciphertexts use the v2 envelope (base64-decode begins with 0x02).
    - Two encryptions of the same plaintext produce different ciphertexts
      because each uses a fresh 16-byte salt.
    - The same vault instance can decrypt both outputs.
    - A v1 legacy ciphertext (fixed salt, version byte 0x01) is still
      accepted by decrypt() for migration safety.
    - A bare Fernet token (no version prefix, pre-v2 format) is still
      accepted as a last-resort legacy fallback.
    - A vault created with key A cannot decrypt a ciphertext from key B.
    """

    def test_new_ciphertext_uses_v2_envelope(self) -> None:
        """Encrypted ciphertexts must start with the v2 version byte (0x02)."""
        vault = CredentialVault("test-master-key-for-v2")
        ct = vault.encrypt("sensitive-value")
        raw = base64.urlsafe_b64decode(ct.encode("ascii"))
        assert raw[:1] == b"\x02", "Version byte must be 0x02 for v2 format"

    def test_v2_envelope_contains_16_byte_salt(self) -> None:
        """v2 envelope has a 16-byte salt at bytes [1:17]."""
        vault = CredentialVault("test-master-key-for-v2")
        ct = vault.encrypt("check-salt-length")
        raw = base64.urlsafe_b64decode(ct.encode("ascii"))
        assert len(raw) >= 17, "Envelope must have at least version + salt bytes"
        # The remaining bytes after version + salt are the Fernet token.
        assert len(raw[17:]) > 0, "Fernet token must follow version+salt"

    def test_same_plaintext_different_ciphertexts_due_to_salt(self) -> None:
        """Two encryptions of the same plaintext must produce different outputs."""
        vault = CredentialVault("test-master-key-for-v2")
        ct1 = vault.encrypt("same-value")
        ct2 = vault.encrypt("same-value")
        assert ct1 != ct2, "Per-credential salt must produce unique ciphertexts"

    def test_v2_decrypt_round_trip(self) -> None:
        """v2 ciphertexts must decrypt back to the original plaintext."""
        vault = CredentialVault("test-master-key-for-v2")
        plaintext = "dapi-super-secret-databricks-token"
        ct = vault.encrypt(plaintext)
        assert vault.decrypt(ct) == plaintext

    def test_v2_decrypt_multiple_round_trips(self) -> None:
        """All v2 ciphertexts from the same vault must decrypt correctly."""
        vault = CredentialVault("test-master-key-for-v2")
        values = ["token-a", "token-b", "", "unicode-\u00e9\u00e0\u00fc"]
        for val in values:
            assert vault.decrypt(vault.encrypt(val)) == val

    def test_wrong_key_cannot_decrypt_v2_ciphertext(self) -> None:
        """A vault with a different secret must not be able to decrypt v2 ciphertexts."""
        vault_a = CredentialVault("secret-key-alpha-v2")
        vault_b = CredentialVault("secret-key-beta-v2")
        ct = vault_a.encrypt("sensitive")
        with pytest.raises(Exception):
            vault_b.decrypt(ct)

    def test_legacy_v1_ciphertext_still_decrypts(self) -> None:
        """v1 (fixed-salt) ciphertexts must still be decryptable for migration safety."""
        import hashlib

        from cryptography.fernet import Fernet

        secret = "legacy-test-secret-key"
        plaintext = "old-credential-value"

        # Build a v1 ciphertext manually (VERSION=0x01 || FERNET_TOKEN).
        fixed_salt = b"ironlayer-credential-vault-v1"
        key_bytes = hashlib.pbkdf2_hmac(
            "sha256",
            secret.encode("utf-8"),
            fixed_salt,
            iterations=480_000,
            dklen=32,
        )
        fernet = Fernet(base64.urlsafe_b64encode(key_bytes))
        fernet_token = fernet.encrypt(plaintext.encode("utf-8"))
        # Wrap with v1 envelope.
        envelope = b"\x01" + fernet_token
        v1_ct = base64.urlsafe_b64encode(envelope).decode("ascii")

        vault = CredentialVault(secret)
        assert vault.decrypt(v1_ct) == plaintext

    def test_bare_fernet_token_falls_back_to_fixed_salt(self) -> None:
        """Bare Fernet tokens (no version prefix) fall back to the v1 fixed salt."""
        import hashlib

        from cryptography.fernet import Fernet

        secret = "legacy-bare-token-secret"
        plaintext = "bare-fernet-credential"

        # Produce a raw Fernet token without any version envelope (pre-v2 format).
        fixed_salt = b"ironlayer-credential-vault-v1"
        key_bytes = hashlib.pbkdf2_hmac(
            "sha256",
            secret.encode("utf-8"),
            fixed_salt,
            iterations=480_000,
            dklen=32,
        )
        fernet = Fernet(base64.urlsafe_b64encode(key_bytes))
        bare_token = fernet.encrypt(plaintext.encode("utf-8"))
        # Re-encode as base64 the way the old encrypt() did: fernet token → str.
        bare_ct = base64.urlsafe_b64encode(bare_token).decode("ascii")

        vault = CredentialVault(secret)
        assert vault.decrypt(bare_ct) == plaintext


# ===========================================================================
# 2. TokenManager — dev mode
# ===========================================================================


class TestTokenManagerDevMode:
    """Tests for TokenManager in DEVELOPMENT auth mode."""

    def _mgr(self) -> TokenManager:
        return TokenManager(_make_token_config(auth_mode=AuthMode.DEVELOPMENT))

    def test_generate_token_returns_bmdev_prefix(self) -> None:
        mgr = self._mgr()
        token = mgr.generate_token(subject="alice", tenant_id="tenant-1")
        assert token.startswith("bmdev.")

    def test_generate_and_validate_happy_path(self) -> None:
        mgr = self._mgr()
        token = mgr.generate_token(subject="alice", tenant_id="tenant-1")
        claims = mgr.validate_token(token)
        assert claims.sub == "alice"
        assert claims.tenant_id == "tenant-1"

    def test_validate_dev_token_wrong_prefix_raises(self) -> None:
        mgr = self._mgr()
        with pytest.raises(PermissionError, match="Invalid token format"):
            mgr.validate_token("notbmdev.abc.def")

    def test_validate_dev_token_wrong_part_count_raises(self) -> None:
        mgr = self._mgr()
        with pytest.raises(PermissionError, match="Invalid token format"):
            mgr.validate_token("bmdev.onlytwoparts")

    def test_validate_dev_token_tampered_payload_raises(self) -> None:
        mgr = self._mgr()
        token = mgr.generate_token(subject="alice", tenant_id="t1")
        parts = token.split(".")
        # Corrupt the payload (middle part)
        tampered_payload = base64.urlsafe_b64encode(b"not-valid-json").decode("ascii")
        tampered = f"{parts[0]}.{tampered_payload}.{parts[2]}"
        with pytest.raises(PermissionError):
            mgr.validate_token(tampered)

    def test_validate_dev_token_wrong_signature_raises(self) -> None:
        mgr = self._mgr()
        token = mgr.generate_token(subject="alice", tenant_id="t1")
        parts = token.split(".")
        bad_sig = "a" * 64
        with pytest.raises(PermissionError, match="signature"):
            mgr.validate_token(f"{parts[0]}.{parts[1]}.{bad_sig}")

    def test_validate_expired_dev_token_raises(self) -> None:
        mgr = self._mgr()
        # Generate a token that is already expired
        expired_token = _make_dev_token_raw(
            secret=_JWT_SECRET,
            exp_offset=-3600.0,  # expired 1 hour ago
        )
        with pytest.raises(PermissionError, match="expired"):
            mgr.validate_token(expired_token)

    def test_generate_token_custom_ttl(self) -> None:
        mgr = self._mgr()
        token = mgr.generate_token(subject="svc", tenant_id="t", ttl_seconds=60)
        claims = mgr.validate_token(token)
        assert claims.exp <= time.time() + 61

    def test_generate_token_ttl_capped_at_max(self) -> None:
        cfg = _make_token_config(auth_mode=AuthMode.DEVELOPMENT)
        # max_token_ttl_seconds defaults to 86400
        mgr = TokenManager(cfg)
        token = mgr.generate_token(subject="svc", tenant_id="t", ttl_seconds=999999)
        claims = mgr.validate_token(token)
        assert claims.exp <= time.time() + 86401

    def test_generate_token_with_scopes_and_role(self) -> None:
        mgr = self._mgr()
        token = mgr.generate_token(
            subject="alice",
            tenant_id="t",
            scopes=["read"],
            role="viewer",
            identity_kind="user",
        )
        claims = mgr.validate_token(token)
        assert claims.scopes == ["read"]
        assert claims.role == "viewer"

    def test_generate_refresh_token_has_refresh_scope(self) -> None:
        mgr = self._mgr()
        token = mgr.generate_refresh_token(subject="alice", tenant_id="t")
        claims = mgr.validate_token(token)
        assert "refresh" in claims.scopes

    def test_auth_mode_property(self) -> None:
        mgr = self._mgr()
        assert mgr.auth_mode == AuthMode.DEVELOPMENT

    def test_validate_dev_token_non_b64_payload_raises(self) -> None:
        """An invalid base64 string in the payload position raises PermissionError."""
        mgr = self._mgr()
        # Build a token with garbage base64
        with pytest.raises(PermissionError):
            mgr.validate_token("bmdev.!!!not-base64!!!.fakesig")


# ===========================================================================
# 3. TokenManager — JWT mode
# ===========================================================================


class TestTokenManagerJWTMode:
    """Tests for TokenManager in JWT auth mode."""

    def _mgr(self, secret: str = _JWT_SECRET, previous: str | None = None) -> TokenManager:
        return TokenManager(_make_token_config(auth_mode=AuthMode.JWT, jwt_secret=secret, jwt_secret_previous=previous))

    def test_generate_token_returns_string(self) -> None:
        mgr = self._mgr()
        token = mgr.generate_token(subject="alice", tenant_id="t1")
        assert isinstance(token, str)
        assert len(token.split(".")) == 3  # JWT has 3 parts

    def test_generate_and_validate_happy_path(self) -> None:
        mgr = self._mgr()
        token = mgr.generate_token(subject="alice", tenant_id="t1")
        claims = mgr.validate_token(token)
        assert claims.sub == "alice"
        assert claims.tenant_id == "t1"

    def test_validate_wrong_secret_raises(self) -> None:
        mgr_sign = self._mgr(secret=_JWT_SECRET)
        mgr_verify = self._mgr(secret=_JWT_SECRET_ALT)
        token = mgr_sign.generate_token(subject="alice", tenant_id="t1")
        with pytest.raises(PermissionError, match="signature"):
            mgr_verify.validate_token(token)

    def test_validate_expired_token_raises(self) -> None:
        mgr = self._mgr()
        # Generate a token with a -1 second TTL so it is immediately expired.
        # We need to go through jwt directly to create an already-expired token.
        import jwt

        payload = {
            "sub": "alice",
            "tenant_id": "t1",
            "iss": "ironlayer",
            "iat": time.time() - 7200,
            "exp": time.time() - 3600,
            "scopes": ["read"],
            "jti": uuid.uuid4().hex,
            "identity_kind": "user",
            "role": None,
        }
        expired_token = jwt.encode(payload, _JWT_SECRET, algorithm="HS256")
        with pytest.raises(PermissionError, match="expired"):
            mgr.validate_token(expired_token)

    def test_validate_invalid_token_raises(self) -> None:
        mgr = self._mgr()
        with pytest.raises(PermissionError):
            mgr.validate_token("not.a.valid.jwt.at.all")

    def test_validate_tampered_token_raises(self) -> None:
        mgr = self._mgr()
        token = mgr.generate_token(subject="alice", tenant_id="t1")
        # Flip a char in the signature part
        parts = token.split(".")
        tampered_sig = parts[2][:-1] + ("a" if parts[2][-1] != "a" else "b")
        tampered = f"{parts[0]}.{parts[1]}.{tampered_sig}"
        with pytest.raises(PermissionError):
            mgr.validate_token(tampered)

    def test_jwt_generate_with_role(self) -> None:
        mgr = self._mgr()
        token = mgr.generate_token(subject="admin", tenant_id="t", role="admin")
        claims = mgr.validate_token(token)
        assert claims.role == "admin"

    def test_jwt_generate_refresh_token(self) -> None:
        mgr = self._mgr()
        token = mgr.generate_refresh_token(subject="alice", tenant_id="t")
        claims = mgr.validate_token(token)
        assert "refresh" in claims.scopes


# ===========================================================================
# 4. JWT secret rotation (jwt_secret_previous fallback)
# ===========================================================================


class TestJWTSecretRotation:
    """Tests for zero-downtime JWT secret rotation."""

    def test_token_signed_with_old_secret_validates_via_previous(self) -> None:
        """A token signed by the old secret should still validate when
        jwt_secret_previous is set to that old secret."""
        # Token was generated before rotation (signed with _JWT_SECRET)
        old_mgr = TokenManager(_make_token_config(auth_mode=AuthMode.JWT, jwt_secret=_JWT_SECRET))
        old_token = old_mgr.generate_token(subject="alice", tenant_id="t")

        # After rotation: new secret is _JWT_SECRET_ALT, previous is _JWT_SECRET
        rotated_mgr = TokenManager(
            _make_token_config(
                auth_mode=AuthMode.JWT,
                jwt_secret=_JWT_SECRET_ALT,
                jwt_secret_previous=_JWT_SECRET,
            )
        )
        claims = rotated_mgr.validate_token(old_token)
        assert claims.sub == "alice"

    def test_token_signed_with_new_secret_validates_normally(self) -> None:
        """New tokens signed with the new secret validate without fallback."""
        rotated_mgr = TokenManager(
            _make_token_config(
                auth_mode=AuthMode.JWT,
                jwt_secret=_JWT_SECRET_ALT,
                jwt_secret_previous=_JWT_SECRET,
            )
        )
        new_token = rotated_mgr.generate_token(subject="bob", tenant_id="t")
        claims = rotated_mgr.validate_token(new_token)
        assert claims.sub == "bob"

    def test_token_with_unknown_secret_fails_even_with_previous(self) -> None:
        """A token signed with a completely different secret must not validate."""
        random_mgr = TokenManager(
            _make_token_config(auth_mode=AuthMode.JWT, jwt_secret="totally-random-key-unknown-xyz!")
        )
        unknown_token = random_mgr.generate_token(subject="eve", tenant_id="t")

        rotated_mgr = TokenManager(
            _make_token_config(
                auth_mode=AuthMode.JWT,
                jwt_secret=_JWT_SECRET_ALT,
                jwt_secret_previous=_JWT_SECRET,
            )
        )
        with pytest.raises(PermissionError, match="signature"):
            rotated_mgr.validate_token(unknown_token)

    def test_no_previous_secret_single_attempt_only(self) -> None:
        """Without jwt_secret_previous, only one secret is tried."""
        mgr_sign = TokenManager(_make_token_config(auth_mode=AuthMode.JWT, jwt_secret=_JWT_SECRET))
        mgr_verify = TokenManager(
            _make_token_config(auth_mode=AuthMode.JWT, jwt_secret=_JWT_SECRET_ALT, jwt_secret_previous=None)
        )
        token = mgr_sign.generate_token(subject="alice", tenant_id="t")
        with pytest.raises(PermissionError):
            mgr_verify.validate_token(token)


# ===========================================================================
# 5. TokenManager — OIDC generate raises ValueError
# ===========================================================================


class TestTokenManagerOIDCGenerateRaises:
    def test_generate_token_oidc_raises_value_error(self) -> None:
        cfg = _make_token_config(
            auth_mode=AuthMode.OIDC_ONPREM,
            oidc_issuer_url="https://accounts.example.com",
        )
        mgr = TokenManager(cfg)
        with pytest.raises(ValueError, match="OIDC on-prem mode does not generate tokens"):
            mgr.generate_token(subject="alice", tenant_id="t")


# ===========================================================================
# 6. TokenManager — async validate_token_async
# ===========================================================================


class TestTokenManagerValidateAsync:
    """validate_token_async dispatches correctly."""

    async def test_validate_token_async_dev_mode(self) -> None:
        mgr = TokenManager(_make_token_config(auth_mode=AuthMode.DEVELOPMENT))
        token = mgr.generate_token(subject="alice", tenant_id="t")
        claims = await mgr.validate_token_async(token)
        assert claims.sub == "alice"

    async def test_validate_token_async_jwt_mode(self) -> None:
        mgr = TokenManager(_make_token_config(auth_mode=AuthMode.JWT))
        token = mgr.generate_token(subject="bob", tenant_id="t")
        claims = await mgr.validate_token_async(token)
        assert claims.sub == "bob"

    async def test_validate_token_async_oidc_mode_calls_oidc(self) -> None:
        """OIDC path calls _validate_oidc_token."""
        cfg = _make_token_config(
            auth_mode=AuthMode.OIDC_ONPREM,
            oidc_issuer_url="https://accounts.example.com",
        )
        mgr = TokenManager(cfg)
        expected_claims = TokenClaims(sub="u", tenant_id="t")
        with patch.object(mgr, "_validate_oidc_token", new=AsyncMock(return_value=expected_claims)) as mock_method:
            claims = await mgr.validate_token_async("some.oidc.token")
            mock_method.assert_awaited_once_with("some.oidc.token")
            assert claims.sub == "u"


# ===========================================================================
# 7. _validate_url_safe — SSRF protection
# ===========================================================================


class TestValidateUrlSafe:
    """Tests for the _validate_url_safe SSRF prevention function."""

    async def test_non_https_scheme_raises(self) -> None:
        with pytest.raises(PermissionError, match="Only HTTPS"):
            await _validate_url_safe("http://accounts.example.com/.well-known/oidc", "accounts.example.com")

    async def test_ftp_scheme_raises(self) -> None:
        with pytest.raises(PermissionError, match="Only HTTPS"):
            await _validate_url_safe("ftp://accounts.example.com/file", "accounts.example.com")

    async def test_wrong_hostname_raises(self) -> None:
        # Mock DNS to return a public IP so we don't fail on DNS
        with patch("asyncio.get_running_loop") as mock_loop:
            loop = MagicMock()
            loop.getaddrinfo = AsyncMock(return_value=[
                (None, None, None, None, ("8.8.8.8", 0))
            ])
            mock_loop.return_value = loop
            with pytest.raises(PermissionError, match="not the allowed issuer host"):
                await _validate_url_safe("https://evil.attacker.com/discovery", "accounts.example.com")

    async def test_exact_hostname_match_with_public_ip(self) -> None:
        """Exact hostname match + public IP should succeed."""
        with patch("asyncio.get_running_loop") as mock_loop:
            loop = MagicMock()
            future = asyncio.get_event_loop().create_future()
            future.set_result([(None, None, None, None, ("1.2.3.4", 0))])
            loop.getaddrinfo = MagicMock(return_value=future)
            mock_loop.return_value = loop
            # Should not raise
            await _validate_url_safe("https://accounts.example.com/discovery", "accounts.example.com")

    async def test_subdomain_match_with_public_ip(self) -> None:
        """Subdomain of allowed host + public IP should succeed."""
        with patch("asyncio.get_running_loop") as mock_loop:
            loop = MagicMock()
            future = asyncio.get_event_loop().create_future()
            future.set_result([(None, None, None, None, ("1.2.3.4", 0))])
            loop.getaddrinfo = MagicMock(return_value=future)
            mock_loop.return_value = loop
            # www.accounts.example.com ends with .accounts.example.com
            await _validate_url_safe("https://www.accounts.example.com/discovery", "accounts.example.com")

    async def test_private_ip_10x_raises(self) -> None:
        """A URL resolving to 10.x.x.x must be rejected."""
        with patch("asyncio.get_running_loop") as mock_loop:
            loop = MagicMock()
            future = asyncio.get_event_loop().create_future()
            future.set_result([(None, None, None, None, ("10.0.0.1", 0))])
            loop.getaddrinfo = MagicMock(return_value=future)
            mock_loop.return_value = loop
            with pytest.raises(PermissionError, match="private/reserved"):
                await _validate_url_safe("https://accounts.example.com/discovery", "accounts.example.com")

    async def test_private_ip_192168_raises(self) -> None:
        """A URL resolving to 192.168.x.x must be rejected."""
        with patch("asyncio.get_running_loop") as mock_loop:
            loop = MagicMock()
            future = asyncio.get_event_loop().create_future()
            future.set_result([(None, None, None, None, ("192.168.1.100", 0))])
            loop.getaddrinfo = MagicMock(return_value=future)
            mock_loop.return_value = loop
            with pytest.raises(PermissionError, match="private/reserved"):
                await _validate_url_safe("https://accounts.example.com/discovery", "accounts.example.com")

    async def test_loopback_ip_raises(self) -> None:
        """A URL resolving to 127.x.x.x loopback must be rejected."""
        with patch("asyncio.get_running_loop") as mock_loop:
            loop = MagicMock()
            future = asyncio.get_event_loop().create_future()
            future.set_result([(None, None, None, None, ("127.0.0.1", 0))])
            loop.getaddrinfo = MagicMock(return_value=future)
            mock_loop.return_value = loop
            with pytest.raises(PermissionError, match="private/reserved"):
                await _validate_url_safe("https://accounts.example.com/discovery", "accounts.example.com")

    async def test_link_local_metadata_endpoint_raises(self) -> None:
        """The cloud metadata endpoint 169.254.169.254 must be rejected."""
        with patch("asyncio.get_running_loop") as mock_loop:
            loop = MagicMock()
            future = asyncio.get_event_loop().create_future()
            future.set_result([(None, None, None, None, ("169.254.169.254", 0))])
            loop.getaddrinfo = MagicMock(return_value=future)
            mock_loop.return_value = loop
            with pytest.raises(PermissionError, match="private/reserved"):
                await _validate_url_safe("https://accounts.example.com/discovery", "accounts.example.com")

    async def test_172_16_private_range_raises(self) -> None:
        """172.16-31.x.x is private and must be rejected."""
        with patch("asyncio.get_running_loop") as mock_loop:
            loop = MagicMock()
            future = asyncio.get_event_loop().create_future()
            future.set_result([(None, None, None, None, ("172.16.0.1", 0))])
            loop.getaddrinfo = MagicMock(return_value=future)
            mock_loop.return_value = loop
            with pytest.raises(PermissionError, match="private/reserved"):
                await _validate_url_safe("https://accounts.example.com/discovery", "accounts.example.com")

    async def test_dns_timeout_raises(self) -> None:
        """DNS timeout must raise PermissionError with timeout message."""
        with patch("asyncio.get_running_loop") as mock_loop:
            loop = MagicMock()

            async def _timeout(*args: Any, **kwargs: Any) -> Any:
                raise asyncio.TimeoutError()

            loop.getaddrinfo = _timeout
            mock_loop.return_value = loop
            with patch("asyncio.wait_for", side_effect=asyncio.TimeoutError()):
                with pytest.raises(PermissionError, match="timed out"):
                    await _validate_url_safe("https://accounts.example.com/discovery", "accounts.example.com")

    async def test_dns_os_error_raises(self) -> None:
        """DNS OSError must raise PermissionError with resolution-failed message."""
        with patch("asyncio.get_running_loop") as mock_loop:
            loop = MagicMock()
            mock_loop.return_value = loop
            with patch("asyncio.wait_for", side_effect=OSError("Name or service not known")):
                with pytest.raises(PermissionError, match="DNS resolution failed"):
                    await _validate_url_safe("https://accounts.example.com/discovery", "accounts.example.com")

    async def test_empty_dns_result_raises(self) -> None:
        """Empty DNS response must raise PermissionError."""
        with patch("asyncio.get_running_loop") as mock_loop:
            loop = MagicMock()
            future = asyncio.get_event_loop().create_future()
            future.set_result([])
            loop.getaddrinfo = MagicMock(return_value=future)
            mock_loop.return_value = loop
            with pytest.raises(PermissionError, match="no results"):
                await _validate_url_safe("https://accounts.example.com/discovery", "accounts.example.com")

    async def test_no_hostname_raises(self) -> None:
        """A URL with no extractable hostname raises PermissionError."""
        with pytest.raises(PermissionError, match="Could not extract hostname"):
            await _validate_url_safe("https:///path", "accounts.example.com")


# ===========================================================================
# 8. OIDCProvider
# ===========================================================================


class TestOIDCProvider:
    """Tests for OIDCProvider class."""

    def test_init_extracts_allowed_issuer_host(self) -> None:
        provider = OIDCProvider("https://accounts.example.com")
        assert provider._allowed_issuer_host == "accounts.example.com"

    def test_init_strips_trailing_slash(self) -> None:
        provider = OIDCProvider("https://accounts.example.com/")
        assert provider._issuer_url == "https://accounts.example.com"

    def test_init_invalid_issuer_url_raises(self) -> None:
        with pytest.raises(PermissionError, match="Could not extract hostname"):
            OIDCProvider("not-a-valid-url")

    def test_map_claims_basic(self) -> None:
        payload: dict[str, Any] = {
            "sub": "user123",
            "tenant_id": "tenant-abc",
            "iss": "https://example.com",
            "iat": time.time(),
            "exp": time.time() + 3600,
            "scope": "read write",
            "jti": "abc",
        }
        claims = OIDCProvider._map_claims(payload)
        assert claims.sub == "user123"
        assert claims.tenant_id == "tenant-abc"
        assert "read" in claims.scopes
        assert "write" in claims.scopes

    def test_map_claims_missing_sub_raises(self) -> None:
        with pytest.raises(PermissionError, match="missing 'sub'"):
            OIDCProvider._map_claims({"tenant_id": "t"})

    def test_map_claims_missing_tenant_id_raises(self) -> None:
        with pytest.raises(PermissionError, match="missing tenant identifier"):
            OIDCProvider._map_claims({"sub": "u"})

    def test_map_claims_uses_org_id(self) -> None:
        payload = {"sub": "u", "org_id": "org-123", "scope": "read"}
        claims = OIDCProvider._map_claims(payload)
        assert claims.tenant_id == "org-123"

    def test_map_claims_uses_azp(self) -> None:
        payload = {"sub": "u", "azp": "app-client-id", "scope": "read"}
        claims = OIDCProvider._map_claims(payload)
        assert claims.tenant_id == "app-client-id"

    def test_map_claims_uses_client_id(self) -> None:
        payload = {"sub": "u", "client_id": "cli-id", "scope": "read"}
        claims = OIDCProvider._map_claims(payload)
        assert claims.tenant_id == "cli-id"

    def test_map_claims_scope_as_list(self) -> None:
        payload = {"sub": "u", "tenant_id": "t", "scp": ["read", "write"]}
        claims = OIDCProvider._map_claims(payload)
        assert "read" in claims.scopes

    def test_map_claims_unknown_scope_defaults_to_read(self) -> None:
        payload = {"sub": "u", "tenant_id": "t", "scope": "unknown_scope"}
        claims = OIDCProvider._map_claims(payload)
        assert claims.scopes == ["read"]

    def test_map_claims_write_scope_from_admin(self) -> None:
        payload = {"sub": "u", "tenant_id": "t", "scope": "admin"}
        claims = OIDCProvider._map_claims(payload)
        assert "write" in claims.scopes

    def test_map_claims_openid_scope_maps_to_read(self) -> None:
        payload = {"sub": "u", "tenant_id": "t", "scope": "openid"}
        claims = OIDCProvider._map_claims(payload)
        assert "read" in claims.scopes

    def test_map_claims_integer_scope_defaults(self) -> None:
        """Non-string, non-list scope type defaults to read."""
        payload = {"sub": "u", "tenant_id": "t", "scope": 42}
        claims = OIDCProvider._map_claims(payload)
        assert claims.scopes == ["read"]

    async def test_fetch_discovery_calls_validate_and_executor(self) -> None:
        """_fetch_discovery validates URL and runs HTTP fetch in executor."""
        provider = OIDCProvider("https://accounts.example.com")
        discovery_doc = {
            "issuer": "https://accounts.example.com",
            "jwks_uri": "https://accounts.example.com/jwks",
            "id_token_signing_alg_values_supported": ["RS256"],
        }

        with patch("api.security._validate_url_safe", new=AsyncMock()) as mock_ssrf:
            with patch("asyncio.get_running_loop") as mock_loop:
                loop = MagicMock()
                loop.run_in_executor = AsyncMock(return_value=json.dumps(discovery_doc).encode())
                mock_loop.return_value = loop

                result = await provider._fetch_discovery()
                assert result["issuer"] == "https://accounts.example.com"
                mock_ssrf.assert_awaited_once()

    async def test_fetch_discovery_missing_fields_raises(self) -> None:
        """Discovery document missing required fields raises PermissionError."""
        provider = OIDCProvider("https://accounts.example.com")
        incomplete_doc = {"issuer": "https://accounts.example.com"}  # missing jwks_uri etc.

        with patch("api.security._validate_url_safe", new=AsyncMock()):
            with patch("asyncio.get_running_loop") as mock_loop:
                loop = MagicMock()
                loop.run_in_executor = AsyncMock(return_value=json.dumps(incomplete_doc).encode())
                mock_loop.return_value = loop

                with pytest.raises(PermissionError, match="missing required fields"):
                    await provider._fetch_discovery()

    async def test_fetch_discovery_invalid_json_raises(self) -> None:
        """Non-JSON response raises PermissionError."""
        provider = OIDCProvider("https://accounts.example.com")

        with patch("api.security._validate_url_safe", new=AsyncMock()):
            with patch("asyncio.get_running_loop") as mock_loop:
                loop = MagicMock()
                loop.run_in_executor = AsyncMock(return_value=b"not-json")
                mock_loop.return_value = loop

                with pytest.raises(PermissionError, match="Failed to parse"):
                    await provider._fetch_discovery()

    async def test_fetch_discovery_http_error_raises(self) -> None:
        """HTTP fetch failure raises PermissionError."""
        provider = OIDCProvider("https://accounts.example.com")

        with patch("api.security._validate_url_safe", new=AsyncMock()):
            with patch("asyncio.get_running_loop") as mock_loop:
                loop = MagicMock()
                loop.run_in_executor = AsyncMock(side_effect=PermissionError("Failed to fetch OIDC discovery"))
                mock_loop.return_value = loop

                with pytest.raises(PermissionError, match="Failed to fetch"):
                    await provider._fetch_discovery()

    async def test_fetch_jwks_fetches_discovery_first_if_none(self) -> None:
        """_fetch_jwks calls _fetch_discovery when _discovery is None."""
        provider = OIDCProvider("https://accounts.example.com")
        discovery_doc = {
            "issuer": "https://accounts.example.com",
            "jwks_uri": "https://accounts.example.com/jwks",
            "id_token_signing_alg_values_supported": ["RS256"],
        }
        jwks_doc = {"keys": [{"kid": "key1", "kty": "RSA"}]}

        call_count = 0

        async def fake_fetch_discovery() -> dict[str, Any]:
            nonlocal call_count
            call_count += 1
            provider._discovery = discovery_doc
            return discovery_doc

        with patch.object(provider, "_fetch_discovery", new=fake_fetch_discovery):
            with patch("api.security._validate_url_safe", new=AsyncMock()):
                with patch("asyncio.get_running_loop") as mock_loop:
                    loop = MagicMock()
                    loop.run_in_executor = AsyncMock(return_value=json.dumps(jwks_doc).encode())
                    mock_loop.return_value = loop

                    result = await provider._fetch_jwks()
                    assert "keys" in result
                    assert call_count == 1

    async def test_fetch_jwks_empty_keys_raises(self) -> None:
        """JWKS response with empty keys raises PermissionError."""
        provider = OIDCProvider("https://accounts.example.com")
        provider._discovery = {
            "issuer": "https://accounts.example.com",
            "jwks_uri": "https://accounts.example.com/jwks",
            "id_token_signing_alg_values_supported": ["RS256"],
        }
        jwks_doc: dict[str, Any] = {"keys": []}

        with patch("api.security._validate_url_safe", new=AsyncMock()):
            with patch("asyncio.get_running_loop") as mock_loop:
                loop = MagicMock()
                loop.run_in_executor = AsyncMock(return_value=json.dumps(jwks_doc).encode())
                mock_loop.return_value = loop

                with pytest.raises(PermissionError, match="no keys"):
                    await provider._fetch_jwks()

    async def test_fetch_jwks_missing_keys_field_raises(self) -> None:
        """JWKS response without 'keys' field raises PermissionError."""
        provider = OIDCProvider("https://accounts.example.com")
        provider._discovery = {
            "issuer": "https://accounts.example.com",
            "jwks_uri": "https://accounts.example.com/jwks",
            "id_token_signing_alg_values_supported": ["RS256"],
        }
        jwks_doc = {"not_keys": []}

        with patch("api.security._validate_url_safe", new=AsyncMock()):
            with patch("asyncio.get_running_loop") as mock_loop:
                loop = MagicMock()
                loop.run_in_executor = AsyncMock(return_value=json.dumps(jwks_doc).encode())
                mock_loop.return_value = loop

                with pytest.raises(PermissionError, match="no keys"):
                    await provider._fetch_jwks()

    async def test_get_signing_key_found_in_cache(self) -> None:
        """Key found in cached JWKS without refetch."""
        provider = OIDCProvider("https://accounts.example.com")
        provider._jwks = {"keys": [{"kid": "key-1", "kty": "RSA"}]}
        provider._jwks_fetched_at = time.time()

        key = await provider._get_signing_key("key-1")
        assert key["kid"] == "key-1"

    async def test_get_signing_key_not_found_raises(self) -> None:
        """Non-existent kid raises PermissionError after cache refresh."""
        provider = OIDCProvider("https://accounts.example.com")
        provider._jwks = {"keys": [{"kid": "key-1", "kty": "RSA"}]}
        provider._jwks_fetched_at = time.time()

        # Patch _fetch_jwks to avoid real network call on cache miss
        async def fake_fetch_jwks() -> dict[str, Any]:
            provider._jwks = {"keys": [{"kid": "key-1", "kty": "RSA"}]}
            provider._jwks_fetched_at = time.time()
            return provider._jwks

        with patch.object(provider, "_fetch_jwks", new=fake_fetch_jwks):
            with pytest.raises(PermissionError, match="No signing key found"):
                await provider._get_signing_key("unknown-kid")

    async def test_get_signing_key_expired_cache_triggers_fetch(self) -> None:
        """Expired cache triggers _fetch_jwks before searching."""
        provider = OIDCProvider("https://accounts.example.com")
        provider._jwks = {"keys": []}
        provider._jwks_fetched_at = time.time() - 9999  # expired

        fetch_called = False

        async def fake_fetch_jwks() -> dict[str, Any]:
            nonlocal fetch_called
            fetch_called = True
            provider._jwks = {"keys": [{"kid": "fresh-key", "kty": "RSA"}]}
            provider._jwks_fetched_at = time.time()
            return provider._jwks  # type: ignore[return-value]

        with patch.object(provider, "_fetch_jwks", new=fake_fetch_jwks):
            key = await provider._get_signing_key("fresh-key")
            assert fetch_called
            assert key["kid"] == "fresh-key"


# ===========================================================================
# 9. TokenConfig — resolve_kms_provider
# ===========================================================================


class TestTokenConfigResolveKmsProvider:
    def test_explicit_aws_kms_returns_aws(self) -> None:
        cfg = _make_token_config()
        cfg.kms_provider = KmsProvider.AWS_KMS
        assert cfg.resolve_kms_provider() == KmsProvider.AWS_KMS

    def test_explicit_azure_returns_azure(self) -> None:
        cfg = _make_token_config()
        cfg.kms_provider = KmsProvider.AZURE_KEYVAULT
        assert cfg.resolve_kms_provider() == KmsProvider.AZURE_KEYVAULT

    def test_auto_detects_aws_from_arn(self) -> None:
        cfg = TokenConfig(
            auth_mode=AuthMode.KMS_EXCHANGE,
            jwt_secret=SecretStr("secret"),
            kms_key_arn="arn:aws:kms:us-east-1:123456789012:key/abc",
            kms_provider=KmsProvider.AUTO,
        )
        assert cfg.resolve_kms_provider() == KmsProvider.AWS_KMS

    def test_auto_detects_azure_from_vault_uri(self) -> None:
        cfg = TokenConfig(
            auth_mode=AuthMode.KMS_EXCHANGE,
            jwt_secret=SecretStr("secret"),
            kms_key_arn="https://myvault.vault.azure.net/keys/mykey",
            kms_provider=KmsProvider.AUTO,
        )
        assert cfg.resolve_kms_provider() == KmsProvider.AZURE_KEYVAULT

    def test_auto_with_no_uri_defaults_to_aws(self) -> None:
        cfg = TokenConfig(
            auth_mode=AuthMode.KMS_EXCHANGE,
            jwt_secret=SecretStr("secret"),
            kms_key_arn=None,
            kms_provider=KmsProvider.AUTO,
        )
        assert cfg.resolve_kms_provider() == KmsProvider.AWS_KMS


# ===========================================================================
# 10. AzureKeyVaultProvider._parse_key_uri
# ===========================================================================


class TestAzureKeyVaultProviderParseKeyUri:
    def test_parse_valid_uri_without_version(self) -> None:
        from api.security import AzureKeyVaultProvider

        vault_url, key_name, key_version = AzureKeyVaultProvider._parse_key_uri(
            "https://myvault.vault.azure.net/keys/mykey"
        )
        assert vault_url == "https://myvault.vault.azure.net"
        assert key_name == "mykey"
        assert key_version is None

    def test_parse_valid_uri_with_version(self) -> None:
        from api.security import AzureKeyVaultProvider

        vault_url, key_name, key_version = AzureKeyVaultProvider._parse_key_uri(
            "https://myvault.vault.azure.net/keys/mykey/abc123"
        )
        assert vault_url == "https://myvault.vault.azure.net"
        assert key_name == "mykey"
        assert key_version == "abc123"

    def test_parse_non_vault_uri_raises(self) -> None:
        from api.security import AzureKeyVaultProvider

        with pytest.raises(ValueError, match="Invalid Azure Key Vault URI"):
            AzureKeyVaultProvider._parse_key_uri("https://example.com/keys/mykey")

    def test_parse_missing_key_path_raises(self) -> None:
        from api.security import AzureKeyVaultProvider

        with pytest.raises(ValueError):
            AzureKeyVaultProvider._parse_key_uri("https://myvault.vault.azure.net/notkeys/mykey")

    def test_parse_no_key_name_raises(self) -> None:
        from api.security import AzureKeyVaultProvider

        with pytest.raises(ValueError):
            AzureKeyVaultProvider._parse_key_uri("https://myvault.vault.azure.net/keys")


# ===========================================================================
# 11. TokenClaims model
# ===========================================================================


class TestTokenClaims:
    def test_default_values(self) -> None:
        claims = TokenClaims(sub="u", tenant_id="t")
        assert claims.iss == "ironlayer"
        assert claims.scopes == ["read", "write"]
        assert claims.identity_kind == "user"
        assert claims.role is None

    def test_custom_values(self) -> None:
        now = time.time()
        claims = TokenClaims(
            sub="svc",
            tenant_id="t",
            iss="custom-issuer",
            iat=now,
            exp=now + 7200,
            scopes=["admin"],
            jti="my-jti",
            identity_kind="service",
            role="admin",
        )
        assert claims.iss == "custom-issuer"
        assert claims.identity_kind == "service"
        assert claims.role == "admin"


# ===========================================================================
# 12. TokenManager — _get_oidc_provider
# ===========================================================================


class TestTokenManagerGetOidcProvider:
    def test_raises_if_no_issuer_url(self) -> None:
        cfg = _make_token_config(auth_mode=AuthMode.OIDC_ONPREM, oidc_issuer_url=None)
        mgr = TokenManager(cfg)
        with pytest.raises(PermissionError, match="OIDC issuer URL not configured"):
            mgr._get_oidc_provider()

    def test_creates_provider_on_first_call(self) -> None:
        cfg = _make_token_config(
            auth_mode=AuthMode.OIDC_ONPREM,
            oidc_issuer_url="https://accounts.example.com",
        )
        mgr = TokenManager(cfg)
        provider = mgr._get_oidc_provider()
        assert isinstance(provider, OIDCProvider)

    def test_returns_cached_provider_on_second_call(self) -> None:
        cfg = _make_token_config(
            auth_mode=AuthMode.OIDC_ONPREM,
            oidc_issuer_url="https://accounts.example.com",
        )
        mgr = TokenManager(cfg)
        p1 = mgr._get_oidc_provider()
        p2 = mgr._get_oidc_provider()
        assert p1 is p2


# ===========================================================================
# 13. TokenManager — OIDC validate_token sync raises RuntimeError in async
# ===========================================================================


class TestTokenManagerOIDCValidateSyncInAsync:
    """Calling the sync validate_token in OIDC mode from inside an async
    context must raise RuntimeError (not PermissionError)."""

    async def test_oidc_sync_validate_from_async_raises_runtime_error(self) -> None:
        cfg = _make_token_config(
            auth_mode=AuthMode.OIDC_ONPREM,
            oidc_issuer_url="https://accounts.example.com",
        )
        mgr = TokenManager(cfg)
        with pytest.raises(RuntimeError, match="validate_token"):
            mgr.validate_token("some.oidc.token")


# ===========================================================================
# 14. CredentialVault async helpers (with mocked DB)
# ===========================================================================


class TestCredentialVaultAsyncHelpers:
    """Tests for the DB-backed async methods on CredentialVault.

    The underlying CredentialRepository is mocked so no real DB is needed.
    """

    async def test_store_credential(self) -> None:
        vault = CredentialVault("test-secret")
        mock_session = MagicMock()
        mock_repo = AsyncMock()
        mock_repo.store = AsyncMock()

        with patch("core_engine.state.repository.CredentialRepository", return_value=mock_repo):
            await vault.store_credential(mock_session, "t1", "databricks_pat", "dapi123")
            mock_repo.store.assert_awaited_once()
            # Verify the value stored is encrypted (not the plaintext)
            stored_value = mock_repo.store.call_args[0][1]
            assert stored_value != "dapi123"

    async def test_get_credential_found(self) -> None:
        vault = CredentialVault("test-secret")
        plaintext = "dapi-token-value"
        encrypted = vault.encrypt(plaintext)

        mock_session = MagicMock()
        mock_repo = AsyncMock()
        mock_repo.get = AsyncMock(return_value=encrypted)

        with patch("core_engine.state.repository.CredentialRepository", return_value=mock_repo):
            result = await vault.get_credential(mock_session, "t1", "pat")
            assert result == plaintext

    async def test_get_credential_not_found_returns_none(self) -> None:
        vault = CredentialVault("test-secret")
        mock_session = MagicMock()
        mock_repo = AsyncMock()
        mock_repo.get = AsyncMock(return_value=None)

        with patch("core_engine.state.repository.CredentialRepository", return_value=mock_repo):
            result = await vault.get_credential(mock_session, "t1", "missing-cred")
            assert result is None

    async def test_delete_credential_returns_true(self) -> None:
        vault = CredentialVault("test-secret")
        mock_session = MagicMock()
        mock_repo = AsyncMock()
        mock_repo.delete = AsyncMock(return_value=True)

        with patch("core_engine.state.repository.CredentialRepository", return_value=mock_repo):
            result = await vault.delete_credential(mock_session, "t1", "pat")
            assert result is True

    async def test_delete_credential_returns_false_when_not_found(self) -> None:
        vault = CredentialVault("test-secret")
        mock_session = MagicMock()
        mock_repo = AsyncMock()
        mock_repo.delete = AsyncMock(return_value=False)

        with patch("core_engine.state.repository.CredentialRepository", return_value=mock_repo):
            result = await vault.delete_credential(mock_session, "t1", "nonexistent")
            assert result is False

    async def test_list_credentials(self) -> None:
        vault = CredentialVault("test-secret")
        mock_session = MagicMock()
        mock_repo = AsyncMock()
        mock_repo.list_names = AsyncMock(return_value=["pat", "api_key"])

        with patch("core_engine.state.repository.CredentialRepository", return_value=mock_repo):
            names = await vault.list_credentials(mock_session, "t1")
            assert names == ["pat", "api_key"]


# ===========================================================================
# 15. AuthMode enum
# ===========================================================================


class TestAuthModeEnum:
    def test_values(self) -> None:
        assert AuthMode.JWT.value == "jwt"
        assert AuthMode.KMS_EXCHANGE.value == "kms_exchange"
        assert AuthMode.OIDC_ONPREM.value == "oidc_onprem"
        assert AuthMode.DEVELOPMENT.value == "development"


# ===========================================================================
# 16. TokenManager — unsupported auth mode
# ===========================================================================


class TestTokenManagerUnsupportedMode:
    def test_validate_token_unsupported_mode_raises(self) -> None:
        cfg = _make_token_config(auth_mode=AuthMode.JWT)
        mgr = TokenManager(cfg)
        # Monkey-patch the config's auth_mode to a fake value
        cfg.auth_mode = "invalid_mode"  # type: ignore[assignment]
        with pytest.raises((PermissionError, ValueError)):
            mgr.validate_token("some.token.value")


# ===========================================================================
# 17. TokenManager — KMS token format validation (no actual KMS calls)
# ===========================================================================


class TestTokenManagerKMSTokenFormatValidation:
    """Tests that the KMS token format is validated before any KMS call."""

    def test_validate_kms_token_wrong_prefix_raises(self) -> None:
        cfg = TokenConfig(
            auth_mode=AuthMode.KMS_EXCHANGE,
            jwt_secret=SecretStr(_JWT_SECRET),
            kms_key_arn="arn:aws:kms:us-east-1:123:key/abc",
        )
        mgr = TokenManager(cfg)
        with pytest.raises(PermissionError, match="Invalid KMS token format"):
            mgr.validate_token("notbmkms.data.here")

    def test_validate_kms_token_wrong_part_count_raises(self) -> None:
        cfg = TokenConfig(
            auth_mode=AuthMode.KMS_EXCHANGE,
            jwt_secret=SecretStr(_JWT_SECRET),
            kms_key_arn="arn:aws:kms:us-east-1:123:key/abc",
        )
        mgr = TokenManager(cfg)
        with pytest.raises(PermissionError, match="Invalid KMS token format"):
            mgr.validate_token("bmkms.onlyonepart")

    def test_generate_kms_token_without_key_arn_raises(self) -> None:
        cfg = TokenConfig(
            auth_mode=AuthMode.KMS_EXCHANGE,
            jwt_secret=SecretStr(_JWT_SECRET),
            kms_key_arn=None,
        )
        mgr = TokenManager(cfg)
        with pytest.raises(ValueError, match="KMS key URI not configured"):
            mgr.generate_token(subject="u", tenant_id="t")


# ===========================================================================
# 18. OIDCProvider.validate_token — integration path with mocked JWT
# ===========================================================================


class TestOIDCProviderValidateToken:
    async def test_validate_token_missing_kid_raises(self) -> None:
        provider = OIDCProvider("https://accounts.example.com")
        import jwt as pyjwt

        # Build a token without a kid in the header
        payload = {
            "sub": "u",
            "tenant_id": "t",
            "iss": "https://accounts.example.com",
            "exp": time.time() + 3600,
            "iat": time.time(),
        }
        # Encode without kid
        token = pyjwt.encode(payload, "secret", algorithm="HS256")
        with pytest.raises(PermissionError, match="missing 'kid'"):
            await provider.validate_token(token)

    async def test_validate_token_invalid_header_raises(self) -> None:
        provider = OIDCProvider("https://accounts.example.com")
        with pytest.raises(PermissionError, match="Invalid token header"):
            await provider.validate_token("not.a.valid.jwt")

    async def test_validate_token_expired_raises(self) -> None:
        """Expired OIDC token raises PermissionError."""
        import jwt as pyjwt
        from cryptography.hazmat.primitives.asymmetric import rsa
        from jwt.algorithms import RSAAlgorithm

        # Generate a real RSA key pair for signing
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        public_key = private_key.public_key()

        # Create a JWK from the public key
        public_jwk = json.loads(RSAAlgorithm.to_jwk(public_key))
        public_jwk["kid"] = "test-kid"

        provider = OIDCProvider("https://accounts.example.com")
        provider._jwks = {"keys": [public_jwk]}
        provider._jwks_fetched_at = time.time()

        payload = {
            "sub": "u",
            "tenant_id": "t",
            "iss": "https://accounts.example.com",
            "exp": time.time() - 3600,  # already expired
            "iat": time.time() - 7200,
        }
        token = pyjwt.encode(payload, private_key, algorithm="RS256", headers={"kid": "test-kid"})

        with pytest.raises(PermissionError, match="expired"):
            await provider.validate_token(token)

    async def test_validate_token_issuer_mismatch_raises(self) -> None:
        """Token from wrong issuer raises PermissionError."""
        import jwt as pyjwt
        from cryptography.hazmat.primitives.asymmetric import rsa
        from jwt.algorithms import RSAAlgorithm

        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        public_key = private_key.public_key()
        public_jwk = json.loads(RSAAlgorithm.to_jwk(public_key))
        public_jwk["kid"] = "test-kid"

        provider = OIDCProvider("https://accounts.example.com")
        provider._jwks = {"keys": [public_jwk]}
        provider._jwks_fetched_at = time.time()

        payload = {
            "sub": "u",
            "tenant_id": "t",
            "iss": "https://evil.attacker.com",  # Wrong issuer
            "exp": time.time() + 3600,
            "iat": time.time(),
        }
        token = pyjwt.encode(payload, private_key, algorithm="RS256", headers={"kid": "test-kid"})

        with pytest.raises(PermissionError, match="issuer mismatch"):
            await provider.validate_token(token)

    async def test_validate_token_happy_path(self) -> None:
        """A valid RS256 OIDC token validates successfully."""
        import jwt as pyjwt
        from cryptography.hazmat.primitives.asymmetric import rsa
        from jwt.algorithms import RSAAlgorithm

        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        public_key = private_key.public_key()
        public_jwk = json.loads(RSAAlgorithm.to_jwk(public_key))
        public_jwk["kid"] = "test-kid"

        provider = OIDCProvider("https://accounts.example.com", audience="my-api")
        provider._jwks = {"keys": [public_jwk]}
        provider._jwks_fetched_at = time.time()

        payload = {
            "sub": "alice",
            "tenant_id": "tenant-123",
            "iss": "https://accounts.example.com",
            "aud": "my-api",
            "exp": time.time() + 3600,
            "iat": time.time(),
            "scope": "read write",
        }
        token = pyjwt.encode(payload, private_key, algorithm="RS256", headers={"kid": "test-kid"})

        claims = await provider.validate_token(token)
        assert claims.sub == "alice"
        assert claims.tenant_id == "tenant-123"

    async def test_validate_token_audience_mismatch_raises(self) -> None:
        """Token with wrong audience raises PermissionError."""
        import jwt as pyjwt
        from cryptography.hazmat.primitives.asymmetric import rsa
        from jwt.algorithms import RSAAlgorithm

        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        public_key = private_key.public_key()
        public_jwk = json.loads(RSAAlgorithm.to_jwk(public_key))
        public_jwk["kid"] = "test-kid"

        provider = OIDCProvider("https://accounts.example.com", audience="correct-audience")
        provider._jwks = {"keys": [public_jwk]}
        provider._jwks_fetched_at = time.time()

        payload = {
            "sub": "alice",
            "tenant_id": "t",
            "iss": "https://accounts.example.com",
            "aud": "wrong-audience",
            "exp": time.time() + 3600,
            "iat": time.time(),
            "scope": "read",
        }
        token = pyjwt.encode(payload, private_key, algorithm="RS256", headers={"kid": "test-kid"})

        with pytest.raises(PermissionError, match="audience mismatch"):
            await provider.validate_token(token)


# ===========================================================================
# 19. _validate_url_safe — invalid IP string from DNS
# ===========================================================================


class TestValidateUrlSafeInvalidIP:
    """Covers the ValueError branch for an invalid IP string from DNS resolution."""

    async def test_invalid_ip_string_from_dns_raises(self) -> None:
        """If DNS returns a non-parseable IP, PermissionError is raised."""
        with patch("asyncio.get_running_loop") as mock_loop:
            loop = MagicMock()
            future = asyncio.get_event_loop().create_future()
            # Simulate a non-parseable IP string in the addr_info 4-tuple
            future.set_result([(None, None, None, None, ("not-an-ip-address", 0))])
            loop.getaddrinfo = MagicMock(return_value=future)
            mock_loop.return_value = loop
            with pytest.raises(PermissionError, match="Invalid IP address"):
                await _validate_url_safe("https://accounts.example.com/disc", "accounts.example.com")


# ===========================================================================
# 20. OIDCProvider._fetch_discovery — _do_fetch inner error path
# ===========================================================================


class TestOIDCFetchDiscoveryDoFetchError:
    """Covers the PermissionError re-raise inside _do_fetch executor."""

    async def test_fetch_discovery_reraises_permission_error_from_executor(self) -> None:
        """PermissionError raised inside the executor is re-raised unwrapped."""
        provider = OIDCProvider("https://accounts.example.com")

        with patch("api.security._validate_url_safe", new=AsyncMock()):
            with patch("asyncio.get_running_loop") as mock_loop:
                loop = MagicMock()
                # Simulate the executor raising a PermissionError (SSRF path)
                loop.run_in_executor = AsyncMock(
                    side_effect=PermissionError("Failed to fetch OIDC discovery document")
                )
                mock_loop.return_value = loop

                with pytest.raises(PermissionError, match="Failed to fetch"):
                    await provider._fetch_discovery()


# ===========================================================================
# 21. OIDCProvider._fetch_jwks — _do_fetch inner error path
# ===========================================================================


class TestOIDCFetchJWKSDoFetchError:
    """Covers the PermissionError re-raise inside _fetch_jwks executor."""

    async def test_fetch_jwks_reraises_permission_error_from_executor(self) -> None:
        """PermissionError raised inside the JWKS executor is re-raised unwrapped."""
        provider = OIDCProvider("https://accounts.example.com")
        provider._discovery = {
            "issuer": "https://accounts.example.com",
            "jwks_uri": "https://accounts.example.com/jwks",
            "id_token_signing_alg_values_supported": ["RS256"],
        }

        with patch("api.security._validate_url_safe", new=AsyncMock()):
            with patch("asyncio.get_running_loop") as mock_loop:
                loop = MagicMock()
                loop.run_in_executor = AsyncMock(
                    side_effect=PermissionError("Failed to fetch JWKS")
                )
                mock_loop.return_value = loop

                with pytest.raises(PermissionError, match="Failed to fetch"):
                    await provider._fetch_jwks()

    async def test_fetch_jwks_invalid_json_raises(self) -> None:
        """Non-JSON JWKS response raises PermissionError with parse message."""
        provider = OIDCProvider("https://accounts.example.com")
        provider._discovery = {
            "issuer": "https://accounts.example.com",
            "jwks_uri": "https://accounts.example.com/jwks",
            "id_token_signing_alg_values_supported": ["RS256"],
        }

        with patch("api.security._validate_url_safe", new=AsyncMock()):
            with patch("asyncio.get_running_loop") as mock_loop:
                loop = MagicMock()
                loop.run_in_executor = AsyncMock(return_value=b"bad-json!!!")
                mock_loop.return_value = loop

                with pytest.raises(PermissionError, match="Failed to parse"):
                    await provider._fetch_jwks()


# ===========================================================================
# 22. OIDCProvider._get_signing_key — key found after forced refresh
# ===========================================================================


class TestOIDCGetSigningKeyAfterRefresh:
    """Covers the code path where the key is found after a forced JWKS refresh."""

    async def test_key_found_after_forced_refresh_returns_key(self) -> None:
        """When kid is not in cached JWKS but found after refresh, returns it."""
        provider = OIDCProvider("https://accounts.example.com")
        # Set a non-expired cache with a different key
        provider._jwks = {"keys": [{"kid": "old-key", "kty": "RSA"}]}
        provider._jwks_fetched_at = time.time()  # not expired

        refresh_count = 0

        async def fake_fetch_jwks() -> dict[str, Any]:
            nonlocal refresh_count
            refresh_count += 1
            # After refresh, the new key is available
            provider._jwks = {"keys": [{"kid": "new-rotated-key", "kty": "RSA"}]}
            provider._jwks_fetched_at = time.time()
            return provider._jwks  # type: ignore[return-value]

        with patch.object(provider, "_fetch_jwks", new=fake_fetch_jwks):
            key = await provider._get_signing_key("new-rotated-key")
            assert refresh_count == 1
            assert key["kid"] == "new-rotated-key"


# ===========================================================================
# 23. OIDCProvider.validate_token — bad JWK raises PermissionError
# ===========================================================================


class TestOIDCValidateTokenBadJWK:
    """Covers the RSAAlgorithm.from_jwk failure path."""

    async def test_bad_jwk_raises_permission_error(self) -> None:
        """A JWK that cannot be converted to an RSA key raises PermissionError."""
        import jwt as pyjwt

        # Create a real RS256 token with a real private key (just to get a valid header+kid)
        from cryptography.hazmat.primitives.asymmetric import rsa

        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        payload = {
            "sub": "u",
            "tenant_id": "t",
            "iss": "https://accounts.example.com",
            "exp": time.time() + 3600,
            "iat": time.time(),
        }
        token = pyjwt.encode(payload, private_key, algorithm="RS256", headers={"kid": "test-kid"})

        provider = OIDCProvider("https://accounts.example.com")
        # Inject a malformed JWK (not a real RSA key)
        provider._jwks = {"keys": [{"kid": "test-kid", "kty": "RSA", "n": "bad", "e": "bad"}]}
        provider._jwks_fetched_at = time.time()

        with pytest.raises(PermissionError, match="Failed to construct public key"):
            await provider.validate_token(token)


# ===========================================================================
# 24. OIDCProvider.validate_token — generic InvalidTokenError
# ===========================================================================


class TestOIDCValidateTokenInvalidTokenError:
    """Covers the jwt.InvalidTokenError catch-all branch."""

    async def test_general_invalid_token_error_raises_permission_error(self) -> None:
        """A jwt.InvalidTokenError not covered by specific branches maps to PermissionError."""
        import jwt as pyjwt
        from cryptography.hazmat.primitives.asymmetric import rsa
        from jwt.algorithms import RSAAlgorithm

        private_key_a = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        private_key_b = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        public_key_b = private_key_b.public_key()

        # Build a JWK from key B but sign the token with key A — invalid signature
        public_jwk = json.loads(RSAAlgorithm.to_jwk(public_key_b))
        public_jwk["kid"] = "test-kid"

        provider = OIDCProvider("https://accounts.example.com")
        provider._jwks = {"keys": [public_jwk]}
        provider._jwks_fetched_at = time.time()

        payload = {
            "sub": "u",
            "tenant_id": "t",
            "iss": "https://accounts.example.com",
            "exp": time.time() + 3600,
            "iat": time.time(),
        }
        # Sign with key A, but verification key is B — this triggers InvalidSignatureError
        token = pyjwt.encode(payload, private_key_a, algorithm="RS256", headers={"kid": "test-kid"})

        with pytest.raises(PermissionError, match="Invalid OIDC token"):
            await provider.validate_token(token)


# ===========================================================================
# 25. TokenManager — dev mode invalid claims JSON
# ===========================================================================


class TestTokenManagerDevModeInvalidClaimsJSON:
    """Covers the PermissionError for invalid claims JSON in dev token."""

    def test_validate_dev_token_invalid_claims_json_raises(self) -> None:
        mgr = TokenManager(_make_token_config(auth_mode=AuthMode.DEVELOPMENT))
        secret = _JWT_SECRET
        # Build a token with valid base64 and valid HMAC but non-JSON payload
        payload_bytes = b"this-is-not-json"
        b64 = base64.urlsafe_b64encode(payload_bytes).decode("ascii")
        sig = hmac.new(
            secret.encode("utf-8"),
            payload_bytes.decode("utf-8").encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        token = f"bmdev.{b64}.{sig}"
        with pytest.raises(PermissionError, match="Invalid token claims"):
            mgr.validate_token(token)


# ===========================================================================
# 26. TokenManager — _validate_oidc_token (async delegate path)
# ===========================================================================


class TestTokenManagerValidateOIDCAsync:
    """Covers _validate_oidc_token delegate inside TokenManager."""

    async def test_validate_oidc_token_delegates_to_provider(self) -> None:
        cfg = _make_token_config(
            auth_mode=AuthMode.OIDC_ONPREM,
            oidc_issuer_url="https://accounts.example.com",
        )
        mgr = TokenManager(cfg)
        expected_claims = TokenClaims(sub="oidc-user", tenant_id="tenant-oidc")

        # Mock the OIDCProvider.validate_token
        mock_provider = AsyncMock()
        mock_provider.validate_token = AsyncMock(return_value=expected_claims)

        with patch.object(mgr, "_get_oidc_provider", return_value=mock_provider):
            claims = await mgr._validate_oidc_token("some.oidc.token")
            mock_provider.validate_token.assert_awaited_once_with("some.oidc.token")
            assert claims.sub == "oidc-user"


# ===========================================================================
# 27. AzureKeyVaultProvider.close()
# ===========================================================================


class TestAzureKeyVaultProviderClose:
    def test_close_when_no_client_does_nothing(self) -> None:
        from api.security import AzureKeyVaultProvider

        provider = AzureKeyVaultProvider.__new__(AzureKeyVaultProvider)
        provider._client = None
        # Should not raise
        provider.close()

    def test_close_calls_client_close_and_sets_none(self) -> None:
        from api.security import AzureKeyVaultProvider

        provider = AzureKeyVaultProvider.__new__(AzureKeyVaultProvider)
        mock_client = MagicMock()
        provider._client = mock_client
        provider.close()
        mock_client.close.assert_called_once()
        assert provider._client is None


# ===========================================================================
# 28. TokenManager.generate_token — unsupported mode
# ===========================================================================


class TestTokenManagerGenerateUnsupportedMode:
    def test_generate_token_unsupported_mode_raises(self) -> None:
        cfg = _make_token_config(auth_mode=AuthMode.JWT)
        mgr = TokenManager(cfg)
        # Force an invalid mode value
        cfg.auth_mode = "completely_unsupported_mode"  # type: ignore[assignment]
        with pytest.raises(ValueError, match="Unsupported auth mode"):
            mgr.generate_token(subject="u", tenant_id="t")
