"""Tests for the license manager module."""

from __future__ import annotations

import base64
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from core_engine.license.feature_flags import Feature, LicenseTier
from core_engine.license.license_manager import (
    LicenseExpiredError,
    LicenseFile,
    LicenseLimitExceededError,
    LicenseManager,
    LicenseVerificationError,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_license_data(
    *,
    tier: str = "enterprise",
    expires_delta_days: int = 365,
    max_models: int = 500,
    max_plan_runs_per_day: int = 100,
    ai_enabled: bool = True,
    features: list[str] | None = None,
    tenant_id: str = "test-tenant",
) -> dict:
    """Build a license data dict (unsigned)."""
    now = datetime.now(timezone.utc)
    return {
        "license_id": "lic-test-001",
        "tenant_id": tenant_id,
        "tier": tier,
        "issued_at": now.isoformat(),
        "expires_at": (now + timedelta(days=expires_delta_days)).isoformat(),
        "max_models": max_models,
        "max_plan_runs_per_day": max_plan_runs_per_day,
        "ai_enabled": ai_enabled,
        "features": features or [],
        "signature": "",
    }


def _sign_license_data(
    data: dict,
    private_key_bytes: bytes,
) -> dict:
    """Sign a license data dict with Ed25519."""
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    payload = {k: v for k, v in data.items() if k != "signature"}
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    message = canonical.encode("utf-8")

    private_key = Ed25519PrivateKey.from_private_bytes(private_key_bytes)
    signature = private_key.sign(message)
    data["signature"] = base64.b64encode(signature).decode("ascii")
    return data


def _generate_test_keypair() -> tuple[bytes, bytes]:
    """Generate an Ed25519 keypair for testing."""
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from cryptography.hazmat.primitives.serialization import (
        Encoding,
        NoEncryption,
        PrivateFormat,
        PublicFormat,
    )

    private_key = Ed25519PrivateKey.generate()
    public_key = private_key.public_key()
    return (
        private_key.private_bytes(Encoding.Raw, PrivateFormat.Raw, NoEncryption()),
        public_key.public_bytes(Encoding.Raw, PublicFormat.Raw),
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def keypair():
    """Generate a fresh Ed25519 keypair for each test."""
    return _generate_test_keypair()


@pytest.fixture
def private_key(keypair):
    return keypair[0]


@pytest.fixture
def public_key(keypair):
    return keypair[1]


# ---------------------------------------------------------------------------
# License file parsing
# ---------------------------------------------------------------------------


class TestLicenseFileParsing:
    """Verify LicenseFile model parsing."""

    def test_parses_valid_license(self) -> None:
        data = _make_license_data()
        lf = LicenseFile(**data)
        assert lf.license_id == "lic-test-001"
        assert lf.tier == LicenseTier.ENTERPRISE

    def test_parses_community_tier(self) -> None:
        data = _make_license_data(tier="community")
        lf = LicenseFile(**data)
        assert lf.tier == LicenseTier.COMMUNITY

    def test_parses_team_tier(self) -> None:
        data = _make_license_data(tier="team")
        lf = LicenseFile(**data)
        assert lf.tier == LicenseTier.TEAM

    def test_default_values(self) -> None:
        data = _make_license_data()
        del data["max_models"]
        del data["max_plan_runs_per_day"]
        lf = LicenseFile(**data)
        assert lf.max_models == 50
        assert lf.max_plan_runs_per_day == 10


# ---------------------------------------------------------------------------
# Signature verification
# ---------------------------------------------------------------------------


class TestSignatureVerification:
    """Verify Ed25519 signature checking."""

    def test_valid_signature(self, private_key, public_key) -> None:
        data = _make_license_data()
        data = _sign_license_data(data, private_key)

        manager = LicenseManager(public_key_bytes=public_key)
        lf = manager.load_license_from_string(json.dumps(data))
        assert lf.license_id == "lic-test-001"
        assert manager.effective_tier == LicenseTier.ENTERPRISE

    def test_invalid_signature_rejected(self, public_key) -> None:
        data = _make_license_data()
        data["signature"] = base64.b64encode(b"invalid" * 10).decode()

        manager = LicenseManager(public_key_bytes=public_key)
        with pytest.raises(LicenseVerificationError, match="verification failed"):
            manager.load_license_from_string(json.dumps(data))

    def test_tampered_data_rejected(self, private_key, public_key) -> None:
        data = _make_license_data()
        data = _sign_license_data(data, private_key)

        # Tamper with the tier after signing.
        data["tier"] = "community"

        manager = LicenseManager(public_key_bytes=public_key)
        with pytest.raises(LicenseVerificationError, match="verification failed"):
            manager.load_license_from_string(json.dumps(data))

    def test_missing_signature_rejected(self, public_key) -> None:
        data = _make_license_data()
        data["signature"] = ""

        manager = LicenseManager(public_key_bytes=public_key)
        with pytest.raises(LicenseVerificationError, match="no signature"):
            manager.load_license_from_string(json.dumps(data))

    def test_wrong_key_rejected(self, private_key) -> None:
        _, other_public = _generate_test_keypair()

        data = _make_license_data()
        data = _sign_license_data(data, private_key)

        manager = LicenseManager(public_key_bytes=other_public)
        with pytest.raises(LicenseVerificationError, match="verification failed"):
            manager.load_license_from_string(json.dumps(data))

    def test_no_public_key_skips_verification(self) -> None:
        """When no public key is set, verification is skipped (testing mode)."""
        data = _make_license_data()
        manager = LicenseManager(public_key_bytes=None)
        lf = manager.load_license_from_string(json.dumps(data))
        assert lf.license_id == "lic-test-001"


# ---------------------------------------------------------------------------
# Expiry checks
# ---------------------------------------------------------------------------


class TestExpiry:
    """Verify license expiration enforcement."""

    def test_expired_license_rejected(self) -> None:
        data = _make_license_data(expires_delta_days=-1)
        manager = LicenseManager(public_key_bytes=None)
        with pytest.raises(LicenseExpiredError, match="expired"):
            manager.load_license_from_string(json.dumps(data))

    def test_valid_license_accepted(self) -> None:
        data = _make_license_data(expires_delta_days=30)
        manager = LicenseManager(public_key_bytes=None)
        lf = manager.load_license_from_string(json.dumps(data))
        assert lf is not None

    def test_just_expired_rejected(self) -> None:
        data = _make_license_data()
        # Set expires_at to 1 second ago.
        data["expires_at"] = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()

        manager = LicenseManager(public_key_bytes=None)
        with pytest.raises(LicenseExpiredError):
            manager.load_license_from_string(json.dumps(data))


# ---------------------------------------------------------------------------
# Entitlement checks
# ---------------------------------------------------------------------------


class TestEntitlements:
    """Verify feature entitlement enforcement."""

    def test_community_has_core_features(self) -> None:
        data = _make_license_data(tier="community")
        manager = LicenseManager(public_key_bytes=None)
        manager.load_license_from_string(json.dumps(data))

        assert manager.check_entitlement(Feature.PLAN_GENERATE)
        assert manager.check_entitlement(Feature.PLAN_APPLY)
        assert manager.check_entitlement(Feature.MODEL_LOADING)
        assert manager.check_entitlement(Feature.LINEAGE_VIEW)

    def test_community_lacks_ai(self) -> None:
        data = _make_license_data(tier="community")
        manager = LicenseManager(public_key_bytes=None)
        manager.load_license_from_string(json.dumps(data))

        assert not manager.check_entitlement(Feature.AI_ADVISORY)

    def test_team_has_ai(self) -> None:
        data = _make_license_data(tier="team")
        manager = LicenseManager(public_key_bytes=None)
        manager.load_license_from_string(json.dumps(data))

        assert manager.check_entitlement(Feature.AI_ADVISORY)
        assert manager.check_entitlement(Feature.COST_TRACKING)
        assert manager.check_entitlement(Feature.API_ACCESS)

    def test_team_lacks_enterprise_features(self) -> None:
        data = _make_license_data(tier="team")
        manager = LicenseManager(public_key_bytes=None)
        manager.load_license_from_string(json.dumps(data))

        assert not manager.check_entitlement(Feature.MULTI_TENANT)
        assert not manager.check_entitlement(Feature.SSO_OIDC)
        assert not manager.check_entitlement(Feature.FAILURE_PREDICTION)

    def test_enterprise_has_all_features(self) -> None:
        data = _make_license_data(tier="enterprise")
        manager = LicenseManager(public_key_bytes=None)
        manager.load_license_from_string(json.dumps(data))

        for feature in Feature:
            assert manager.check_entitlement(feature), f"Missing: {feature.value}"

    def test_require_entitlement_raises(self) -> None:
        data = _make_license_data(tier="community")
        manager = LicenseManager(public_key_bytes=None)
        manager.load_license_from_string(json.dumps(data))

        with pytest.raises(LicenseLimitExceededError, match="higher license tier"):
            manager.require_entitlement(Feature.AI_ADVISORY)

    def test_require_entitlement_passes(self) -> None:
        data = _make_license_data(tier="enterprise")
        manager = LicenseManager(public_key_bytes=None)
        manager.load_license_from_string(json.dumps(data))

        # Should not raise.
        manager.require_entitlement(Feature.AI_ADVISORY)
        manager.require_entitlement(Feature.MULTI_TENANT)


# ---------------------------------------------------------------------------
# Limit checks
# ---------------------------------------------------------------------------


class TestLimits:
    """Verify model and plan run limits."""

    def test_model_limit_within(self) -> None:
        data = _make_license_data(max_models=100)
        manager = LicenseManager(public_key_bytes=None)
        manager.load_license_from_string(json.dumps(data))

        assert manager.check_model_limit(50)
        assert manager.check_model_limit(100)

    def test_model_limit_exceeded(self) -> None:
        data = _make_license_data(max_models=100)
        manager = LicenseManager(public_key_bytes=None)
        manager.load_license_from_string(json.dumps(data))

        assert not manager.check_model_limit(101)

    def test_plan_limit_within(self) -> None:
        data = _make_license_data(max_plan_runs_per_day=50)
        manager = LicenseManager(public_key_bytes=None)
        manager.load_license_from_string(json.dumps(data))

        assert manager.check_daily_plan_limit(50)

    def test_plan_limit_exceeded(self) -> None:
        data = _make_license_data(max_plan_runs_per_day=50)
        manager = LicenseManager(public_key_bytes=None)
        manager.load_license_from_string(json.dumps(data))

        assert not manager.check_daily_plan_limit(51)

    def test_community_defaults(self) -> None:
        manager = LicenseManager(public_key_bytes=None)
        # No license loaded -- community defaults.
        assert manager.check_model_limit(50)
        assert not manager.check_model_limit(51)
        assert manager.check_daily_plan_limit(10)
        assert not manager.check_daily_plan_limit(11)


# ---------------------------------------------------------------------------
# AI enablement
# ---------------------------------------------------------------------------


class TestAIEnabled:
    """Verify AI feature gating."""

    def test_ai_enabled_enterprise(self) -> None:
        data = _make_license_data(tier="enterprise", ai_enabled=True)
        manager = LicenseManager(public_key_bytes=None)
        manager.load_license_from_string(json.dumps(data))

        assert manager.is_ai_enabled()

    def test_ai_disabled_by_flag(self) -> None:
        data = _make_license_data(tier="enterprise", ai_enabled=False)
        manager = LicenseManager(public_key_bytes=None)
        manager.load_license_from_string(json.dumps(data))

        assert not manager.is_ai_enabled()

    def test_ai_disabled_community(self) -> None:
        data = _make_license_data(tier="community", ai_enabled=True)
        manager = LicenseManager(public_key_bytes=None)
        manager.load_license_from_string(json.dumps(data))

        # Community tier doesn't have AI_ADVISORY feature.
        assert not manager.is_ai_enabled()

    def test_no_license_no_ai(self) -> None:
        manager = LicenseManager(public_key_bytes=None)
        assert not manager.is_ai_enabled()


# ---------------------------------------------------------------------------
# License info
# ---------------------------------------------------------------------------


class TestLicenseInfo:
    """Verify license info API response."""

    def test_info_with_license(self) -> None:
        data = _make_license_data(tier="team")
        manager = LicenseManager(public_key_bytes=None)
        manager.load_license_from_string(json.dumps(data))

        info = manager.get_license_info()
        assert info["tier"] == "team"
        assert info["licensed"] is True
        assert info["license_id"] == "lic-test-001"

    def test_info_without_license(self) -> None:
        manager = LicenseManager(public_key_bytes=None)
        info = manager.get_license_info()
        assert info["tier"] == "community"
        assert info["licensed"] is False
        assert info["max_models"] == 50

    def test_effective_tier_default(self) -> None:
        manager = LicenseManager(public_key_bytes=None)
        assert manager.effective_tier == LicenseTier.COMMUNITY


# ---------------------------------------------------------------------------
# File loading
# ---------------------------------------------------------------------------


class TestFileLoading:
    """Verify license file loading from disk."""

    def test_load_from_file(self, tmp_path: Path) -> None:
        data = _make_license_data()
        license_path = tmp_path / "license.json"
        license_path.write_text(json.dumps(data))

        manager = LicenseManager(public_key_bytes=None)
        lf = manager.load_license(license_path)
        assert lf.license_id == "lic-test-001"

    def test_file_not_found(self, tmp_path: Path) -> None:
        manager = LicenseManager(public_key_bytes=None)
        with pytest.raises(FileNotFoundError):
            manager.load_license(tmp_path / "missing.json")

    def test_load_signed_file(self, tmp_path: Path, private_key, public_key) -> None:
        data = _make_license_data()
        data = _sign_license_data(data, private_key)

        license_path = tmp_path / "license.json"
        license_path.write_text(json.dumps(data))

        manager = LicenseManager(public_key_bytes=public_key)
        lf = manager.load_license(license_path)
        assert lf.tier == LicenseTier.ENTERPRISE
        assert manager.effective_tier == LicenseTier.ENTERPRISE
