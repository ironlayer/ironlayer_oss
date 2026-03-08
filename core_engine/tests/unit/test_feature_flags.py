"""Tests for license feature flags and tier mappings."""

from __future__ import annotations

import pytest

from core_engine.license.feature_flags import (
    Feature,
    LicenseTier,
    TIER_FEATURES,
    get_required_tier,
    get_tier_features,
    is_feature_enabled,
)


class TestTierHierarchy:
    """Verify that higher tiers include all lower-tier features."""

    def test_community_is_subset_of_team(self) -> None:
        community = TIER_FEATURES[LicenseTier.COMMUNITY]
        team = TIER_FEATURES[LicenseTier.TEAM]
        assert community.issubset(team)

    def test_team_is_subset_of_enterprise(self) -> None:
        team = TIER_FEATURES[LicenseTier.TEAM]
        enterprise = TIER_FEATURES[LicenseTier.ENTERPRISE]
        assert team.issubset(enterprise)

    def test_enterprise_contains_all_features(self) -> None:
        enterprise = TIER_FEATURES[LicenseTier.ENTERPRISE]
        for feature in Feature:
            assert feature in enterprise, f"Enterprise missing: {feature.value}"

    def test_community_is_smallest(self) -> None:
        sizes = {tier: len(features) for tier, features in TIER_FEATURES.items()}
        assert sizes[LicenseTier.COMMUNITY] < sizes[LicenseTier.TEAM]
        assert sizes[LicenseTier.TEAM] < sizes[LicenseTier.ENTERPRISE]


class TestIsFeatureEnabled:
    """Verify is_feature_enabled checks."""

    def test_core_feature_all_tiers(self) -> None:
        for tier in LicenseTier:
            assert is_feature_enabled(tier, Feature.PLAN_GENERATE)

    def test_ai_community_disabled(self) -> None:
        assert not is_feature_enabled(LicenseTier.COMMUNITY, Feature.AI_ADVISORY)

    def test_ai_team_enabled(self) -> None:
        assert is_feature_enabled(LicenseTier.TEAM, Feature.AI_ADVISORY)

    def test_multi_tenant_community_disabled(self) -> None:
        assert not is_feature_enabled(LicenseTier.COMMUNITY, Feature.MULTI_TENANT)

    def test_multi_tenant_team_disabled(self) -> None:
        assert not is_feature_enabled(LicenseTier.TEAM, Feature.MULTI_TENANT)

    def test_multi_tenant_enterprise_enabled(self) -> None:
        assert is_feature_enabled(LicenseTier.ENTERPRISE, Feature.MULTI_TENANT)


class TestGetTierFeatures:
    """Verify get_tier_features returns the correct set."""

    def test_community_features(self) -> None:
        features = get_tier_features(LicenseTier.COMMUNITY)
        assert Feature.PLAN_GENERATE in features
        assert Feature.AI_ADVISORY not in features

    def test_team_features(self) -> None:
        features = get_tier_features(LicenseTier.TEAM)
        assert Feature.AI_ADVISORY in features
        assert Feature.MULTI_TENANT not in features

    def test_enterprise_features(self) -> None:
        features = get_tier_features(LicenseTier.ENTERPRISE)
        assert Feature.MULTI_TENANT in features
        assert Feature.SSO_OIDC in features


class TestGetRequiredTier:
    """Verify minimum tier lookup for features."""

    def test_core_feature_is_community(self) -> None:
        assert get_required_tier(Feature.PLAN_GENERATE) == LicenseTier.COMMUNITY

    def test_ai_is_team(self) -> None:
        assert get_required_tier(Feature.AI_ADVISORY) == LicenseTier.TEAM

    def test_multi_tenant_is_enterprise(self) -> None:
        assert get_required_tier(Feature.MULTI_TENANT) == LicenseTier.ENTERPRISE

    def test_sso_is_enterprise(self) -> None:
        assert get_required_tier(Feature.SSO_OIDC) == LicenseTier.ENTERPRISE

    def test_cost_tracking_is_team(self) -> None:
        assert get_required_tier(Feature.COST_TRACKING) == LicenseTier.TEAM

    def test_all_features_have_tier(self) -> None:
        for feature in Feature:
            tier = get_required_tier(feature)
            assert tier in LicenseTier, f"No tier for {feature.value}"
