"""Comprehensive RBAC tests -- role x permission matrix.

Validates the complete RBAC permission matrix for all five roles
(VIEWER, OPERATOR, ENGINEER, ADMIN, SERVICE) against every permission
in the system.  Also verifies SERVICE role restrictions on require_role
and the behaviour when role claims are missing from tokens.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from api.middleware.rbac import (
    ROLE_PERMISSIONS,
    Permission,
    Role,
    get_user_role,
    require_permission,
    require_role,
    role_has_permission,
)

# ---------------------------------------------------------------------------
# Role x Permission matrix
# ---------------------------------------------------------------------------


class TestRolePermissionMatrix:
    """Verify every role has exactly the correct permissions."""

    def test_viewer_has_only_read_permissions(self) -> None:
        """VIEWER should have exactly READ_PLANS, READ_MODELS, READ_RUNS, READ_TEST_RESULTS."""
        expected = {
            Permission.READ_PLANS,
            Permission.READ_MODELS,
            Permission.READ_RUNS,
            Permission.READ_TEST_RESULTS,
        }
        assert ROLE_PERMISSIONS[Role.VIEWER] == expected

    def test_viewer_cannot_write(self) -> None:
        """VIEWER must not have any write/mutate permissions."""
        write_perms = [
            Permission.CREATE_PLANS,
            Permission.APPROVE_PLANS,
            Permission.APPLY_PLANS,
            Permission.WRITE_MODELS,
            Permission.CREATE_BACKFILLS,
            Permission.MANAGE_CREDENTIALS,
            Permission.MANAGE_SETTINGS,
            Permission.MANAGE_WEBHOOKS,
            Permission.RUN_TESTS,
            Permission.MANAGE_ENVIRONMENTS,
            Permission.CREATE_EPHEMERAL_ENVS,
            Permission.PROMOTE_ENVIRONMENTS,
            Permission.VIEW_ANALYTICS,
            Permission.VIEW_REPORTS,
            Permission.MANAGE_HEALTH,
            Permission.VIEW_INVOICES,
        ]
        for perm in write_perms:
            assert not role_has_permission(Role.VIEWER, perm), f"VIEWER should not have {perm.value}"

    def test_operator_has_viewer_plus_approve_backfill_audit(self) -> None:
        """OPERATOR should have VIEWER perms + APPROVE_PLANS, CREATE_BACKFILLS, READ_AUDIT."""
        expected = ROLE_PERMISSIONS[Role.VIEWER] | {
            Permission.APPROVE_PLANS,
            Permission.CREATE_BACKFILLS,
            Permission.READ_AUDIT,
        }
        assert ROLE_PERMISSIONS[Role.OPERATOR] == expected

    def test_operator_cannot_create_plans(self) -> None:
        """OPERATOR must not create or apply plans."""
        assert not role_has_permission(Role.OPERATOR, Permission.CREATE_PLANS)
        assert not role_has_permission(Role.OPERATOR, Permission.APPLY_PLANS)
        assert not role_has_permission(Role.OPERATOR, Permission.WRITE_MODELS)

    def test_engineer_has_operator_plus_create_apply_write(self) -> None:
        """ENGINEER should have OPERATOR perms + CREATE_PLANS, APPLY_PLANS, WRITE_MODELS, etc."""
        expected = ROLE_PERMISSIONS[Role.OPERATOR] | {
            Permission.CREATE_PLANS,
            Permission.APPLY_PLANS,
            Permission.WRITE_MODELS,
            Permission.CREATE_EPHEMERAL_ENVS,
            Permission.RUN_TESTS,
        }
        assert ROLE_PERMISSIONS[Role.ENGINEER] == expected

    def test_engineer_cannot_manage_credentials(self) -> None:
        """ENGINEER must not manage credentials, settings, or webhooks."""
        assert not role_has_permission(Role.ENGINEER, Permission.MANAGE_CREDENTIALS)
        assert not role_has_permission(Role.ENGINEER, Permission.MANAGE_SETTINGS)
        assert not role_has_permission(Role.ENGINEER, Permission.MANAGE_WEBHOOKS)
        assert not role_has_permission(Role.ENGINEER, Permission.MANAGE_ENVIRONMENTS)

    def test_admin_has_all_permissions(self) -> None:
        """ADMIN should have every single permission defined in the system."""
        admin_perms = ROLE_PERMISSIONS[Role.ADMIN]
        all_perms = set(Permission)
        assert admin_perms == all_perms, f"ADMIN missing: {all_perms - admin_perms}"

    def test_admin_is_strict_superset_of_engineer(self) -> None:
        """ADMIN permissions must be a strict superset of ENGINEER."""
        assert ROLE_PERMISSIONS[Role.ENGINEER] < ROLE_PERMISSIONS[Role.ADMIN]

    def test_service_has_only_machine_operations(self) -> None:
        """SERVICE should have exactly READ_PLANS, READ_MODELS, READ_RUNS, CREATE_PLANS, APPLY_PLANS."""
        expected = {
            Permission.READ_PLANS,
            Permission.READ_MODELS,
            Permission.READ_RUNS,
            Permission.CREATE_PLANS,
            Permission.APPLY_PLANS,
        }
        assert ROLE_PERMISSIONS[Role.SERVICE] == expected

    def test_service_cannot_approve(self) -> None:
        """SERVICE must not approve plans (human-only operation)."""
        assert not role_has_permission(Role.SERVICE, Permission.APPROVE_PLANS)

    def test_service_cannot_manage_anything(self) -> None:
        """SERVICE must not manage credentials, settings, or webhooks."""
        assert not role_has_permission(Role.SERVICE, Permission.MANAGE_CREDENTIALS)
        assert not role_has_permission(Role.SERVICE, Permission.MANAGE_SETTINGS)
        assert not role_has_permission(Role.SERVICE, Permission.MANAGE_WEBHOOKS)
        assert not role_has_permission(Role.SERVICE, Permission.MANAGE_ENVIRONMENTS)
        assert not role_has_permission(Role.SERVICE, Permission.MANAGE_HEALTH)

    def test_service_cannot_write_models(self) -> None:
        """SERVICE must not write models or create backfills."""
        assert not role_has_permission(Role.SERVICE, Permission.WRITE_MODELS)
        assert not role_has_permission(Role.SERVICE, Permission.CREATE_BACKFILLS)

    def test_service_is_not_in_hierarchy(self) -> None:
        """SERVICE (10) is outside the 0-3 hierarchy range."""
        assert Role.SERVICE.value == 10
        assert Role.SERVICE > Role.ADMIN

    @pytest.mark.parametrize("permission", list(Permission))
    def test_every_permission_assigned_to_admin(self, permission: Permission) -> None:
        """Every defined permission must be granted to ADMIN."""
        assert role_has_permission(Role.ADMIN, permission), f"ADMIN should have {permission.value}"

    def test_hierarchy_is_strict_superset_chain(self) -> None:
        """VIEWER < OPERATOR < ENGINEER < ADMIN in permission set sizes."""
        ordered = [Role.VIEWER, Role.OPERATOR, Role.ENGINEER, Role.ADMIN]
        for i in range(1, len(ordered)):
            lower = ROLE_PERMISSIONS[ordered[i - 1]]
            higher = ROLE_PERMISSIONS[ordered[i]]
            assert lower < higher, f"{ordered[i].name} should be a strict superset of {ordered[i - 1].name}"


# ---------------------------------------------------------------------------
# SERVICE role restrictions on require_role
# ---------------------------------------------------------------------------


class TestServiceRoleRestrictions:
    """Verify SERVICE cannot bypass role-level checks via numeric comparison."""

    def test_require_role_admin_rejects_service(self) -> None:
        """require_role(Role.ADMIN) with SERVICE role raises 403."""
        guard = require_role(Role.ADMIN)

        with pytest.raises(HTTPException) as exc_info:
            guard(role=Role.SERVICE)

        assert exc_info.value.status_code == 403
        assert "Service accounts must use permission-based auth" in exc_info.value.detail

    def test_require_role_viewer_rejects_service(self) -> None:
        """require_role(Role.VIEWER) with SERVICE role raises 403."""
        guard = require_role(Role.VIEWER)

        with pytest.raises(HTTPException) as exc_info:
            guard(role=Role.SERVICE)

        assert exc_info.value.status_code == 403
        assert "Service accounts" in exc_info.value.detail

    def test_require_role_operator_rejects_service(self) -> None:
        """require_role(Role.OPERATOR) with SERVICE role raises 403."""
        guard = require_role(Role.OPERATOR)

        with pytest.raises(HTTPException) as exc_info:
            guard(role=Role.SERVICE)

        assert exc_info.value.status_code == 403

    def test_require_role_engineer_rejects_service(self) -> None:
        """require_role(Role.ENGINEER) with SERVICE role raises 403."""
        guard = require_role(Role.ENGINEER)

        with pytest.raises(HTTPException) as exc_info:
            guard(role=Role.SERVICE)

        assert exc_info.value.status_code == 403

    def test_require_permission_read_plans_passes_for_service(self) -> None:
        """require_permission(READ_PLANS) with SERVICE role passes."""
        guard = require_permission(Permission.READ_PLANS)
        # Should return the role without raising
        result = guard(role=Role.SERVICE)
        assert result == Role.SERVICE

    def test_require_permission_create_plans_passes_for_service(self) -> None:
        """require_permission(CREATE_PLANS) with SERVICE role passes."""
        guard = require_permission(Permission.CREATE_PLANS)
        result = guard(role=Role.SERVICE)
        assert result == Role.SERVICE

    def test_require_permission_apply_plans_passes_for_service(self) -> None:
        """require_permission(APPLY_PLANS) with SERVICE role passes."""
        guard = require_permission(Permission.APPLY_PLANS)
        result = guard(role=Role.SERVICE)
        assert result == Role.SERVICE

    def test_require_permission_manage_settings_rejects_service(self) -> None:
        """require_permission(MANAGE_SETTINGS) with SERVICE role raises 403."""
        guard = require_permission(Permission.MANAGE_SETTINGS)

        with pytest.raises(HTTPException) as exc_info:
            guard(role=Role.SERVICE)

        assert exc_info.value.status_code == 403
        assert "permission denied" in exc_info.value.detail.lower()

    def test_require_permission_approve_plans_rejects_service(self) -> None:
        """require_permission(APPROVE_PLANS) with SERVICE role raises 403."""
        guard = require_permission(Permission.APPROVE_PLANS)

        with pytest.raises(HTTPException) as exc_info:
            guard(role=Role.SERVICE)

        assert exc_info.value.status_code == 403

    def test_require_permission_manage_credentials_rejects_service(self) -> None:
        """require_permission(MANAGE_CREDENTIALS) with SERVICE role raises 403."""
        guard = require_permission(Permission.MANAGE_CREDENTIALS)

        with pytest.raises(HTTPException) as exc_info:
            guard(role=Role.SERVICE)

        assert exc_info.value.status_code == 403

    def test_require_permission_write_models_rejects_service(self) -> None:
        """require_permission(WRITE_MODELS) with SERVICE role raises 403."""
        guard = require_permission(Permission.WRITE_MODELS)

        with pytest.raises(HTTPException) as exc_info:
            guard(role=Role.SERVICE)

        assert exc_info.value.status_code == 403

    def test_require_permission_read_audit_rejects_service(self) -> None:
        """require_permission(READ_AUDIT) with SERVICE role raises 403."""
        guard = require_permission(Permission.READ_AUDIT)

        with pytest.raises(HTTPException) as exc_info:
            guard(role=Role.SERVICE)

        assert exc_info.value.status_code == 403


# ---------------------------------------------------------------------------
# require_role with normal roles
# ---------------------------------------------------------------------------


class TestRequireRoleNormalRoles:
    """Verify require_role works correctly for hierarchical roles."""

    def test_admin_passes_admin_check(self) -> None:
        """ADMIN passes require_role(ADMIN)."""
        guard = require_role(Role.ADMIN)
        assert guard(role=Role.ADMIN) == Role.ADMIN

    def test_engineer_passes_operator_check(self) -> None:
        """ENGINEER passes require_role(OPERATOR) due to higher rank."""
        guard = require_role(Role.OPERATOR)
        assert guard(role=Role.ENGINEER) == Role.ENGINEER

    def test_viewer_fails_operator_check(self) -> None:
        """VIEWER fails require_role(OPERATOR)."""
        guard = require_role(Role.OPERATOR)

        with pytest.raises(HTTPException) as exc_info:
            guard(role=Role.VIEWER)

        assert exc_info.value.status_code == 403
        assert "Insufficient role" in exc_info.value.detail

    def test_operator_fails_engineer_check(self) -> None:
        """OPERATOR fails require_role(ENGINEER)."""
        guard = require_role(Role.ENGINEER)

        with pytest.raises(HTTPException) as exc_info:
            guard(role=Role.OPERATOR)

        assert exc_info.value.status_code == 403

    def test_viewer_passes_viewer_check(self) -> None:
        """VIEWER passes require_role(VIEWER)."""
        guard = require_role(Role.VIEWER)
        assert guard(role=Role.VIEWER) == Role.VIEWER


# ---------------------------------------------------------------------------
# Missing role claim
# ---------------------------------------------------------------------------


class TestMissingRoleClaim:
    """Verify missing role claim is handled correctly by get_user_role."""

    def test_authenticated_without_role_returns_401(self) -> None:
        """Authenticated request (has sub) without role claim raises 401."""
        request = MagicMock()
        # Simulate authenticated request: sub is set, role is not
        request.state = MagicMock(spec=[])
        request.state.sub = "user@example.com"
        # Make getattr(request.state, "role", None) return None
        # by ensuring 'role' attribute doesn't exist
        del_attrs = set()
        original_getattr = type(request.state).__getattribute__

        def custom_getattr(self, name):
            if name == "role":
                raise AttributeError(name)
            if name == "sub":
                return "user@example.com"
            return original_getattr(self, name)

        # Use a simpler approach: mock the state object properly
        mock_state = type("State", (), {"sub": "user@example.com"})()
        request.state = mock_state

        with pytest.raises(HTTPException) as exc_info:
            get_user_role(request)

        assert exc_info.value.status_code == 401
        assert "Missing role claim" in exc_info.value.detail

    def test_unauthenticated_without_role_defaults_to_viewer(self) -> None:
        """Unauthenticated request (no sub, no role) defaults to VIEWER."""
        request = MagicMock()
        # Simulate unauthenticated request: neither sub nor role is set
        mock_state = type("State", (), {})()
        request.state = mock_state

        role = get_user_role(request)
        assert role == Role.VIEWER

    def test_valid_role_claim_parsed_correctly(self) -> None:
        """Valid role claim is parsed and returned."""
        request = MagicMock()
        mock_state = type("State", (), {"sub": "user@example.com", "role": "engineer"})()
        request.state = mock_state

        role = get_user_role(request)
        assert role == Role.ENGINEER

    def test_invalid_role_claim_raises_403(self) -> None:
        """Invalid role claim string raises 403."""
        request = MagicMock()
        mock_state = type("State", (), {"sub": "user@example.com", "role": "superadmin"})()
        request.state = mock_state

        with pytest.raises(HTTPException) as exc_info:
            get_user_role(request)

        assert exc_info.value.status_code == 403
        assert "Unrecognised role" in exc_info.value.detail

    def test_service_role_claim_parsed(self) -> None:
        """SERVICE role claim is correctly parsed."""
        request = MagicMock()
        mock_state = type("State", (), {"sub": "svc-account", "role": "service"})()
        request.state = mock_state

        role = get_user_role(request)
        assert role == Role.SERVICE

    def test_case_insensitive_role_parsing(self) -> None:
        """Role parsing is case-insensitive."""
        request = MagicMock()
        mock_state = type("State", (), {"sub": "user@example.com", "role": "ADMIN"})()
        request.state = mock_state

        role = get_user_role(request)
        assert role == Role.ADMIN

    def test_whitespace_trimmed_from_role(self) -> None:
        """Leading/trailing whitespace in role claim is trimmed."""
        request = MagicMock()
        mock_state = type("State", (), {"sub": "user@example.com", "role": "  operator  "})()
        request.state = mock_state

        role = get_user_role(request)
        assert role == Role.OPERATOR


# ---------------------------------------------------------------------------
# require_permission matrix for all roles
# ---------------------------------------------------------------------------


class TestRequirePermissionMatrix:
    """Exhaustive check that require_permission enforces the ROLE_PERMISSIONS map."""

    @pytest.mark.parametrize(
        "role,permission,should_pass",
        [
            # VIEWER can read
            (Role.VIEWER, Permission.READ_PLANS, True),
            (Role.VIEWER, Permission.READ_MODELS, True),
            (Role.VIEWER, Permission.READ_RUNS, True),
            (Role.VIEWER, Permission.READ_TEST_RESULTS, True),
            # VIEWER cannot write
            (Role.VIEWER, Permission.CREATE_PLANS, False),
            (Role.VIEWER, Permission.APPLY_PLANS, False),
            (Role.VIEWER, Permission.APPROVE_PLANS, False),
            (Role.VIEWER, Permission.WRITE_MODELS, False),
            (Role.VIEWER, Permission.MANAGE_CREDENTIALS, False),
            # OPERATOR can approve and read audit
            (Role.OPERATOR, Permission.APPROVE_PLANS, True),
            (Role.OPERATOR, Permission.CREATE_BACKFILLS, True),
            (Role.OPERATOR, Permission.READ_AUDIT, True),
            (Role.OPERATOR, Permission.CREATE_PLANS, False),
            # ENGINEER can create/apply
            (Role.ENGINEER, Permission.CREATE_PLANS, True),
            (Role.ENGINEER, Permission.APPLY_PLANS, True),
            (Role.ENGINEER, Permission.WRITE_MODELS, True),
            (Role.ENGINEER, Permission.RUN_TESTS, True),
            (Role.ENGINEER, Permission.MANAGE_CREDENTIALS, False),
            (Role.ENGINEER, Permission.MANAGE_SETTINGS, False),
            # ADMIN can do everything
            (Role.ADMIN, Permission.MANAGE_CREDENTIALS, True),
            (Role.ADMIN, Permission.MANAGE_SETTINGS, True),
            (Role.ADMIN, Permission.MANAGE_WEBHOOKS, True),
            (Role.ADMIN, Permission.VIEW_ANALYTICS, True),
            (Role.ADMIN, Permission.MANAGE_ENVIRONMENTS, True),
            # SERVICE limited
            (Role.SERVICE, Permission.READ_PLANS, True),
            (Role.SERVICE, Permission.CREATE_PLANS, True),
            (Role.SERVICE, Permission.APPLY_PLANS, True),
            (Role.SERVICE, Permission.APPROVE_PLANS, False),
            (Role.SERVICE, Permission.MANAGE_SETTINGS, False),
            (Role.SERVICE, Permission.WRITE_MODELS, False),
        ],
    )
    def test_permission_enforcement(self, role: Role, permission: Permission, should_pass: bool) -> None:
        """Verify require_permission correctly allows or denies based on role x permission."""
        guard = require_permission(permission)

        if should_pass:
            result = guard(role=role)
            assert result == role
        else:
            with pytest.raises(HTTPException) as exc_info:
                guard(role=role)
            assert exc_info.value.status_code == 403
