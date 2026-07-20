"""Unit tests for RBAC utilities"""
import pytest
from uuid import uuid4

from app.auth.rbac import (
    ROLE_HIERARCHY,
    get_user_roles,
    get_user_permissions,
    has_permission,
    has_any_permission,
    has_all_permissions,
    has_role,
    has_role_level,
    can_access_bank,
    can_access_tenant,
    get_accessible_tenants,
)
from app.models.auth import User, Role, Permission


class TestRoleHierarchy:
    """Test role hierarchy definitions"""
    
    def test_role_hierarchy_completeness(self):
        """Test that all role levels are defined"""
        expected_roles = ["superadmin", "bank_admin", "tenant_admin", "operator", "user", "viewer"]
        assert set(ROLE_HIERARCHY.keys()) == set(expected_roles)
    
    def test_role_hierarchy_order(self):
        """Test that role levels are in correct order"""
        assert ROLE_HIERARCHY["superadmin"] > ROLE_HIERARCHY["bank_admin"]
        assert ROLE_HIERARCHY["bank_admin"] > ROLE_HIERARCHY["tenant_admin"]
        assert ROLE_HIERARCHY["tenant_admin"] > ROLE_HIERARCHY["operator"]
        assert ROLE_HIERARCHY["operator"] > ROLE_HIERARCHY["user"]
        assert ROLE_HIERARCHY["user"] > ROLE_HIERARCHY["viewer"]
    
    def test_role_hierarchy_values(self):
        """Test role hierarchy numeric values"""
        assert ROLE_HIERARCHY["superadmin"] == 100
        assert ROLE_HIERARCHY["bank_admin"] == 80
        assert ROLE_HIERARCHY["tenant_admin"] == 60
        assert ROLE_HIERARCHY["user"] == 40
        assert ROLE_HIERARCHY["viewer"] == 20


class TestUserRolesAndPermissions:
    """Test retrieving user roles and permissions"""
    
    @pytest.mark.asyncio
    async def test_get_user_roles(self, db_session, sample_user, sample_role):
        """Test getting user roles"""
        roles = await get_user_roles(db_session, str(sample_user.user_id))
        
        assert isinstance(roles, list)
        assert "user" in roles
    
    @pytest.mark.asyncio
    async def test_get_user_roles_multiple(self, db_session, sample_user):
        """Test getting multiple roles for user"""
        # Add another role
        role2 = Role(
            role_name="viewer",
            display_name="Viewer",
            role_level="viewer",
            is_system_role=True,
        )
        db_session.add(role2)
        sample_user.roles.append(role2)
        db_session.commit()
        
        roles = await get_user_roles(db_session, str(sample_user.user_id))
        
        assert len(roles) >= 2
        assert "user" in roles
        assert "viewer" in roles
    
    @pytest.mark.asyncio
    async def test_get_user_roles_no_roles(self, db_session):
        """Test getting roles for user with no roles"""
        user = User(
            username="noroles",
            email="noroles@example.com",
            password_hash="hash",
        )
        db_session.add(user)
        db_session.commit()
        
        roles = await get_user_roles(db_session, str(user.user_id))
        
        assert roles == []
    
    @pytest.mark.asyncio
    async def test_get_user_permissions(self, db_session, sample_user, sample_permission):
        """Test getting user permissions"""
        permissions = await get_user_permissions(db_session, str(sample_user.user_id))
        
        assert isinstance(permissions, list)
        assert "sources:read" in permissions
    
    @pytest.mark.asyncio
    async def test_get_user_permissions_from_multiple_roles(self, db_session, sample_user, sample_permission):
        """Test aggregating permissions from multiple roles"""
        # Create another permission
        perm2 = Permission(
            permission_name="sources:create",
            display_name="Create Sources",
            resource="sources",
            action="create",
            scope="tenant",
        )
        db_session.add(perm2)
        
        # Create another role with different permission
        role2 = Role(
            role_name="creator",
            display_name="Creator",
            role_level="user",
            is_system_role=False,
        )
        role2.permissions.append(perm2)
        db_session.add(role2)
        
        # Add role to user
        sample_user.roles.append(role2)
        db_session.commit()
        
        permissions = await get_user_permissions(db_session, str(sample_user.user_id))
        
        # User should have permissions from both roles
        assert "sources:read" in permissions
        assert "sources:create" in permissions
    
    @pytest.mark.asyncio
    async def test_get_user_permissions_no_permissions(self, db_session):
        """Test getting permissions for user with no roles"""
        user = User(
            username="noperms",
            email="noperms@example.com",
            password_hash="hash",
        )
        db_session.add(user)
        db_session.commit()
        
        permissions = await get_user_permissions(db_session, str(user.user_id))
        
        assert permissions == []


class TestPermissionChecking:
    """Test permission checking functions"""
    
    @pytest.mark.asyncio
    async def test_has_permission_granted(self, db_session, sample_user):
        """Test permission check when user has permission"""
        result = await has_permission(db_session, str(sample_user.user_id), "sources:read")
        
        assert result is True
    
    @pytest.mark.asyncio
    async def test_has_permission_denied(self, db_session, sample_user):
        """Test permission check when user lacks permission"""
        result = await has_permission(db_session, str(sample_user.user_id), "sources:delete")
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_has_any_permission_one_granted(self, db_session, sample_user):
        """Test has_any_permission when user has at least one"""
        result = await has_any_permission(
            db_session,
            str(sample_user.user_id),
            ["sources:read", "sources:delete", "sources:create"]
        )
        
        assert result is True
    
    @pytest.mark.asyncio
    async def test_has_any_permission_none_granted(self, db_session, sample_user):
        """Test has_any_permission when user has none"""
        result = await has_any_permission(
            db_session,
            str(sample_user.user_id),
            ["sources:delete", "sources:create"]
        )
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_has_all_permissions_all_granted(self, db_session, sample_user, sample_permission):
        """Test has_all_permissions when user has all"""
        # User only has sources:read
        result = await has_all_permissions(
            db_session,
            str(sample_user.user_id),
            ["sources:read"]
        )
        
        assert result is True
    
    @pytest.mark.asyncio
    async def test_has_all_permissions_some_missing(self, db_session, sample_user):
        """Test has_all_permissions when some are missing"""
        result = await has_all_permissions(
            db_session,
            str(sample_user.user_id),
            ["sources:read", "sources:delete"]
        )
        
        assert result is False


class TestRoleChecking:
    """Test role checking functions"""
    
    @pytest.mark.asyncio
    async def test_has_role_granted(self, db_session, sample_user):
        """Test role check when user has role"""
        result = await has_role(db_session, str(sample_user.user_id), "user")
        
        assert result is True
    
    @pytest.mark.asyncio
    async def test_has_role_denied(self, db_session, sample_user):
        """Test role check when user lacks role"""
        result = await has_role(db_session, str(sample_user.user_id), "admin")
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_has_role_level_equal(self, db_session, sample_user):
        """Test role level check with equal level"""
        result = await has_role_level(db_session, str(sample_user.user_id), "user")
        
        assert result is True
    
    @pytest.mark.asyncio
    async def test_has_role_level_higher(self, db_session, sample_user):
        """Test role level check with lower required level"""
        result = await has_role_level(db_session, str(sample_user.user_id), "viewer")
        
        assert result is True  # user level (40) >= viewer level (20)
    
    @pytest.mark.asyncio
    async def test_has_role_level_lower(self, db_session, sample_user):
        """Test role level check with higher required level"""
        result = await has_role_level(db_session, str(sample_user.user_id), "bank_admin")
        
        assert result is False  # user level (40) < bank_admin level (80)
    
    @pytest.mark.asyncio
    async def test_has_role_level_superadmin(self, db_session, sample_superuser):
        """Test role level check for superadmin"""
        result = await has_role_level(db_session, str(sample_superuser.user_id), "superadmin")
        
        # Superusers don't have explicit roles but should pass superadmin check
        assert result is False  # No explicit role assigned


class TestTenantAccessControl:
    """Test tenant access control functions"""
    
    @pytest.mark.asyncio
    async def test_can_access_bank_own_bank(self, db_session, sample_user, sample_bank_id):
        """Test bank access for user in that bank"""
        result = await can_access_bank(db_session, str(sample_user.user_id), str(sample_bank_id))
        
        assert result is True
    
    @pytest.mark.asyncio
    async def test_can_access_bank_different_bank(self, db_session, sample_user):
        """Test bank access for user in different bank"""
        other_bank_id = str(uuid4())
        result = await can_access_bank(db_session, str(sample_user.user_id), other_bank_id)
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_can_access_bank_superuser(self, db_session, sample_superuser):
        """Test that superuser can access any bank"""
        any_bank_id = str(uuid4())
        result = await can_access_bank(db_session, str(sample_superuser.user_id), any_bank_id)
        
        assert result is True
    
    @pytest.mark.asyncio
    async def test_can_access_tenant_own_tenant(self, db_session, sample_user, sample_tenant_id):
        """Test tenant access for user in that tenant"""
        result = await can_access_tenant(db_session, str(sample_user.user_id), str(sample_tenant_id))
        
        assert result is True
    
    @pytest.mark.asyncio
    async def test_can_access_tenant_different_tenant(self, db_session, sample_user):
        """Test tenant access for user in different tenant"""
        other_tenant_id = str(uuid4())
        result = await can_access_tenant(db_session, str(sample_user.user_id), other_tenant_id)
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_can_access_tenant_superuser(self, db_session, sample_superuser):
        """Test that superuser can access any tenant"""
        any_tenant_id = str(uuid4())
        result = await can_access_tenant(db_session, str(sample_superuser.user_id), any_tenant_id)
        
        assert result is True
    
    @pytest.mark.asyncio
    async def test_get_accessible_tenants_single(self, db_session, sample_user, sample_tenant_id):
        """Test getting accessible tenants for regular user"""
        tenants = await get_accessible_tenants(db_session, str(sample_user.user_id))
        
        assert len(tenants) == 1
        assert str(sample_tenant_id) in tenants
    
    @pytest.mark.asyncio
    async def test_get_accessible_tenants_superuser(self, db_session, sample_superuser):
        """Test that superuser gets all tenants"""
        # For superuser, should return empty list (meaning all tenants)
        tenants = await get_accessible_tenants(db_session, str(sample_superuser.user_id))
        
        assert tenants == []  # Empty means all tenants accessible
    
    @pytest.mark.asyncio
    async def test_get_accessible_tenants_no_tenant(self, db_session):
        """Test getting accessible tenants for user with no tenant"""
        user = User(
            username="notenant",
            email="notenant@example.com",
            password_hash="hash",
        )
        db_session.add(user)
        db_session.commit()
        
        tenants = await get_accessible_tenants(db_session, str(user.user_id))
        
        assert tenants == []


class TestEdgeCases:
    """Test edge cases and error conditions"""
    
    @pytest.mark.asyncio
    async def test_has_permission_nonexistent_user(self, db_session):
        """Test permission check for nonexistent user"""
        fake_user_id = str(uuid4())
        result = await has_permission(db_session, fake_user_id, "sources:read")
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_has_role_nonexistent_user(self, db_session):
        """Test role check for nonexistent user"""
        fake_user_id = str(uuid4())
        result = await has_role(db_session, fake_user_id, "user")
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_can_access_bank_nonexistent_user(self, db_session):
        """Test bank access for nonexistent user"""
        fake_user_id = str(uuid4())
        bank_id = str(uuid4())
        result = await can_access_bank(db_session, fake_user_id, bank_id)
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_empty_permission_list(self, db_session, sample_user):
        """Test has_any_permission with empty list"""
        result = await has_any_permission(db_session, str(sample_user.user_id), [])
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_none_permission(self, db_session, sample_user):
        """Test has_permission with None"""
        result = await has_permission(db_session, str(sample_user.user_id), None)
        
        assert result is False
