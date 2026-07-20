"""
Initialize authentication data
Create default roles, permissions, and superadmin user
"""
import asyncio
import sys
from pathlib import Path
from uuid import UUID

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy.orm import Session
from sqlalchemy import select

from app.database import SessionLocal, engine
from app.models.auth import Role, Permission, User
from app.auth.password import hash_password
from app.config import settings


# Default permissions for each resource
PERMISSIONS = [
    # Connector Definitions
    {"permission_name": "connector_definitions:read", "display_name": "Read Connector Definitions", "resource": "connector_definitions", "action": "read", "scope": "global"},
    {"permission_name": "connector_definitions:create", "display_name": "Create Connector Definitions", "resource": "connector_definitions", "action": "create", "scope": "global"},
    {"permission_name": "connector_definitions:update", "display_name": "Update Connector Definitions", "resource": "connector_definitions", "action": "update", "scope": "global"},
    {"permission_name": "connector_definitions:delete", "display_name": "Delete Connector Definitions", "resource": "connector_definitions", "action": "delete", "scope": "global"},
    
    # Sources
    {"permission_name": "sources:read", "display_name": "Read Sources", "resource": "sources", "action": "read", "scope": "tenant"},
    {"permission_name": "sources:create", "display_name": "Create Sources", "resource": "sources", "action": "create", "scope": "tenant"},
    {"permission_name": "sources:update", "display_name": "Update Sources", "resource": "sources", "action": "update", "scope": "tenant"},
    {"permission_name": "sources:delete", "display_name": "Delete Sources", "resource": "sources", "action": "delete", "scope": "tenant"},
    
    # Destinations
    {"permission_name": "destinations:read", "display_name": "Read Destinations", "resource": "destinations", "action": "read", "scope": "tenant"},
    {"permission_name": "destinations:create", "display_name": "Create Destinations", "resource": "destinations", "action": "create", "scope": "tenant"},
    {"permission_name": "destinations:update", "display_name": "Update Destinations", "resource": "destinations", "action": "update", "scope": "tenant"},
    {"permission_name": "destinations:delete", "display_name": "Delete Destinations", "resource": "destinations", "action": "delete", "scope": "tenant"},
    
    # Connections
    {"permission_name": "connections:read", "display_name": "Read Connections", "resource": "connections", "action": "read", "scope": "tenant"},
    {"permission_name": "connections:create", "display_name": "Create Connections", "resource": "connections", "action": "create", "scope": "tenant"},
    {"permission_name": "connections:update", "display_name": "Update Connections", "resource": "connections", "action": "update", "scope": "tenant"},
    {"permission_name": "connections:delete", "display_name": "Delete Connections", "resource": "connections", "action": "delete", "scope": "tenant"},
    {"permission_name": "connections:start", "display_name": "Start Connections", "resource": "connections", "action": "start", "scope": "tenant"},
    {"permission_name": "connections:stop", "display_name": "Stop Connections", "resource": "connections", "action": "stop", "scope": "tenant"},
    
    # Streams
    {"permission_name": "streams:read", "display_name": "Read Streams", "resource": "streams", "action": "read", "scope": "tenant"},
    {"permission_name": "streams:create", "display_name": "Create Streams", "resource": "streams", "action": "create", "scope": "tenant"},
    {"permission_name": "streams:update", "display_name": "Update Streams", "resource": "streams", "action": "update", "scope": "tenant"},
    {"permission_name": "streams:delete", "display_name": "Delete Streams", "resource": "streams", "action": "delete", "scope": "tenant"},
    
    # Transformations
    {"permission_name": "transformations:read", "display_name": "Read Transformations", "resource": "transformations", "action": "read", "scope": "tenant"},
    {"permission_name": "transformations:create", "display_name": "Create Transformations", "resource": "transformations", "action": "create", "scope": "tenant"},
    {"permission_name": "transformations:update", "display_name": "Update Transformations", "resource": "transformations", "action": "update", "scope": "tenant"},
    {"permission_name": "transformations:delete", "display_name": "Delete Transformations", "resource": "transformations", "action": "delete", "scope": "tenant"},
    
    # Data Quality Policies
    {"permission_name": "dq_policies:read", "display_name": "Read DQ Policies", "resource": "dq_policies", "action": "read", "scope": "tenant"},
    {"permission_name": "dq_policies:create", "display_name": "Create DQ Policies", "resource": "dq_policies", "action": "create", "scope": "tenant"},
    {"permission_name": "dq_policies:update", "display_name": "Update DQ Policies", "resource": "dq_policies", "action": "update", "scope": "tenant"},
    {"permission_name": "dq_policies:delete", "display_name": "Delete DQ Policies", "resource": "dq_policies", "action": "delete", "scope": "tenant"},
    
    # UDFs
    {"permission_name": "udfs:read", "display_name": "Read UDFs", "resource": "udfs", "action": "read", "scope": "tenant"},
    {"permission_name": "udfs:create", "display_name": "Create UDFs", "resource": "udfs", "action": "create", "scope": "tenant"},
    {"permission_name": "udfs:update", "display_name": "Update UDFs", "resource": "udfs", "action": "update", "scope": "tenant"},
    {"permission_name": "udfs:delete", "display_name": "Delete UDFs", "resource": "udfs", "action": "delete", "scope": "tenant"},
    
    # Monitoring
    {"permission_name": "monitoring:read", "display_name": "Read Monitoring Data", "resource": "monitoring", "action": "read", "scope": "tenant"},
    
    # Schema Evolution
    {"permission_name": "schema_evolution:read", "display_name": "Read Schema Evolution", "resource": "schema_evolution", "action": "read", "scope": "tenant"},
    {"permission_name": "schema_evolution:approve", "display_name": "Approve Schema Changes", "resource": "schema_evolution", "action": "approve", "scope": "tenant"},
    
    # Users (admin permissions)
    {"permission_name": "users:read", "display_name": "Read Users", "resource": "users", "action": "read", "scope": "tenant"},
    {"permission_name": "users:create", "display_name": "Create Users", "resource": "users", "action": "create", "scope": "tenant"},
    {"permission_name": "users:update", "display_name": "Update Users", "resource": "users", "action": "update", "scope": "tenant"},
    {"permission_name": "users:delete", "display_name": "Delete Users", "resource": "users", "action": "delete", "scope": "tenant"},
    
    # Roles (admin permissions)
    {"permission_name": "roles:read", "display_name": "Read Roles", "resource": "roles", "action": "read", "scope": "tenant"},
    {"permission_name": "roles:create", "display_name": "Create Roles", "resource": "roles", "action": "create", "scope": "tenant"},
    {"permission_name": "roles:update", "display_name": "Update Roles", "resource": "roles", "action": "update", "scope": "tenant"},
    {"permission_name": "roles:delete", "display_name": "Delete Roles", "resource": "roles", "action": "delete", "scope": "tenant"},
]


# Default roles with their permissions
ROLES = [
    {
        "role_name": "superadmin",
        "display_name": "Super Administrator",
        "description": "Full system access across all banks and tenants",
        "role_level": "superadmin",
        "is_system_role": True,
        "permissions": ["*"],  # All permissions
    },
    {
        "role_name": "bank_admin",
        "display_name": "Bank Administrator",
        "description": "Full access to all tenants within a bank",
        "role_level": "bank_admin",
        "is_system_role": True,
        "permissions": [
            "sources:read", "sources:create", "sources:update", "sources:delete",
            "destinations:read", "destinations:create", "destinations:update", "destinations:delete",
            "connections:read", "connections:create", "connections:update", "connections:delete",
            "connections:start", "connections:stop",
            "streams:read", "streams:create", "streams:update", "streams:delete",
            "transformations:read", "transformations:create", "transformations:update", "transformations:delete",
            "dq_policies:read", "dq_policies:create", "dq_policies:update", "dq_policies:delete",
            "udfs:read", "udfs:create", "udfs:update", "udfs:delete",
            "monitoring:read",
            "schema_evolution:read", "schema_evolution:approve",
            "users:read", "users:create", "users:update", "users:delete",
            "roles:read", "roles:create", "roles:update", "roles:delete",
        ],
    },
    {
        "role_name": "tenant_admin",
        "display_name": "Tenant Administrator",
        "description": "Full access within a specific tenant",
        "role_level": "tenant_admin",
        "is_system_role": True,
        "permissions": [
            "sources:read", "sources:create", "sources:update", "sources:delete",
            "destinations:read", "destinations:create", "destinations:update", "destinations:delete",
            "connections:read", "connections:create", "connections:update", "connections:delete",
            "connections:start", "connections:stop",
            "streams:read", "streams:create", "streams:update", "streams:delete",
            "transformations:read", "transformations:create", "transformations:update", "transformations:delete",
            "dq_policies:read", "dq_policies:create", "dq_policies:update", "dq_policies:delete",
            "udfs:read", "udfs:create", "udfs:update", "udfs:delete",
            "monitoring:read",
            "schema_evolution:read", "schema_evolution:approve",
            "users:read", "users:create", "users:update",
        ],
    },
    {
        "role_name": "user",
        "display_name": "User",
        "description": "Standard user with create/update access",
        "role_level": "user",
        "is_system_role": True,
        "permissions": [
            "sources:read", "sources:create", "sources:update",
            "destinations:read", "destinations:create", "destinations:update",
            "connections:read", "connections:create", "connections:update",
            "connections:start", "connections:stop",
            "streams:read", "streams:create", "streams:update",
            "transformations:read", "transformations:create", "transformations:update",
            "dq_policies:read", "dq_policies:create", "dq_policies:update",
            "udfs:read", "udfs:create", "udfs:update",
            "monitoring:read",
            "schema_evolution:read",
        ],
    },
    {
        "role_name": "viewer",
        "display_name": "Viewer",
        "description": "Read-only access",
        "role_level": "viewer",
        "is_system_role": True,
        "permissions": [
            "sources:read",
            "destinations:read",
            "connections:read",
            "streams:read",
            "transformations:read",
            "dq_policies:read",
            "udfs:read",
            "monitoring:read",
            "schema_evolution:read",
        ],
    },
]


def create_permissions(db: Session) -> dict:
    """Create default permissions"""
    permission_map = {}
    
    print("Creating permissions...")
    for perm_data in PERMISSIONS:
        # Check if permission already exists
        stmt = select(Permission).where(Permission.permission_name == perm_data["permission_name"])
        existing = db.execute(stmt).scalar_one_or_none()
        
        if existing:
            print(f"  ✓ Permission '{perm_data['permission_name']}' already exists")
            permission_map[perm_data["permission_name"]] = existing
        else:
            permission = Permission(**perm_data)
            db.add(permission)
            db.flush()
            permission_map[perm_data["permission_name"]] = permission
            print(f"  ✓ Created permission '{perm_data['permission_name']}'")
    
    db.commit()
    return permission_map


def create_roles(db: Session, permission_map: dict):
    """Create default roles with permissions"""
    print("\nCreating roles...")
    
    for role_data in ROLES:
        # Extract permissions list
        permission_names = role_data.pop("permissions")
        
        # Check if role already exists
        stmt = select(Role).where(Role.role_name == role_data["role_name"])
        existing = db.execute(stmt).scalar_one_or_none()
        
        if existing:
            print(f"  ✓ Role '{role_data['role_name']}' already exists")
            role = existing
        else:
            role = Role(**role_data)
            db.add(role)
            db.flush()
            print(f"  ✓ Created role '{role_data['role_name']}'")
        
        # Assign permissions
        if "*" in permission_names:
            # Superadmin gets all permissions
            role.permissions = list(permission_map.values())
            print(f"    ✓ Assigned all permissions to '{role.role_name}'")
        else:
            # Assign specific permissions
            role.permissions = [permission_map[perm_name] for perm_name in permission_names if perm_name in permission_map]
            print(f"    ✓ Assigned {len(role.permissions)} permissions to '{role.role_name}'")
    
    db.commit()


def create_superadmin(db: Session):
    """Create default superadmin user"""
    print("\nCreating superadmin user...")
    
    # Check if superadmin already exists
    stmt = select(User).where(User.username == "admin")
    existing = db.execute(stmt).scalar_one_or_none()
    
    if existing:
        print("  ✓ Superadmin user 'admin' already exists")
        return
    
    # Hash default password
    default_password = "Admin@123"
    password_hash = hash_password(default_password)
    
    # Default bank and sub-tenant for superadmin
    default_bank_id = UUID("00000000-0000-4000-a000-000000000001")
    default_tenant_id = UUID("00000000-0000-4000-a000-000000000002")

    # Create superadmin user
    admin_user = User(
        username="admin",
        email="admin@fusion.dev",
        password_hash=password_hash,
        first_name="Super",
        last_name="Admin",
        is_superuser=True,
        is_active=True,
        is_email_verified=True,
        bank_id=default_bank_id,
        sub_tenant_id=default_tenant_id,
    )
    
    db.add(admin_user)
    db.flush()
    
    # Assign superadmin role
    stmt = select(Role).where(Role.role_name == "superadmin")
    superadmin_role = db.execute(stmt).scalar_one()
    admin_user.roles.append(superadmin_role)
    
    db.commit()
    
    print(f"  ✓ Created superadmin user 'admin'")
    print(f"    Username: admin")
    print(f"    Password: {default_password}")
    print(f"    ⚠️  IMPORTANT: Change this password immediately in production!")


def main():
    """Initialize authentication data"""
    print("=" * 60)
    print("Initializing Authentication Data")
    print("=" * 60)
    
    db = SessionLocal()
    
    try:
        # Create permissions
        permission_map = create_permissions(db)
        
        # Create roles
        create_roles(db, permission_map)
        
        # Create superadmin user
        create_superadmin(db)
        
        print("\n" + "=" * 60)
        print("✅ Authentication data initialized successfully!")
        print("=" * 60)
        
        # Also seed connector definitions
        from scripts.init_connector_data import seed_connectors
        seed_connectors(db)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
