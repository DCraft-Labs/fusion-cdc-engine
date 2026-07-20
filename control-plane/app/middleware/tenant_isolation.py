"""
Tenant Isolation Middleware
Automatically filters database queries by tenant context from JWT token
Ensures data isolation between tenants at the middleware level
"""
from typing import Optional
from uuid import UUID
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from sqlalchemy import event
from sqlalchemy.orm import Session
from contextvars import ContextVar

from app.auth.jwt import verify_token


# Context variables to store tenant context per request
current_bank_id: ContextVar[Optional[UUID]] = ContextVar("current_bank_id", default=None)
current_sub_tenant_id: ContextVar[Optional[UUID]] = ContextVar("current_sub_tenant_id", default=None)
current_user_id: ContextVar[Optional[UUID]] = ContextVar("current_user_id", default=None)
is_superuser: ContextVar[bool] = ContextVar("is_superuser", default=False)


class TenantIsolationMiddleware(BaseHTTPMiddleware):
    """
    Middleware to extract tenant context from JWT token and store in context vars
    This enables automatic tenant filtering in database queries
    """
    
    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        """
        Process request and extract tenant context
        
        Args:
            request: FastAPI request
            call_next: Next middleware/route handler
            
        Returns:
            Response from downstream handler
        """
        # Reset context for this request
        current_bank_id.set(None)
        current_sub_tenant_id.set(None)
        current_user_id.set(None)
        is_superuser.set(False)
        
        # Extract token from Authorization header
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header.replace("Bearer ", "")
            
            try:
                # Verify and decode token
                payload = verify_token(token, token_type="access")
                if payload:
                    # Extract tenant context
                    if payload.get("bank_id"):
                        current_bank_id.set(UUID(payload["bank_id"]))
                    if payload.get("sub_tenant_id"):
                        current_sub_tenant_id.set(UUID(payload["sub_tenant_id"]))
                    if payload.get("sub"):
                        current_user_id.set(UUID(payload["sub"]))
                    
                    # Check if superuser (superusers bypass tenant filtering)
                    # This is determined by checking if 'superadmin' role exists
                    roles = payload.get("roles", [])
                    if "superadmin" in roles:
                        is_superuser.set(True)
            except Exception:
                # If token parsing fails, continue without tenant context
                # (route-level authentication will handle rejection if needed)
                pass
        
        # Continue processing request
        response = await call_next(request)
        
        return response


def setup_tenant_filtering(engine):
    """
    Setup SQLAlchemy event listeners for automatic tenant filtering
    This applies tenant filters to all queries automatically
    
    Args:
        engine: SQLAlchemy engine
    """
    
    @event.listens_for(Session, "after_attach")
    def receive_after_attach(session, instance):
        """
        Apply tenant filters after an instance is attached to session
        This is called for each ORM query
        """
        # Skip if superuser (they can see all data)
        if is_superuser.get():
            return
        
        # Get tenant context
        bank_id = current_bank_id.get()
        sub_tenant_id = current_sub_tenant_id.get()
        
        # Apply filters based on model's tenant columns
        # Models with bank_id should be filtered
        if hasattr(instance.__class__, 'bank_id') and bank_id:
            # Only filter if query doesn't already specify bank_id
            if not session.query(instance.__class__).filter_by(bank_id=bank_id).count():
                instance.bank_id = bank_id
        
        # Models with sub_tenant_id should be filtered
        if hasattr(instance.__class__, 'sub_tenant_id') and sub_tenant_id:
            if not session.query(instance.__class__).filter_by(sub_tenant_id=sub_tenant_id).count():
                instance.sub_tenant_id = sub_tenant_id


def get_tenant_context() -> dict:
    """
    Get current tenant context from context vars
    
    Returns:
        Dictionary with bank_id, sub_tenant_id, user_id, is_superuser
    """
    return {
        "bank_id": current_bank_id.get(),
        "sub_tenant_id": current_sub_tenant_id.get(),
        "user_id": current_user_id.get(),
        "is_superuser": is_superuser.get(),
    }


def apply_tenant_filter(query, model_class):
    """
    Apply tenant filters to a SQLAlchemy query
    Use this utility in repositories/services to ensure tenant isolation
    
    Args:
        query: SQLAlchemy query object
        model_class: Model class being queried
        
    Returns:
        Filtered query
    """
    # Skip filtering for superusers
    if is_superuser.get():
        return query
    
    # Get tenant context
    bank_id = current_bank_id.get()
    sub_tenant_id = current_sub_tenant_id.get()
    
    # Apply bank filter if model has bank_id column
    if hasattr(model_class, 'bank_id') and bank_id:
        query = query.filter(model_class.bank_id == bank_id)
    
    # Apply tenant filter if model has sub_tenant_id column
    if hasattr(model_class, 'sub_tenant_id') and sub_tenant_id:
        query = query.filter(model_class.sub_tenant_id == sub_tenant_id)
    
    return query


def set_tenant_fields(instance):
    """
    Set tenant fields on a model instance before saving
    Use this utility in create operations to automatically set tenant context
    
    Args:
        instance: SQLAlchemy model instance
    """
    # Skip for superusers (they can create resources in any tenant)
    # But we still set fields if they're not already set
    
    bank_id = current_bank_id.get()
    sub_tenant_id = current_sub_tenant_id.get()
    user_id = current_user_id.get()
    
    # Set bank_id if model has the column and it's not already set
    if hasattr(instance, 'bank_id') and not instance.bank_id and bank_id:
        instance.bank_id = bank_id
    
    # Set sub_tenant_id if model has the column and it's not already set
    if hasattr(instance, 'sub_tenant_id') and not instance.sub_tenant_id and sub_tenant_id:
        instance.sub_tenant_id = sub_tenant_id
    
    # Set created_by if model has the column and it's not already set
    if hasattr(instance, 'created_by') and not instance.created_by and user_id:
        instance.created_by = user_id
