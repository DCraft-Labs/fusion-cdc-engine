"""Authentication middleware"""

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response


class AuthMiddleware(BaseHTTPMiddleware):
    """
    Authentication middleware for JWT validation.
    
    TODO: Implement full JWT validation with Keycloak
    """
    
    async def dispatch(self, request: Request, call_next):
        # Skip auth for health and docs endpoints
        if request.url.path in ["/health", "/health/ready", "/health/live", "/api/docs", "/api/redoc", "/api/openapi.json", "/"]:
            return await call_next(request)
        
        # TODO: Implement JWT validation
        # 1. Extract Bearer token from Authorization header
        # 2. Validate JWT signature
        # 3. Extract user info (user_id, bank_id, sub_tenant_id, roles)
        # 4. Attach to request.state for use in endpoints
        
        response = await call_next(request)
        return response
