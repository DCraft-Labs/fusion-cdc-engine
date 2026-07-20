"""
Spec §5 (P5-3): OIDC / external-IdP token validation for the Fusion control-plane.

If the environment variable OIDC_ISSUER is set, the control-plane will accept
both its own local JWTs (for service-to-service calls / legacy clients) AND
tokens issued by the external IdP.

Supported env vars:
  OIDC_ISSUER      — e.g. https://accounts.google.com or https://login.microsoftonline.com/<tenant>/v2.0
  OIDC_CLIENT_ID   — required; must appear in the 'aud' claim of the token
  OIDC_JWKS_URI    — optional; if not set, derived as {OIDC_ISSUER}/.well-known/jwks.json
  OIDC_ALGORITHMS  — comma-separated list of allowed signing algorithms (default: RS256)

Integration with get_current_user:
  In app/auth/dependencies.py, call try_oidc_token() before the local JWT path:

      payload = try_oidc_token(token)
      if payload is None:
          payload = verify_token(token, token_type="access")   # local JWT fallback

The function is intentionally synchronous so it can be called from FastAPI
dependency-injection without async ceremony.  JWKS keys are cached in-process
with a 10-minute TTL via a simple dict cache.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, Optional

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration (loaded lazily from environment)
# ---------------------------------------------------------------------------

_OIDC_ISSUER: Optional[str] = None
_OIDC_CLIENT_ID: Optional[str] = None
_OIDC_JWKS_URI: Optional[str] = None
_OIDC_ALGORITHMS: list[str] = ["RS256"]


def _load_config() -> None:
    global _OIDC_ISSUER, _OIDC_CLIENT_ID, _OIDC_JWKS_URI, _OIDC_ALGORITHMS
    _OIDC_ISSUER = os.environ.get("OIDC_ISSUER", "").strip() or None
    _OIDC_CLIENT_ID = os.environ.get("OIDC_CLIENT_ID", "").strip() or None
    raw_uri = os.environ.get("OIDC_JWKS_URI", "").strip()
    _OIDC_JWKS_URI = raw_uri or (f"{_OIDC_ISSUER}/.well-known/jwks.json" if _OIDC_ISSUER else None)
    algos = os.environ.get("OIDC_ALGORITHMS", "RS256").strip()
    _OIDC_ALGORITHMS = [a.strip() for a in algos.split(",") if a.strip()]


_load_config()


def oidc_enabled() -> bool:
    """Return True if OIDC validation is configured."""
    return bool(_OIDC_ISSUER and _OIDC_CLIENT_ID)


# ---------------------------------------------------------------------------
# JWKS key cache
# ---------------------------------------------------------------------------

_JWKS_CACHE: Dict[str, Any] = {}  # {"keys": [...], "fetched_at": float}
_JWKS_TTL = 600  # seconds


def _get_jwks() -> list[dict]:
    """Fetch and cache JWKS from the IdP.  Refreshes every _JWKS_TTL seconds."""
    now = time.monotonic()
    if _JWKS_CACHE.get("fetched_at", 0) + _JWKS_TTL > now:
        return _JWKS_CACHE.get("keys", [])

    try:
        import urllib.request
        import json as _json
        with urllib.request.urlopen(_OIDC_JWKS_URI, timeout=5) as resp:  # noqa: S310
            data = _json.loads(resp.read())
        keys = data.get("keys", [])
        _JWKS_CACHE["keys"] = keys
        _JWKS_CACHE["fetched_at"] = now
        log.debug("OIDC: fetched %d JWKS keys from %s", len(keys), _OIDC_JWKS_URI)
        return keys
    except Exception as exc:
        log.warning("OIDC: failed to fetch JWKS from %s: %s", _OIDC_JWKS_URI, exc)
        return _JWKS_CACHE.get("keys", [])


# ---------------------------------------------------------------------------
# Token validation
# ---------------------------------------------------------------------------

def validate_oidc_token(token: str) -> Optional[Dict[str, Any]]:
    """
    Validate an OIDC JWT token and return the decoded claims dict on success,
    or None if validation fails or OIDC is not configured.

    Claims verified:
      - Signature via JWKS
      - iss == OIDC_ISSUER
      - aud contains OIDC_CLIENT_ID
      - exp / nbf
    """
    if not oidc_enabled():
        return None

    try:
        from jose import jwt as jose_jwt, JWTError
    except ImportError:
        log.warning("OIDC: python-jose not installed — cannot validate OIDC tokens")
        return None

    keys = _get_jwks()
    if not keys:
        log.warning("OIDC: no JWKS keys available — skipping OIDC validation")
        return None

    last_error: Exception | None = None
    for key in keys:
        try:
            claims = jose_jwt.decode(
                token,
                key,
                algorithms=_OIDC_ALGORITHMS,
                issuer=_OIDC_ISSUER,
                audience=_OIDC_CLIENT_ID,
                options={"verify_exp": True, "verify_nbf": True},
            )
            log.debug("OIDC: token validated for sub=%s", claims.get("sub"))
            return claims
        except JWTError as exc:
            last_error = exc
            continue
        except Exception as exc:
            last_error = exc
            continue

    log.debug("OIDC: all JWKS keys tried, last error: %s", last_error)
    return None


def extract_oidc_user_info(claims: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize OIDC claims into a flat dict understood by get_current_user.

    Fusion control-plane expects:
      - 'sub'      → user_id (external)
      - 'email'    → user email
      - 'name'     → display name / username
      - 'groups'   → list of group names (mapped to roles by bank/tenant admins)

    Azure AD / Entra ID uses 'preferred_username'; Google uses 'email'.
    """
    return {
        "sub": claims.get("sub") or claims.get("oid", ""),
        "email": claims.get("email") or claims.get("preferred_username", ""),
        "username": claims.get("name") or claims.get("preferred_username") or claims.get("email", ""),
        "groups": claims.get("groups") or claims.get("roles") or [],
        "issuer": claims.get("iss", ""),
    }
