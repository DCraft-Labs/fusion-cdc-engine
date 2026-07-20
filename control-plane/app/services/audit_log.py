"""Audit log helper — shared by all API handlers.

Records `audit_logs` rows with the canonical action names expected by
scripts/e2e/audit_metadata.py (user.login, source.create, source.update,
destination.create, destination.update, destination.test,
connection.create, connection.update, connection.sync,
connection_run.start, connection_run.complete, checkpoint.update).
"""
from __future__ import annotations

from typing import Optional

from sqlalchemy.orm import Session

from app.models.auth import AuditLog, User


def record_audit(
    db: Session,
    action: str,
    *,
    user: Optional[User] = None,
    resource_type: Optional[str] = None,
    resource_id: Optional[str] = None,
    status: str = "success",
    details: Optional[dict] = None,
    ip_address: Optional[str] = None,
) -> None:
    """Insert an audit_logs row. Safe to call inside an existing transaction;
    commits independently so the audit record is durable even if the caller
    later rolls back."""
    log = AuditLog(
        user_id=str(user.user_id) if user is not None else None,
        username=user.username if user is not None else None,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        status=status,
        details=details or {},
        ip_address=ip_address,
    )
    db.add(log)
    db.commit()
