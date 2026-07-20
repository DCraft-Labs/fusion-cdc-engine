"""
System Models - Alert model REBUILT FROM schema_postgres.sql
Matches actual database schema exactly
"""
from sqlalchemy import Column, String, Integer, Boolean, Text, ForeignKey, text, DateTime
from sqlalchemy.dialects.postgresql import UUID, JSONB
from app.models.base import BaseModel


class Alert(BaseModel):
    """System and connection alerts - NO TimestampMixin as table has no created_at/updated_at"""
    
    __tablename__ = "alerts"
    
    # Primary Key
    alert_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Scope
    connection_id = Column(
        UUID(as_uuid=True),
        ForeignKey("connections.connection_id", ondelete="CASCADE"),
        nullable=True,
    )
    sub_tenant_id = Column(UUID(as_uuid=True), nullable=True)
    bank_id = Column(UUID(as_uuid=True), nullable=True)
    
    # Alert Information
    alert_type = Column(String(100), nullable=False)
    severity = Column(String(50), nullable=False, index=True)
    title = Column(String(500), nullable=False)
    message = Column(Text, nullable=False)
    alert_context = Column(JSONB, nullable=True)
    
    # Timing
    triggered_at = Column(DateTime(timezone=True), server_default=text("now()"), index=True)
    
    # Acknowledgment
    acknowledged = Column(Boolean, server_default=text("false"))
    acknowledged_at = Column(DateTime(timezone=True), nullable=True)
    acknowledged_by = Column(UUID(as_uuid=True), nullable=True)
    
    # Resolution
    resolved = Column(Boolean, server_default=text("false"))
    resolved_at = Column(DateTime(timezone=True), nullable=True)
    resolution_notes = Column(Text, nullable=True)
    
    # Webhook
    webhook_sent = Column(Boolean, server_default=text("false"))
    webhook_sent_at = Column(DateTime(timezone=True), nullable=True)
    webhook_response_status = Column(Integer, nullable=True)
