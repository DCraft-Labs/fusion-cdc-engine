"""
Alert Management API
REST endpoints for alert rules, notification channels, alerts, and escalation
"""
from datetime import datetime, timedelta
from typing import Optional, List
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import and_, or_, func, desc

from app.database import get_db
from app.models.auth import User
from app.models.alerting import (
    NotificationChannel, AlertRule, AlertRuleChannel, AlertEscalationPolicy,
    AlertEvaluation, AlertHistory, AlertNotificationLog, AlertSuppression
)
from app.models.system import Alert
from app.models.connection import Connection
from app.models.source_destination import Source, Destination
from app.schemas.alerting import *
from app.auth import get_current_user, require_permission


router = APIRouter(prefix="/alerts", tags=["alerts"])


# ============================================================================
# Helper Functions
# ============================================================================

def _get_channel_by_id(db: Session, channel_id: UUID, user: User) -> NotificationChannel:
    """Get notification channel by ID with tenant filtering"""
    channel = db.query(NotificationChannel).filter(
        NotificationChannel.channel_id == channel_id,
        NotificationChannel.sub_tenant_id == user.sub_tenant_id,
        NotificationChannel.is_deleted == False,
    ).first()
    
    if not channel:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Notification channel not found"
        )
    
    return channel


def _get_rule_by_id(db: Session, rule_id: UUID, user: User) -> AlertRule:
    """Get alert rule by ID with tenant filtering"""
    rule = db.query(AlertRule).filter(
        AlertRule.rule_id == rule_id,
        AlertRule.sub_tenant_id == user.sub_tenant_id,
        AlertRule.is_deleted == False,
    ).first()
    
    if not rule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Alert rule not found"
        )
    
    return rule


def _get_alert_by_id(db: Session, alert_id: UUID, user: User) -> Alert:
    """Get alert by ID with tenant filtering"""
    alert = db.query(Alert).filter(
        Alert.alert_id == alert_id,
        Alert.sub_tenant_id == user.sub_tenant_id,
    ).first()
    
    if not alert:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Alert not found"
        )
    
    return alert


def _validate_channel_config(channel_type: str, config: dict) -> tuple[bool, str]:
    """Validate notification channel configuration"""
    # Basic validation - in production, implement comprehensive validation
    if channel_type == "email":
        if "recipients" not in config or not config["recipients"]:
            return False, "Email channel requires recipients"
    elif channel_type == "slack":
        if "webhook_url" not in config:
            return False, "Slack channel requires webhook_url"
    elif channel_type == "webhook":
        if "url" not in config or "method" not in config:
            return False, "Webhook channel requires url and method"
    elif channel_type == "pagerduty":
        if "integration_key" not in config:
            return False, "PagerDuty channel requires integration_key"
    elif channel_type == "msteams":
        if "webhook_url" not in config:
            return False, "MS Teams channel requires webhook_url"
    
    return True, ""


# ============================================================================
# Notification Channel Endpoints
# ============================================================================

@router.post(
    "/channels",
    response_model=NotificationChannelResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_permission("alerts:create"))]
)
async def create_notification_channel(
    channel_data: NotificationChannelCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Create a new notification channel"""
    
    # Validate configuration
    is_valid, error_msg = _validate_channel_config(channel_data.channel_type, channel_data.config)
    if not is_valid:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_msg
        )
    
    # Check duplicate name
    existing = db.query(NotificationChannel).filter(
        NotificationChannel.sub_tenant_id == current_user.sub_tenant_id,
        NotificationChannel.channel_name == channel_data.channel_name,
        NotificationChannel.is_deleted == False,
    ).first()
    
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Notification channel with name '{channel_data.channel_name}' already exists"
        )
    
    # Create channel
    channel = NotificationChannel(
        bank_id=current_user.bank_id,
        sub_tenant_id=current_user.sub_tenant_id,
        channel_name=channel_data.channel_name,
        channel_type=channel_data.channel_type,
        description=channel_data.description,
        config=channel_data.config,
        auth_config=channel_data.auth_config,
        is_active=channel_data.is_active,
        rate_limit_per_hour=channel_data.rate_limit_per_hour,
        rate_limit_per_day=channel_data.rate_limit_per_day,
        tags=channel_data.tags,
        created_by=current_user.user_id,
    )
    
    db.add(channel)
    db.commit()
    db.refresh(channel)
    
    return channel


@router.get(
    "/channels",
    response_model=NotificationChannelListResponse,
)
async def list_notification_channels(
    channel_type: Optional[str] = Query(None),
    is_active: Optional[bool] = Query(None),
    is_verified: Optional[bool] = Query(None),
    search: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """List notification channels with filters"""
    
    # Base query with tenant filtering
    query = db.query(NotificationChannel).filter(
        NotificationChannel.sub_tenant_id == current_user.sub_tenant_id,
        NotificationChannel.is_deleted == False,
    )
    
    # Apply filters
    if channel_type:
        query = query.filter(NotificationChannel.channel_type == channel_type)
    
    if is_active is not None:
        query = query.filter(NotificationChannel.is_active == is_active)
    
    if is_verified is not None:
        query = query.filter(NotificationChannel.is_verified == is_verified)
    
    if search:
        search_filter = or_(
            NotificationChannel.channel_name.ilike(f"%{search}%"),
            NotificationChannel.description.ilike(f"%{search}%"),
        )
        query = query.filter(search_filter)
    
    # Get total count
    total = query.count()
    
    # Apply pagination and ordering
    channels = query.order_by(NotificationChannel.created_at.desc()).offset((page - 1) * page_size).limit(page_size).all()
    
    return NotificationChannelListResponse(
        channels=channels,
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get(
    "/channels/{channel_id}",
    response_model=NotificationChannelResponse,
)
async def get_notification_channel(
    channel_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get notification channel details"""
    
    channel = _get_channel_by_id(db, channel_id, current_user)
    return channel


@router.patch(
    "/channels/{channel_id}",
    response_model=NotificationChannelResponse,
    dependencies=[Depends(require_permission("alerts:update"))]
)
async def update_notification_channel(
    channel_id: UUID,
    channel_data: NotificationChannelUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Update notification channel"""
    
    channel = _get_channel_by_id(db, channel_id, current_user)
    
    # Check duplicate name if changing
    if channel_data.channel_name and channel_data.channel_name != channel.channel_name:
        existing = db.query(NotificationChannel).filter(
            NotificationChannel.sub_tenant_id == current_user.sub_tenant_id,
            NotificationChannel.channel_name == channel_data.channel_name,
            NotificationChannel.channel_id != channel_id,
            NotificationChannel.is_deleted == False,
        ).first()
        
        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Notification channel with name '{channel_data.channel_name}' already exists"
            )
    
    # Validate configuration if changed
    if channel_data.config:
        is_valid, error_msg = _validate_channel_config(channel.channel_type, channel_data.config)
        if not is_valid:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=error_msg
            )
    
    # Update fields
    update_data = channel_data.dict(exclude_unset=True)
    for field, value in update_data.items():
        setattr(channel, field, value)
    
    channel.updated_by = current_user.user_id
    
    db.commit()
    db.refresh(channel)
    
    return channel


@router.delete(
    "/channels/{channel_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(require_permission("alerts:delete"))]
)
async def delete_notification_channel(
    channel_id: UUID,
    force: bool = Query(False, description="Force delete even if used in alert rules"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Delete notification channel (soft delete)"""
    
    channel = _get_channel_by_id(db, channel_id, current_user)
    
    # Check if channel is used in alert rules
    if not force:
        rule_count = db.query(AlertRuleChannel).filter(
            AlertRuleChannel.channel_id == channel_id,
        ).count()
        
        if rule_count > 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Channel is used in {rule_count} alert rule(s). Use force=true to delete anyway."
            )
    
    # Soft delete
    channel.is_deleted = True
    channel.is_active = False
    channel.updated_by = current_user.user_id
    
    db.commit()


@router.post(
    "/channels/{channel_id}/test",
    response_model=NotificationChannelTestResponse,
    dependencies=[Depends(require_permission("alerts:create"))]
)
async def test_notification_channel(
    channel_id: UUID,
    test_request: NotificationChannelTestRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Test notification channel"""
    
    channel = _get_channel_by_id(db, channel_id, current_user)
    
    # TODO: Implement actual notification sending
    # For now, return mock success
    import time
    start_time = time.time()
    
    # Simulate sending
    success = True
    delivery_time_ms = int((time.time() - start_time) * 1000)
    
    # Update channel test status
    channel.last_test_at = datetime.utcnow()
    channel.last_test_status = "success" if success else "failure"
    
    if success:
        channel.is_verified = True
        channel.verified_at = datetime.utcnow()
        channel.last_test_error = None
    else:
        channel.last_test_error = "Test failed"
    
    db.commit()
    
    return NotificationChannelTestResponse(
        success=success,
        status="success" if success else "failure",
        message=f"Test notification sent successfully to {channel.channel_type} channel",
        delivery_time_ms=delivery_time_ms,
        tested_at=datetime.utcnow(),
    )


# ============================================================================
# Alert Rule Endpoints
# ============================================================================

@router.post(
    "/rules",
    response_model=AlertRuleResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_permission("alerts:create"))]
)
async def create_alert_rule(
    rule_data: AlertRuleCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Create a new alert rule"""
    
    # Validate scope resources exist
    if rule_data.connection_id:
        connection = db.query(Connection).filter(
            Connection.connection_id == rule_data.connection_id,
            Connection.sub_tenant_id == current_user.sub_tenant_id,
        ).first()
        if not connection:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Connection not found"
            )
    
    if rule_data.source_id:
        source = db.query(Source).filter(
            Source.source_id == rule_data.source_id,
            Source.sub_tenant_id == current_user.sub_tenant_id,
        ).first()
        if not source:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Source not found"
            )
    
    if rule_data.destination_id:
        destination = db.query(Destination).filter(
            Destination.destination_id == rule_data.destination_id,
            Destination.sub_tenant_id == current_user.sub_tenant_id,
        ).first()
        if not destination:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Destination not found"
            )
    
    # Validate notification channels exist
    for channel_id in rule_data.notification_channel_ids:
        channel = db.query(NotificationChannel).filter(
            NotificationChannel.channel_id == channel_id,
            NotificationChannel.sub_tenant_id == current_user.sub_tenant_id,
            NotificationChannel.is_deleted == False,
        ).first()
        if not channel:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Notification channel {channel_id} not found"
            )
    
    # Check duplicate name
    existing = db.query(AlertRule).filter(
        AlertRule.sub_tenant_id == current_user.sub_tenant_id,
        AlertRule.rule_name == rule_data.rule_name,
        AlertRule.is_deleted == False,
    ).first()
    
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Alert rule with name '{rule_data.rule_name}' already exists"
        )
    
    # Determine scope_id based on scope_type
    scope_id = None
    if rule_data.scope_type == "connection":
        scope_id = rule_data.connection_id
    elif rule_data.scope_type == "source":
        scope_id = rule_data.source_id
    elif rule_data.scope_type == "destination":
        scope_id = rule_data.destination_id
    elif rule_data.scope_type == "stream":
        scope_id = rule_data.stream_id
    
    # Create rule
    rule = AlertRule(
        bank_id=current_user.bank_id,
        sub_tenant_id=current_user.sub_tenant_id,
        rule_name=rule_data.rule_name,
        description=rule_data.description,
        alert_type=rule_data.alert_type,
        severity=rule_data.severity,
        scope_type=rule_data.scope_type,
        scope_id=scope_id,
        connection_id=rule_data.connection_id,
        source_id=rule_data.source_id,
        destination_id=rule_data.destination_id,
        condition_type=rule_data.condition_type,
        condition_definition=rule_data.condition_definition,
        consecutive_failures_required=rule_data.consecutive_failures,
        evaluation_window_minutes=rule_data.evaluation_window_minutes,
        auto_resolve=rule_data.auto_resolve,
        auto_resolve_after_minutes=rule_data.auto_resolve_after_minutes,
        is_active=rule_data.is_active,
        created_by=current_user.user_id,
    )
    
    db.add(rule)
    db.flush()  # Get rule_id
    
    # Create channel associations
    for channel_id in rule_data.notification_channel_ids:
        rule_channel = AlertRuleChannel(
            rule_id=rule.rule_id,
            channel_id=channel_id,
        )
        db.add(rule_channel)
    
    db.commit()
    db.refresh(rule)
    
    # Build response with aggregated data
    response = AlertRuleResponse(
        **rule.__dict__,
        notification_channel_ids=rule_data.notification_channel_ids,
        active_alerts_count=0,
        total_evaluations=0,
        total_triggers=0,
    )
    
    return response


@router.get(
    "/rules",
    response_model=AlertRuleListResponse,
)
async def list_alert_rules(
    filters: AlertRuleSearchFilters = Depends(),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """List alert rules with filters"""
    
    # Base query with tenant filtering
    query = db.query(AlertRule).filter(
        AlertRule.sub_tenant_id == current_user.sub_tenant_id,
        AlertRule.is_deleted == False,
    )
    
    # Apply filters
    if filters.alert_type:
        query = query.filter(AlertRule.alert_type == filters.alert_type)
    
    if filters.severity:
        query = query.filter(AlertRule.severity == filters.severity)
    
    if filters.scope_type:
        query = query.filter(AlertRule.scope_type == filters.scope_type)
    
    if filters.connection_id:
        query = query.filter(AlertRule.connection_id == filters.connection_id)
    
    if filters.is_active is not None:
        query = query.filter(AlertRule.is_active == filters.is_active)
    
    if filters.search:
        search_filter = or_(
            AlertRule.rule_name.ilike(f"%{filters.search}%"),
            AlertRule.description.ilike(f"%{filters.search}%"),
        )
        query = query.filter(search_filter)
    
    # Get total count
    total = query.count()
    
    # Apply pagination and ordering
    rules = query.order_by(AlertRule.created_at.desc()).offset((filters.page - 1) * filters.page_size).limit(filters.page_size).all()
    
    # Build responses with aggregated data
    rule_responses = []
    for rule in rules:
        # Get channel IDs
        channel_ids = [rc.channel_id for rc in db.query(AlertRuleChannel).filter(
            AlertRuleChannel.rule_id == rule.rule_id
        ).all()]
        
        # Count active alerts
        active_alerts = db.query(Alert).filter(
            Alert.alert_type == rule.alert_type,
            Alert.connection_id == rule.connection_id,
            Alert.resolved == False,
            Alert.acknowledged == False,
        ).count()
        
        # Count evaluations and triggers
        total_evaluations = db.query(AlertEvaluation).filter(
            AlertEvaluation.rule_id == rule.rule_id
        ).count()
        
        total_triggers = db.query(AlertEvaluation).filter(
            AlertEvaluation.rule_id == rule.rule_id,
            AlertEvaluation.passed == False
        ).count()
        
        rule_responses.append(AlertRuleResponse(
            **rule.__dict__,
            notification_channel_ids=channel_ids,
            active_alerts_count=active_alerts,
            total_evaluations=total_evaluations,
            total_triggers=total_triggers,
        ))
    
    return AlertRuleListResponse(
        rules=rule_responses,
        total=total,
        page=filters.page,
        page_size=filters.page_size,
    )


@router.get(
    "/rules/{rule_id}",
    response_model=AlertRuleResponse,
)
async def get_alert_rule(
    rule_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get alert rule details"""
    
    rule = _get_rule_by_id(db, rule_id, current_user)
    
    # Get channel IDs
    channel_ids = [rc.channel_id for rc in db.query(AlertRuleChannel).filter(
        AlertRuleChannel.rule_id == rule_id
    ).all()]
    
    # Count active alerts
    active_alerts = db.query(Alert).filter(
        Alert.alert_type == rule.alert_type,
        Alert.connection_id == rule.connection_id,
        Alert.resolved == False,
        Alert.acknowledged == False,
    ).count()
    
    # Count evaluations and triggers
    total_evaluations = db.query(AlertEvaluation).filter(
        AlertEvaluation.rule_id == rule_id
    ).count()
    
    total_triggers = db.query(AlertEvaluation).filter(
        AlertEvaluation.rule_id == rule_id,
        AlertEvaluation.passed == False
    ).count()
    
    return AlertRuleResponse(
        **rule.__dict__,
        notification_channel_ids=channel_ids,
        active_alerts_count=active_alerts,
        total_evaluations=total_evaluations,
        total_triggers=total_triggers,
    )


@router.patch(
    "/rules/{rule_id}",
    response_model=AlertRuleResponse,
    dependencies=[Depends(require_permission("alerts:update"))]
)
async def update_alert_rule(
    rule_id: UUID,
    rule_data: AlertRuleUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Update alert rule"""
    
    rule = _get_rule_by_id(db, rule_id, current_user)
    
    # Check duplicate name if changing
    if rule_data.rule_name and rule_data.rule_name != rule.rule_name:
        existing = db.query(AlertRule).filter(
            AlertRule.sub_tenant_id == current_user.sub_tenant_id,
            AlertRule.rule_name == rule_data.rule_name,
            AlertRule.rule_id != rule_id,
            AlertRule.is_deleted == False,
        ).first()
        
        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Alert rule with name '{rule_data.rule_name}' already exists"
            )
    
    # Update notification channels if provided
    if rule_data.notification_channel_ids is not None:
        # Validate channels exist
        for channel_id in rule_data.notification_channel_ids:
            channel = db.query(NotificationChannel).filter(
                NotificationChannel.channel_id == channel_id,
                NotificationChannel.sub_tenant_id == current_user.sub_tenant_id,
                NotificationChannel.is_deleted == False,
            ).first()
            if not channel:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Notification channel {channel_id} not found"
                )
        
        # Remove existing associations
        db.query(AlertRuleChannel).filter(
            AlertRuleChannel.rule_id == rule_id
        ).delete()
        
        # Create new associations
        for channel_id in rule_data.notification_channel_ids:
            rule_channel = AlertRuleChannel(
                rule_id=rule_id,
                channel_id=channel_id,
            )
            db.add(rule_channel)
    
    # Update fields
    update_data = rule_data.dict(exclude_unset=True, exclude={"notification_channel_ids"})
    for field, value in update_data.items():
        setattr(rule, field, value)
    
    rule.updated_by = current_user.user_id
    
    db.commit()
    db.refresh(rule)
    
    # Get updated channel IDs
    channel_ids = [rc.channel_id for rc in db.query(AlertRuleChannel).filter(
        AlertRuleChannel.rule_id == rule_id
    ).all()]
    
    # Count active alerts
    active_alerts = db.query(Alert).filter(
        Alert.alert_type == rule.alert_type,
        Alert.connection_id == rule.connection_id,
        Alert.resolved == False,
        Alert.acknowledged == False,
    ).count()
    
    # Count evaluations and triggers
    total_evaluations = db.query(AlertEvaluation).filter(
        AlertEvaluation.rule_id == rule_id
    ).count()
    
    total_triggers = db.query(AlertEvaluation).filter(
        AlertEvaluation.rule_id == rule_id,
        AlertEvaluation.passed == False
    ).count()
    
    return AlertRuleResponse(
        **rule.__dict__,
        notification_channel_ids=channel_ids,
        active_alerts_count=active_alerts,
        total_evaluations=total_evaluations,
        total_triggers=total_triggers,
    )


@router.delete(
    "/rules/{rule_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(require_permission("alerts:delete"))]
)
async def delete_alert_rule(
    rule_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Delete alert rule (soft delete)"""
    
    rule = _get_rule_by_id(db, rule_id, current_user)
    
    # Soft delete
    rule.is_deleted = True
    rule.is_active = False
    rule.updated_by = current_user.user_id
    
    db.commit()


@router.post(
    "/rules/test",
    response_model=AlertRuleTestResponse,
    dependencies=[Depends(require_permission("alerts:create"))]
)
async def test_alert_rule(
    test_request: AlertRuleTestRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Test alert rule evaluation"""
    
    # Validate connection if provided
    if test_request.connection_id:
        connection = db.query(Connection).filter(
            Connection.connection_id == test_request.connection_id,
            Connection.sub_tenant_id == current_user.sub_tenant_id,
        ).first()
        if not connection:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Connection not found"
            )
    
    # TODO: Implement actual rule evaluation
    # For now, return mock evaluation
    import time
    start_time = time.time()
    
    condition_met = True
    evaluated_value = 95.5
    threshold_value = 90.0
    
    evaluation_time_ms = int((time.time() - start_time) * 1000)
    
    return AlertRuleTestResponse(
        test_passed=True,
        condition_met=condition_met,
        evaluated_value=evaluated_value,
        threshold_value=threshold_value,
        evaluation_time_ms=evaluation_time_ms,
        test_data={
            "metric": "lag_seconds",
            "current_value": evaluated_value,
            "threshold": threshold_value,
            "operator": "gt",
        },
        message=f"Condition {'met' if condition_met else 'not met'}: {evaluated_value} > {threshold_value}",
        tested_at=datetime.utcnow(),
    )


@router.get(
    "/rules/{rule_id}/evaluations",
    response_model=AlertEvaluationListResponse,
)
async def list_rule_evaluations(
    rule_id: UUID,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """List evaluation history for an alert rule"""
    
    rule = _get_rule_by_id(db, rule_id, current_user)
    
    # Base query
    query = db.query(AlertEvaluation).filter(
        AlertEvaluation.rule_id == rule_id
    )
    
    # Get total count
    total = query.count()
    
    # Apply pagination and ordering
    evaluations = query.order_by(AlertEvaluation.evaluated_at.desc()).offset((page - 1) * page_size).limit(page_size).all()
    
    return AlertEvaluationListResponse(
        evaluations=evaluations,
        total=total,
        page=page,
        page_size=page_size,
    )


# ============================================================================
# Alert Management Endpoints
# ============================================================================

@router.get(
    "",
    response_model=AlertListResponse,
)
async def list_alerts(
    filters: AlertSearchFilters = Depends(),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """List alerts with filters"""
    
    # Base query with tenant filtering
    query = db.query(Alert).filter(
        Alert.sub_tenant_id == current_user.sub_tenant_id,
    )
    
    # Apply filters
    if filters.alert_type:
        query = query.filter(Alert.alert_type == filters.alert_type)
    
    if filters.severity:
        query = query.filter(Alert.severity == filters.severity)
    
    if filters.status:
        if filters.status == "active":
            query = query.filter(Alert.resolved == False, Alert.acknowledged == False)
        elif filters.status == "acknowledged":
            query = query.filter(Alert.acknowledged == True, Alert.resolved == False)
        elif filters.status == "resolved":
            query = query.filter(Alert.resolved == True)
    
    if filters.connection_id:
        query = query.filter(Alert.connection_id == filters.connection_id)
    
    if filters.source_id:
        query = query.filter(Alert.source_id == filters.source_id)
    
    if filters.destination_id:
        query = query.filter(Alert.destination_id == filters.destination_id)
    
    if filters.search:
        search_filter = or_(
            Alert.title.ilike(f"%{filters.search}%"),
            Alert.message.ilike(f"%{filters.search}%"),
        )
        query = query.filter(search_filter)
    
    # Get total count
    total = query.count()
    
    # Apply pagination and ordering
    alerts = query.order_by(Alert.triggered_at.desc()).offset((filters.page - 1) * filters.page_size).limit(filters.page_size).all()
    
    return AlertListResponse(
        alerts=alerts,
        total=total,
        page=filters.page,
        page_size=filters.page_size,
    )


@router.get(
    "/{alert_id}",
    response_model=AlertResponse,
)
async def get_alert(
    alert_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get alert details"""
    
    alert = _get_alert_by_id(db, alert_id, current_user)
    return alert


@router.post(
    "/{alert_id}/acknowledge",
    response_model=AlertResponse,
    dependencies=[Depends(require_permission("alerts:update"))]
)
async def acknowledge_alert(
    alert_id: UUID,
    ack_request: AlertAcknowledgeRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Acknowledge an alert"""
    
    alert = _get_alert_by_id(db, alert_id, current_user)
    
    if alert.status != "active":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only active alerts can be acknowledged"
        )
    
    # Create history record
    history = AlertHistory(
        alert_id=alert_id,
        changed_by=current_user.user_id,
        old_status=alert.status,
        new_status="acknowledged",
        change_reason=ack_request.notes,
    )
    db.add(history)
    
    # Update alert
    alert.acknowledged = True
    alert.acknowledged_at = datetime.utcnow()
    
    db.commit()
    db.refresh(alert)
    
    return alert


@router.post(
    "/{alert_id}/resolve",
    response_model=AlertResponse,
    dependencies=[Depends(require_permission("alerts:update"))]
)
async def resolve_alert(
    alert_id: UUID,
    resolve_request: AlertResolveRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Resolve an alert"""
    
    alert = _get_alert_by_id(db, alert_id, current_user)
    
    if alert.status == "resolved":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Alert is already resolved"
        )
    
    # Create history record
    history = AlertHistory(
        alert_id=alert_id,
        changed_by=current_user.user_id,
        old_status=alert.status,
        new_status="resolved",
        change_reason=resolve_request.notes,
    )
    db.add(history)
    
    # Update alert
    alert.resolved = True
    alert.resolved_at = datetime.utcnow()
    alert.resolution_notes = resolve_request.notes
    
    db.commit()
    db.refresh(alert)
    
    return alert


@router.get(
    "/{alert_id}/history",
    response_model=AlertHistoryListResponse,
)
async def get_alert_history(
    alert_id: UUID,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get alert history"""
    
    alert = _get_alert_by_id(db, alert_id, current_user)
    
    # Base query
    query = db.query(AlertHistory).filter(
        AlertHistory.alert_id == alert_id
    )
    
    # Get total count
    total = query.count()
    
    # Apply pagination and ordering
    history = query.order_by(AlertHistory.changed_at.desc()).offset((page - 1) * page_size).limit(page_size).all()
    
    return AlertHistoryListResponse(
        history=history,
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get(
    "/{alert_id}/notifications",
    response_model=AlertNotificationLogListResponse,
)
async def get_alert_notifications(
    alert_id: UUID,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get alert notification logs"""
    
    alert = _get_alert_by_id(db, alert_id, current_user)
    
    # Base query
    query = db.query(AlertNotificationLog).filter(
        AlertNotificationLog.alert_id == alert_id
    )
    
    # Get total count
    total = query.count()
    
    # Apply pagination and ordering
    logs = query.order_by(AlertNotificationLog.sent_at.desc()).offset((page - 1) * page_size).limit(page_size).all()
    
    return AlertNotificationLogListResponse(
        logs=logs,
        total=total,
        page=page,
        page_size=page_size,
    )


# ============================================================================
# Alert Suppression Endpoints
# ============================================================================

@router.post(
    "/suppressions",
    response_model=AlertSuppressionResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_permission("alerts:create"))]
)
async def create_alert_suppression(
    suppression_data: AlertSuppressionCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Create alert suppression"""
    
    # Validate scope resources
    if suppression_data.rule_ids:
        for rule_id in suppression_data.rule_ids:
            rule = db.query(AlertRule).filter(
                AlertRule.rule_id == rule_id,
                AlertRule.sub_tenant_id == current_user.sub_tenant_id,
            ).first()
            if not rule:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Alert rule {rule_id} not found"
                )
    
    if suppression_data.connection_ids:
        for connection_id in suppression_data.connection_ids:
            connection = db.query(Connection).filter(
                Connection.connection_id == connection_id,
                Connection.sub_tenant_id == current_user.sub_tenant_id,
        ).first()
        if not connection:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Connection not found"
            )
    
    # Create suppression
    suppression = AlertSuppression(
        bank_id=current_user.bank_id,
        sub_tenant_id=current_user.sub_tenant_id,
        suppression_name=suppression_data.suppression_name,
        description=suppression_data.description,
        scope_type=suppression_data.scope_type,
        rule_ids=suppression_data.rule_ids,
        connection_ids=suppression_data.connection_ids,
        start_time=suppression_data.start_time,
        end_time=suppression_data.end_time,
        is_recurring=suppression_data.is_recurring,
        recurrence_pattern=suppression_data.recurrence_pattern,
        created_by=current_user.user_id,
    )
    
    db.add(suppression)
    db.commit()
    db.refresh(suppression)
    
    return suppression


@router.get(
    "/suppressions",
    response_model=AlertSuppressionListResponse,
)
async def list_alert_suppressions(
    is_active: Optional[bool] = Query(None),
    scope_type: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """List alert suppressions"""
    
    # Base query with tenant filtering
    query = db.query(AlertSuppression).filter(
        AlertSuppression.sub_tenant_id == current_user.sub_tenant_id,
    )
    
    # Apply filters
    if is_active is not None:
        current_time = datetime.utcnow()
        if is_active:
            query = query.filter(
                AlertSuppression.is_active == True,
                AlertSuppression.start_time <= current_time,
                AlertSuppression.end_time >= current_time,
            )
        else:
            query = query.filter(
                or_(
                    AlertSuppression.is_active == False,
                    AlertSuppression.end_time < current_time,
                )
            )
    
    if scope_type:
        query = query.filter(AlertSuppression.scope_type == scope_type)
    
    # Get total count
    total = query.count()
    
    # Apply pagination and ordering
    suppressions = query.order_by(AlertSuppression.created_at.desc()).offset((page - 1) * page_size).limit(page_size).all()
    
    return AlertSuppressionListResponse(
        suppressions=suppressions,
        total=total,
        page=page,
        page_size=page_size,
    )


@router.patch(
    "/suppressions/{suppression_id}",
    response_model=AlertSuppressionResponse,
    dependencies=[Depends(require_permission("alerts:update"))]
)
async def update_alert_suppression(
    suppression_id: UUID,
    suppression_data: AlertSuppressionUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Update alert suppression"""
    
    suppression = db.query(AlertSuppression).filter(
        AlertSuppression.suppression_id == suppression_id,
        AlertSuppression.sub_tenant_id == current_user.sub_tenant_id,
    ).first()
    
    if not suppression:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Alert suppression not found"
        )
    
    # Update fields
    update_data = suppression_data.dict(exclude_unset=True)
    for field, value in update_data.items():
        setattr(suppression, field, value)
    
    db.commit()
    db.refresh(suppression)
    
    return suppression


@router.delete(
    "/suppressions/{suppression_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(require_permission("alerts:delete"))]
)
async def delete_alert_suppression(
    suppression_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Delete alert suppression"""
    
    suppression = db.query(AlertSuppression).filter(
        AlertSuppression.suppression_id == suppression_id,
        AlertSuppression.sub_tenant_id == current_user.sub_tenant_id,
    ).first()
    
    if not suppression:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Alert suppression not found"
        )
    
    db.delete(suppression)
    db.commit()


# ============================================================================
# Statistics and Dashboard Endpoints
# ============================================================================

@router.get(
    "/statistics",
    response_model=AlertStatistics,
)
async def get_alert_statistics(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get alert statistics"""
    
    # Base query with tenant filtering
    base_query = db.query(Alert).filter(
        Alert.sub_tenant_id == current_user.sub_tenant_id,
    )
    
    # Count by status
    total_alerts = base_query.count()
    active_alerts = base_query.filter(Alert.resolved == False, Alert.acknowledged == False).count()
    acknowledged_alerts = base_query.filter(Alert.acknowledged == True, Alert.resolved == False).count()
    resolved_alerts = base_query.filter(Alert.resolved == True).count()
    
    # Count by severity
    critical_alerts = base_query.filter(Alert.severity == "critical").count()
    error_alerts = base_query.filter(Alert.severity == "error").count()
    warning_alerts = base_query.filter(Alert.severity == "warning").count()
    info_alerts = base_query.filter(Alert.severity == "info").count()
    
    # Count by type
    alerts_by_type_query = db.query(
        Alert.alert_type,
        func.count(Alert.alert_id).label("count")
    ).filter(
        Alert.sub_tenant_id == current_user.sub_tenant_id,
    ).group_by(Alert.alert_type).all()
    
    alerts_by_type = {row.alert_type: row.count for row in alerts_by_type_query}
    
    # Time-based counts
    now = datetime.utcnow()
    alerts_last_24h = base_query.filter(
        Alert.triggered_at >= now - timedelta(hours=24)
    ).count()
    
    alerts_last_7d = base_query.filter(
        Alert.triggered_at >= now - timedelta(days=7)
    ).count()
    
    # Average resolution time
    resolved_with_time = db.query(Alert).filter(
        Alert.sub_tenant_id == current_user.sub_tenant_id,
        Alert.resolved == True,
        Alert.resolved_at.isnot(None),
    ).all()
    
    if resolved_with_time:
        total_resolution_time = sum([
            (alert.resolved_at - alert.triggered_at).total_seconds() / 60
            for alert in resolved_with_time
        ])
        avg_resolution_time = total_resolution_time / len(resolved_with_time)
    else:
        avg_resolution_time = None
    
    return AlertStatistics(
        total_alerts=total_alerts,
        active_alerts=active_alerts,
        acknowledged_alerts=acknowledged_alerts,
        resolved_alerts=resolved_alerts,
        critical_alerts=critical_alerts,
        error_alerts=error_alerts,
        warning_alerts=warning_alerts,
        info_alerts=info_alerts,
        alerts_by_type=alerts_by_type,
        alerts_last_24h=alerts_last_24h,
        alerts_last_7d=alerts_last_7d,
        avg_resolution_time_minutes=avg_resolution_time,
    )


@router.get(
    "/dashboard",
    response_model=AlertDashboard,
)
async def get_alert_dashboard(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get alert dashboard data"""
    
    # Get statistics
    statistics_response = await get_alert_statistics(db, current_user)
    
    # Get top alert types
    top_types_query = db.query(
        Alert.alert_type,
        func.count(Alert.alert_id).label("count")
    ).filter(
        Alert.sub_tenant_id == current_user.sub_tenant_id,
        Alert.resolved == False,
        Alert.acknowledged == False,
    ).group_by(Alert.alert_type).order_by(desc("count")).limit(10).all()
    
    top_alert_types = [
        {"alert_type": row.alert_type, "count": row.count}
        for row in top_types_query
    ]
    
    # Get recent critical alerts
    recent_critical = db.query(Alert).filter(
        Alert.sub_tenant_id == current_user.sub_tenant_id,
        Alert.severity == "critical",
        Alert.resolved == False,
    ).order_by(Alert.triggered_at.desc()).limit(10).all()
    
    # Count unacknowledged critical
    unacknowledged_critical = db.query(Alert).filter(
        Alert.sub_tenant_id == current_user.sub_tenant_id,
        Alert.severity == "critical",
        Alert.resolved == False,
        Alert.acknowledged == False,
    ).count()
    
    # Determine trend
    now = datetime.utcnow()
    alerts_today = db.query(Alert).filter(
        Alert.sub_tenant_id == current_user.sub_tenant_id,
        Alert.triggered_at >= now - timedelta(days=1),
    ).count()
    
    alerts_yesterday = db.query(Alert).filter(
        Alert.sub_tenant_id == current_user.sub_tenant_id,
        Alert.triggered_at >= now - timedelta(days=2),
        Alert.triggered_at < now - timedelta(days=1),
    ).count()
    
    if alerts_today < alerts_yesterday * 0.8:
        trend = "improving"
    elif alerts_today > alerts_yesterday * 1.2:
        trend = "worsening"
    else:
        trend = "stable"
    
    return AlertDashboard(
        statistics=statistics_response,
        top_alert_types=top_alert_types,
        recent_critical_alerts=recent_critical,
        unacknowledged_critical=unacknowledged_critical,
        trend=trend,
    )
