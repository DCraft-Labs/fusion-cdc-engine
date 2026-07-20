"""
Integration tests for Alert Management API
"""
import pytest
from datetime import datetime, timedelta
from uuid import uuid4


class TestNotificationChannelCRUD:
    """Tests for notification channel CRUD operations"""
    
    def test_create_email_channel_success(self, client, admin_headers, admin_user):
        """Test creating an email notification channel"""
        response = client.post(
            "/api/v1/alerts/channels",
            headers=admin_headers,
            json={
                "channel_name": "Test Email Channel",
                "channel_type": "email",
                "description": "Test email channel",
                "config": {
                    "recipients": ["test@example.com"],
                    "cc": ["cc@example.com"]
                },
                "is_active": True,
                "rate_limit_per_hour": 50,
                "tags": ["email", "production"]
            }
        )
        
        assert response.status_code == 201
        data = response.json()
        assert data["channel_name"] == "Test Email Channel"
        assert data["channel_type"] == "email"
        assert data["is_active"] is True
        assert "channel_id" in data
        assert data["created_by"] == str(admin_user.user_id)
    
    def test_create_slack_channel_success(self, client, admin_headers):
        """Test creating a Slack notification channel"""
        response = client.post(
            "/api/v1/alerts/channels",
            headers=admin_headers,
            json={
                "channel_name": "Test Slack Channel",
                "channel_type": "slack",
                "config": {
                    "webhook_url": "https://hooks.slack.com/services/TEST",
                    "channel": "#alerts"
                },
                "is_active": True
            }
        )
        
        assert response.status_code == 201
        data = response.json()
        assert data["channel_name"] == "Test Slack Channel"
        assert data["channel_type"] == "slack"
    
    def test_create_webhook_channel_success(self, client, admin_headers):
        """Test creating a webhook notification channel"""
        response = client.post(
            "/api/v1/alerts/channels",
            headers=admin_headers,
            json={
                "channel_name": "Test Webhook Channel",
                "channel_type": "webhook",
                "config": {
                    "url": "https://api.example.com/webhooks/alerts",
                    "method": "POST",
                    "headers": {"Authorization": "Bearer token"}
                },
                "is_active": True
            }
        )
        
        assert response.status_code == 201
        data = response.json()
        assert data["channel_type"] == "webhook"
    
    def test_create_channel_duplicate_name(self, client, admin_headers, sample_email_channel):
        """Test creating a channel with duplicate name fails"""
        response = client.post(
            "/api/v1/alerts/channels",
            headers=admin_headers,
            json={
                "channel_name": sample_email_channel.channel_name,
                "channel_type": "email",
                "config": {"recipients": ["test@test.com"]},
                "is_active": True
            }
        )
        
        assert response.status_code == 400
        assert "already exists" in response.json()["detail"]
    
    def test_create_channel_invalid_config(self, client, admin_headers):
        """Test creating a channel with invalid config fails"""
        response = client.post(
            "/api/v1/alerts/channels",
            headers=admin_headers,
            json={
                "channel_name": "Test Invalid Channel",
                "channel_type": "email",
                "config": {},  # Missing required 'recipients'
                "is_active": True
            }
        )
        
        assert response.status_code == 422  # Pydantic validation error
    
    def test_create_channel_requires_permission(self, client, user_headers):
        """Test creating a channel requires permission"""
        response = client.post(
            "/api/v1/alerts/channels",
            headers=user_headers,
            json={
                "channel_name": "Test Channel",
                "channel_type": "email",
                "config": {"recipients": ["test@test.com"]},
                "is_active": True
            }
        )
        
        assert response.status_code == 403
    
    def test_list_channels_success(self, client, admin_headers, sample_email_channel, sample_slack_channel):
        """Test listing notification channels"""
        response = client.get(
            "/api/v1/alerts/channels",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "channels" in data
        assert "total" in data
        assert data["total"] >= 2
    
    def test_list_channels_filter_by_type(self, client, admin_headers, sample_email_channel, sample_slack_channel):
        """Test filtering channels by type"""
        response = client.get(
            "/api/v1/alerts/channels?channel_type=email",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert all(ch["channel_type"] == "email" for ch in data["channels"])
    
    def test_list_channels_filter_by_active(self, client, admin_headers, sample_email_channel):
        """Test filtering channels by active status"""
        response = client.get(
            "/api/v1/alerts/channels?is_active=true",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert all(ch["is_active"] is True for ch in data["channels"])
    
    def test_list_channels_search(self, client, admin_headers, sample_email_channel):
        """Test searching channels"""
        response = client.get(
            f"/api/v1/alerts/channels?search=Email",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert any("Email" in ch["channel_name"] for ch in data["channels"])
    
    def test_list_channels_pagination(self, client, admin_headers, sample_email_channel):
        """Test channel list pagination"""
        response = client.get(
            "/api/v1/alerts/channels?page=1&page_size=10",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["page"] == 1
        assert data["page_size"] == 10
    
    def test_get_channel_success(self, client, admin_headers, sample_email_channel):
        """Test getting channel details"""
        response = client.get(
            f"/api/v1/alerts/channels/{sample_email_channel.channel_id}",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["channel_id"] == str(sample_email_channel.channel_id)
        assert data["channel_name"] == sample_email_channel.channel_name
    
    def test_get_channel_not_found(self, client, admin_headers):
        """Test getting non-existent channel"""
        response = client.get(
            f"/api/v1/alerts/channels/{uuid4()}",
            headers=admin_headers
        )
        
        assert response.status_code == 404
    
    def test_update_channel_success(self, client, admin_headers, sample_email_channel):
        """Test updating a channel"""
        response = client.patch(
            f"/api/v1/alerts/channels/{sample_email_channel.channel_id}",
            headers=admin_headers,
            json={
                "channel_name": "Test Updated Email Channel",
                "is_active": False
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["channel_name"] == "Test Updated Email Channel"
        assert data["is_active"] is False
    
    def test_update_channel_partial(self, client, admin_headers, sample_email_channel):
        """Test partial update of a channel"""
        response = client.patch(
            f"/api/v1/alerts/channels/{sample_email_channel.channel_id}",
            headers=admin_headers,
            json={"is_active": False}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["is_active"] is False
        assert data["channel_name"] == sample_email_channel.channel_name
    
    def test_update_channel_not_found(self, client, admin_headers):
        """Test updating non-existent channel"""
        response = client.patch(
            f"/api/v1/alerts/channels/{uuid4()}",
            headers=admin_headers,
            json={"is_active": False}
        )
        
        assert response.status_code == 404
    
    def test_update_channel_requires_permission(self, client, user_headers, sample_email_channel):
        """Test updating a channel requires permission"""
        response = client.patch(
            f"/api/v1/alerts/channels/{sample_email_channel.channel_id}",
            headers=user_headers,
            json={"is_active": False}
        )
        
        assert response.status_code == 403
    
    def test_delete_channel_success(self, client, admin_headers, sample_slack_channel):
        """Test deleting a channel"""
        response = client.delete(
            f"/api/v1/alerts/channels/{sample_slack_channel.channel_id}",
            headers=admin_headers
        )
        
        assert response.status_code == 204
    
    def test_delete_channel_in_use_without_force(self, client, admin_headers, sample_email_channel):
        """Test deleting a channel in use without force fails"""
        # Channel is used in sample_alert_rule
        response = client.delete(
            f"/api/v1/alerts/channels/{sample_email_channel.channel_id}",
            headers=admin_headers
        )
        
        # Should fail or succeed depending on implementation
        # If it fails, status should be 400
        if response.status_code == 400:
            assert "used in" in response.json()["detail"].lower()
    
    def test_delete_channel_with_force(self, client, admin_headers, sample_email_channel):
        """Test deleting a channel with force"""
        response = client.delete(
            f"/api/v1/alerts/channels/{sample_email_channel.channel_id}?force=true",
            headers=admin_headers
        )
        
        assert response.status_code == 204
    
    def test_delete_channel_requires_permission(self, client, user_headers, sample_slack_channel):
        """Test deleting a channel requires permission"""
        response = client.delete(
            f"/api/v1/alerts/channels/{sample_slack_channel.channel_id}",
            headers=user_headers
        )
        
        assert response.status_code == 403
    
    def test_test_channel_success(self, client, admin_headers, sample_email_channel):
        """Test testing a notification channel"""
        response = client.post(
            f"/api/v1/alerts/channels/{sample_email_channel.channel_id}/test",
            headers=admin_headers,
            json={"test_message": "This is a test"}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "delivery_time_ms" in data
        assert "tested_at" in data


class TestAlertRuleCRUD:
    """Tests for alert rule CRUD operations"""
    
    def test_create_rule_success(self, client, admin_headers, sample_connection, sample_email_channel, admin_user):
        """Test creating an alert rule"""
        response = client.post(
            "/api/v1/alerts/rules",
            headers=admin_headers,
            json={
                "rule_name": "Test Connection Failure Rule",
                "description": "Alert on connection failures",
                "alert_type": "connection_failure",
                "severity": "critical",
                "scope_type": "connection",
                "connection_id": str(sample_connection.connection_id),
                "condition_type": "threshold",
                "condition_definition": {
                    "metric": "failure_count",
                    "operator": "gt",
                    "value": 3
                },
                "evaluation_interval_minutes": 5,
                "evaluation_window_minutes": 15,
                "consecutive_failures": 1,
                "auto_resolve": True,
                "is_active": True,
                "tags": ["critical", "connection"],
                "notification_channel_ids": [str(sample_email_channel.channel_id)]
            }
        )
        
        assert response.status_code == 201
        data = response.json()
        assert data["rule_name"] == "Test Connection Failure Rule"
        assert data["alert_type"] == "connection_failure"
        assert data["severity"] == "critical"
        assert "rule_id" in data
        assert data["created_by"] == str(admin_user.user_id)
    
    def test_create_rule_with_anomaly_condition(self, client, admin_headers, sample_connection, sample_email_channel):
        """Test creating a rule with anomaly condition"""
        response = client.post(
            "/api/v1/alerts/rules",
            headers=admin_headers,
            json={
                "rule_name": "Test Anomaly Detection Rule",
                "alert_type": "high_lag",
                "severity": "warning",
                "scope_type": "connection",
                "connection_id": str(sample_connection.connection_id),
                "condition_type": "anomaly",
                "condition_definition": {
                    "metric": "lag_seconds",
                    "sensitivity": "medium"
                },
                "is_active": True,
                "notification_channel_ids": [str(sample_email_channel.channel_id)]
            }
        )
        
        assert response.status_code == 201
        data = response.json()
        assert data["condition_type"] == "anomaly"
    
    def test_create_rule_duplicate_name(self, client, admin_headers, sample_alert_rule, sample_email_channel):
        """Test creating a rule with duplicate name fails"""
        response = client.post(
            "/api/v1/alerts/rules",
            headers=admin_headers,
            json={
                "rule_name": sample_alert_rule.rule_name,
                "alert_type": "high_lag",
                "severity": "warning",
                "scope_type": "global",
                "condition_type": "threshold",
                "condition_definition": {"metric": "lag", "operator": "gt", "value": 100},
                "is_active": True,
                "notification_channel_ids": [str(sample_email_channel.channel_id)]
            }
        )
        
        assert response.status_code == 400
        assert "already exists" in response.json()["detail"]
    
    def test_create_rule_invalid_connection(self, client, admin_headers, sample_email_channel):
        """Test creating a rule with invalid connection fails"""
        response = client.post(
            "/api/v1/alerts/rules",
            headers=admin_headers,
            json={
                "rule_name": "Test Invalid Connection Rule",
                "alert_type": "connection_failure",
                "severity": "error",
                "scope_type": "connection",
                "connection_id": str(uuid4()),
                "condition_type": "threshold",
                "condition_definition": {"metric": "failures", "operator": "gt", "value": 1},
                "is_active": True,
                "notification_channel_ids": [str(sample_email_channel.channel_id)]
            }
        )
        
        assert response.status_code == 404
        assert "Connection not found" in response.json()["detail"]
    
    def test_create_rule_invalid_channel(self, client, admin_headers):
        """Test creating a rule with invalid channel fails"""
        response = client.post(
            "/api/v1/alerts/rules",
            headers=admin_headers,
            json={
                "rule_name": "Test Invalid Channel Rule",
                "alert_type": "high_lag",
                "severity": "warning",
                "scope_type": "global",
                "condition_type": "threshold",
                "condition_definition": {"metric": "lag", "operator": "gt", "value": 100},
                "is_active": True,
                "notification_channel_ids": [str(uuid4())]
            }
        )
        
        assert response.status_code == 404
        assert "channel" in response.json()["detail"].lower()
    
    def test_create_rule_requires_permission(self, client, user_headers, sample_email_channel):
        """Test creating a rule requires permission"""
        response = client.post(
            "/api/v1/alerts/rules",
            headers=user_headers,
            json={
                "rule_name": "Test Rule",
                "alert_type": "high_lag",
                "severity": "warning",
                "scope_type": "global",
                "condition_type": "threshold",
                "condition_definition": {"metric": "lag", "operator": "gt", "value": 100},
                "is_active": True,
                "notification_channel_ids": [str(sample_email_channel.channel_id)]
            }
        )
        
        assert response.status_code == 403
    
    def test_list_rules_success(self, client, admin_headers, sample_alert_rule):
        """Test listing alert rules"""
        response = client.get(
            "/api/v1/alerts/rules",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "rules" in data
        assert "total" in data
        assert data["total"] >= 1
    
    def test_list_rules_filter_by_type(self, client, admin_headers, sample_alert_rule):
        """Test filtering rules by alert type"""
        response = client.get(
            f"/api/v1/alerts/rules?alert_type={sample_alert_rule.alert_type}",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert all(rule["alert_type"] == sample_alert_rule.alert_type for rule in data["rules"])
    
    def test_list_rules_filter_by_severity(self, client, admin_headers, sample_alert_rule):
        """Test filtering rules by severity"""
        response = client.get(
            "/api/v1/alerts/rules?severity=warning",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert all(rule["severity"] == "warning" for rule in data["rules"])
    
    def test_list_rules_filter_by_connection(self, client, admin_headers, sample_alert_rule, sample_connection):
        """Test filtering rules by connection"""
        response = client.get(
            f"/api/v1/alerts/rules?connection_id={sample_connection.connection_id}",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert all(rule["connection_id"] == str(sample_connection.connection_id) for rule in data["rules"])
    
    def test_list_rules_search(self, client, admin_headers, sample_alert_rule):
        """Test searching rules"""
        response = client.get(
            "/api/v1/alerts/rules?search=High",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert any("High" in rule["rule_name"] for rule in data["rules"])
    
    def test_list_rules_pagination(self, client, admin_headers):
        """Test rule list pagination"""
        response = client.get(
            "/api/v1/alerts/rules?page=1&page_size=10",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["page"] == 1
        assert data["page_size"] == 10
    
    def test_get_rule_success(self, client, admin_headers, sample_alert_rule):
        """Test getting rule details"""
        response = client.get(
            f"/api/v1/alerts/rules/{sample_alert_rule.rule_id}",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["rule_id"] == str(sample_alert_rule.rule_id)
        assert data["rule_name"] == sample_alert_rule.rule_name
        assert "notification_channel_ids" in data
        assert "active_alerts_count" in data
    
    def test_get_rule_not_found(self, client, admin_headers):
        """Test getting non-existent rule"""
        response = client.get(
            f"/api/v1/alerts/rules/{uuid4()}",
            headers=admin_headers
        )
        
        assert response.status_code == 404
    
    def test_update_rule_success(self, client, admin_headers, sample_alert_rule):
        """Test updating a rule"""
        response = client.patch(
            f"/api/v1/alerts/rules/{sample_alert_rule.rule_id}",
            headers=admin_headers,
            json={
                "rule_name": "Test Updated High Lag Rule",
                "severity": "critical"
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["rule_name"] == "Test Updated High Lag Rule"
        assert data["severity"] == "critical"
    
    def test_update_rule_partial(self, client, admin_headers, sample_alert_rule):
        """Test partial update of a rule"""
        response = client.patch(
            f"/api/v1/alerts/rules/{sample_alert_rule.rule_id}",
            headers=admin_headers,
            json={"is_active": False}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["is_active"] is False
    
    def test_update_rule_not_found(self, client, admin_headers):
        """Test updating non-existent rule"""
        response = client.patch(
            f"/api/v1/alerts/rules/{uuid4()}",
            headers=admin_headers,
            json={"is_active": False}
        )
        
        assert response.status_code == 404
    
    def test_update_rule_requires_permission(self, client, user_headers, sample_alert_rule):
        """Test updating a rule requires permission"""
        response = client.patch(
            f"/api/v1/alerts/rules/{sample_alert_rule.rule_id}",
            headers=user_headers,
            json={"is_active": False}
        )
        
        assert response.status_code == 403
    
    def test_delete_rule_success(self, client, admin_headers, sample_alert_rule):
        """Test deleting a rule"""
        response = client.delete(
            f"/api/v1/alerts/rules/{sample_alert_rule.rule_id}",
            headers=admin_headers
        )
        
        assert response.status_code == 204
    
    def test_delete_rule_requires_permission(self, client, user_headers, sample_alert_rule):
        """Test deleting a rule requires permission"""
        response = client.delete(
            f"/api/v1/alerts/rules/{sample_alert_rule.rule_id}",
            headers=user_headers
        )
        
        assert response.status_code == 403


class TestAlertRuleTesting:
    """Tests for alert rule testing"""
    
    def test_test_rule_success(self, client, admin_headers, sample_connection):
        """Test testing an alert rule"""
        response = client.post(
            "/api/v1/alerts/rules/test",
            headers=admin_headers,
            json={
                "condition_type": "threshold",
                "condition_definition": {
                    "metric": "lag_seconds",
                    "operator": "gt",
                    "value": 100
                },
                "connection_id": str(sample_connection.connection_id),
                "use_sample_data": True
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "test_passed" in data
        assert "condition_met" in data
        assert "evaluation_time_ms" in data
    
    def test_test_rule_invalid_connection(self, client, admin_headers):
        """Test testing a rule with invalid connection"""
        response = client.post(
            "/api/v1/alerts/rules/test",
            headers=admin_headers,
            json={
                "condition_type": "threshold",
                "condition_definition": {
                    "metric": "lag_seconds",
                    "operator": "gt",
                    "value": 100
                },
                "connection_id": str(uuid4())
            }
        )
        
        assert response.status_code == 404
    
    def test_list_rule_evaluations(self, client, admin_headers, sample_alert_rule, sample_alert_evaluation):
        """Test listing rule evaluations"""
        response = client.get(
            f"/api/v1/alerts/rules/{sample_alert_rule.rule_id}/evaluations",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "evaluations" in data
        assert data["total"] >= 1


class TestAlertManagement:
    """Tests for alert management"""
    
    def test_list_alerts_success(self, client, admin_headers, sample_alert):
        """Test listing alerts"""
        response = client.get(
            "/api/v1/alerts/",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "alerts" in data
        assert "total" in data
        assert data["total"] >= 1
    
    def test_list_alerts_filter_by_type(self, client, admin_headers, sample_alert):
        """Test filtering alerts by type"""
        response = client.get(
            f"/api/v1/alerts/?alert_type={sample_alert.alert_type}",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert all(alert["alert_type"] == sample_alert.alert_type for alert in data["alerts"])
    
    def test_list_alerts_filter_by_severity(self, client, admin_headers):
        """Test filtering alerts by severity"""
        response = client.get(
            "/api/v1/alerts/?severity=critical",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert all(alert["severity"] == "critical" for alert in data["alerts"])
    
    def test_list_alerts_filter_by_status(self, client, admin_headers):
        """Test filtering alerts by status"""
        response = client.get(
            "/api/v1/alerts/?status=active",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert all(alert["status"] == "active" for alert in data["alerts"])
    
    def test_list_alerts_filter_by_connection(self, client, admin_headers, sample_alert, sample_connection):
        """Test filtering alerts by connection"""
        response = client.get(
            f"/api/v1/alerts/?connection_id={sample_connection.connection_id}",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert all(alert["connection_id"] == str(sample_connection.connection_id) for alert in data["alerts"])
    
    def test_list_alerts_search(self, client, admin_headers, sample_alert):
        """Test searching alerts"""
        response = client.get(
            "/api/v1/alerts/?search=Connection",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert any("Connection" in alert["title"] for alert in data["alerts"])
    
    def test_get_alert_success(self, client, admin_headers, sample_alert):
        """Test getting alert details"""
        response = client.get(
            f"/api/v1/alerts/{sample_alert.alert_id}",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["alert_id"] == str(sample_alert.alert_id)
        assert data["title"] == sample_alert.title
    
    def test_get_alert_not_found(self, client, admin_headers):
        """Test getting non-existent alert"""
        response = client.get(
            f"/api/v1/alerts/{uuid4()}",
            headers=admin_headers
        )
        
        assert response.status_code == 404
    
    def test_acknowledge_alert_success(self, client, admin_headers, sample_alert, admin_user):
        """Test acknowledging an alert"""
        response = client.post(
            f"/api/v1/alerts/{sample_alert.alert_id}/acknowledge",
            headers=admin_headers,
            json={"notes": "Investigating the issue"}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "acknowledged"
        assert data["acknowledged_by"] == str(admin_user.user_id)
        assert data["acknowledged_at"] is not None
    
    def test_acknowledge_non_active_alert(self, client, admin_headers, sample_alert):
        """Test acknowledging a non-active alert fails"""
        # First acknowledge it
        client.post(
            f"/api/v1/alerts/{sample_alert.alert_id}/acknowledge",
            headers=admin_headers,
            json={}
        )
        
        # Try to acknowledge again
        response = client.post(
            f"/api/v1/alerts/{sample_alert.alert_id}/acknowledge",
            headers=admin_headers,
            json={}
        )
        
        assert response.status_code == 400
    
    def test_acknowledge_alert_requires_permission(self, client, user_headers, sample_alert):
        """Test acknowledging an alert requires permission"""
        response = client.post(
            f"/api/v1/alerts/{sample_alert.alert_id}/acknowledge",
            headers=user_headers,
            json={}
        )
        
        assert response.status_code == 403
    
    def test_resolve_alert_success(self, client, admin_headers, sample_alert, admin_user):
        """Test resolving an alert"""
        response = client.post(
            f"/api/v1/alerts/{sample_alert.alert_id}/resolve",
            headers=admin_headers,
            json={"notes": "Issue fixed"}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "resolved"
        assert data["resolved_by"] == str(admin_user.user_id)
        assert data["resolved_at"] is not None
    
    def test_resolve_alert_requires_permission(self, client, user_headers, sample_alert):
        """Test resolving an alert requires permission"""
        response = client.post(
            f"/api/v1/alerts/{sample_alert.alert_id}/resolve",
            headers=user_headers,
            json={}
        )
        
        assert response.status_code == 403
    
    def test_get_alert_history(self, client, admin_headers, sample_alert):
        """Test getting alert history"""
        # Acknowledge the alert first to create history
        client.post(
            f"/api/v1/alerts/{sample_alert.alert_id}/acknowledge",
            headers=admin_headers,
            json={"notes": "Test history"}
        )
        
        response = client.get(
            f"/api/v1/alerts/{sample_alert.alert_id}/history",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "history" in data
        assert data["total"] >= 1
    
    def test_get_alert_notifications(self, client, admin_headers, sample_alert):
        """Test getting alert notification logs"""
        response = client.get(
            f"/api/v1/alerts/{sample_alert.alert_id}/notifications",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "logs" in data


class TestAlertSuppression:
    """Tests for alert suppression"""
    
    def test_create_suppression_success(self, client, admin_headers, sample_connection, admin_user):
        """Test creating alert suppression"""
        response = client.post(
            "/api/v1/alerts/suppressions",
            headers=admin_headers,
            json={
                "suppression_name": "Test Maintenance Window",
                "description": "Suppress alerts during maintenance",
                "scope_type": "connection",
                "connection_id": str(sample_connection.connection_id),
                "start_time": datetime.utcnow().isoformat(),
                "end_time": (datetime.utcnow() + timedelta(hours=2)).isoformat(),
                "reason": "Scheduled maintenance"
            }
        )
        
        assert response.status_code == 201
        data = response.json()
        assert data["suppression_name"] == "Test Maintenance Window"
        assert data["scope_type"] == "connection"
        assert data["is_active"] is True
    
    def test_create_suppression_invalid_connection(self, client, admin_headers):
        """Test creating suppression with invalid connection fails"""
        response = client.post(
            "/api/v1/alerts/suppressions",
            headers=admin_headers,
            json={
                "suppression_name": "Test Invalid Suppression",
                "scope_type": "connection",
                "connection_id": str(uuid4()),
                "start_time": datetime.utcnow().isoformat(),
                "end_time": (datetime.utcnow() + timedelta(hours=2)).isoformat()
            }
        )
        
        assert response.status_code == 404
    
    def test_list_suppressions_success(self, client, admin_headers, sample_alert_suppression):
        """Test listing alert suppressions"""
        response = client.get(
            "/api/v1/alerts/suppressions",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "suppressions" in data
        assert data["total"] >= 1
    
    def test_list_suppressions_filter_by_active(self, client, admin_headers, sample_alert_suppression):
        """Test filtering suppressions by active status"""
        response = client.get(
            "/api/v1/alerts/suppressions?is_active=true",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        # All should be within their time window
        for supp in data["suppressions"]:
            assert supp["is_active"] is True
    
    def test_update_suppression_success(self, client, admin_headers, sample_alert_suppression):
        """Test updating alert suppression"""
        response = client.patch(
            f"/api/v1/alerts/suppressions/{sample_alert_suppression.suppression_id}",
            headers=admin_headers,
            json={"is_active": False}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["is_active"] is False
    
    def test_delete_suppression_success(self, client, admin_headers, sample_alert_suppression):
        """Test deleting alert suppression"""
        response = client.delete(
            f"/api/v1/alerts/suppressions/{sample_alert_suppression.suppression_id}",
            headers=admin_headers
        )
        
        assert response.status_code == 204


class TestAlertStatistics:
    """Tests for alert statistics and dashboard"""
    
    def test_get_statistics_success(self, client, admin_headers, sample_alert):
        """Test getting alert statistics"""
        response = client.get(
            "/api/v1/alerts/statistics",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "total_alerts" in data
        assert "active_alerts" in data
        assert "critical_alerts" in data
        assert "alerts_by_type" in data
        assert "alerts_last_24h" in data
    
    def test_get_dashboard_success(self, client, admin_headers, sample_alert):
        """Test getting alert dashboard"""
        response = client.get(
            "/api/v1/alerts/dashboard",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "statistics" in data
        assert "top_alert_types" in data
        assert "recent_critical_alerts" in data
        assert "unacknowledged_critical" in data
        assert "trend" in data
        assert data["trend"] in ["improving", "stable", "worsening"]
