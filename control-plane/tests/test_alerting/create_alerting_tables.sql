-- Alerting tables for TODO #11

CREATE TABLE IF NOT EXISTS notification_channels (
    channel_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    bank_id UUID NOT NULL,
    sub_tenant_id UUID NOT NULL,
    channel_name VARCHAR(255) NOT NULL,
    channel_type VARCHAR(50) NOT NULL,
    description TEXT,
    config JSONB NOT NULL,
    auth_config JSONB,
    is_active BOOLEAN NOT NULL DEFAULT true,
    is_verified BOOLEAN NOT NULL DEFAULT false,
    verified_at TIMESTAMP WITH TIME ZONE,
    last_test_at TIMESTAMP WITH TIME ZONE,
    last_test_status VARCHAR(50),
    last_test_error TEXT,
    rate_limit_per_hour INTEGER,
    rate_limit_per_day INTEGER,
    tags JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_by UUID,
    updated_by UUID,
    is_deleted BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_notification_channels_bank_id ON notification_channels(bank_id);
CREATE INDEX IF NOT EXISTS ix_notification_channels_sub_tenant_id ON notification_channels(sub_tenant_id);
CREATE INDEX IF NOT EXISTS ix_notification_channels_channel_type ON notification_channels(channel_type);

CREATE TABLE IF NOT EXISTS alert_rules (
    rule_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    bank_id UUID NOT NULL,
    sub_tenant_id UUID NOT NULL,
    rule_name VARCHAR(255) NOT NULL,
    description TEXT,
    alert_type VARCHAR(50) NOT NULL,
    severity VARCHAR(50) NOT NULL,
    scope_type VARCHAR(50) NOT NULL,
    scope_id UUID,
    connection_id UUID,
    source_id UUID,
    destination_id UUID,
    condition_type VARCHAR(50) NOT NULL,
    condition_definition JSONB NOT NULL,
    threshold_value NUMERIC,
    consecutive_failures_required INTEGER NOT NULL DEFAULT 1,
    evaluation_window_minutes INTEGER NOT NULL DEFAULT 5,
    cooldown_minutes INTEGER NOT NULL DEFAULT 15,
    auto_resolve BOOLEAN NOT NULL DEFAULT false,
    auto_resolve_after_minutes INTEGER,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_by UUID,
    updated_by UUID,
    is_deleted BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_alert_rules_bank_id ON alert_rules(bank_id);
CREATE INDEX IF NOT EXISTS ix_alert_rules_sub_tenant_id ON alert_rules(sub_tenant_id);
CREATE INDEX IF NOT EXISTS ix_alert_rules_connection_id ON alert_rules(connection_id);
CREATE INDEX IF NOT EXISTS ix_alert_rules_alert_type ON alert_rules(alert_type);
CREATE INDEX IF NOT EXISTS ix_alert_rules_severity ON alert_rules(severity);

CREATE TABLE IF NOT EXISTS alert_rule_channels (
    rule_channel_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rule_id UUID NOT NULL REFERENCES alert_rules(rule_id),
    channel_id UUID NOT NULL REFERENCES notification_channels(channel_id),
    priority INTEGER NOT NULL DEFAULT 1,
    severity_filter VARCHAR(50)[],
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    UNIQUE(rule_id, channel_id)
);

CREATE INDEX IF NOT EXISTS ix_alert_rule_channels_rule_id ON alert_rule_channels(rule_id);
CREATE INDEX IF NOT EXISTS ix_alert_rule_channels_channel_id ON alert_rule_channels(channel_id);

CREATE TABLE IF NOT EXISTS alert_escalation_policies (
    policy_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rule_id UUID NOT NULL REFERENCES alert_rules(rule_id),
    escalation_level INTEGER NOT NULL,
    escalate_after_minutes INTEGER NOT NULL,
    channel_ids UUID[] NOT NULL,
    message_template TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_alert_escalation_policies_rule_id ON alert_escalation_policies(rule_id);

CREATE TABLE IF NOT EXISTS alert_evaluations (
    evaluation_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rule_id UUID NOT NULL REFERENCES alert_rules(rule_id),
    evaluated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    passed BOOLEAN NOT NULL,
    metric_value NUMERIC,
    threshold_value NUMERIC,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    evaluation_details JSONB NOT NULL DEFAULT '{}'::jsonb,
    execution_time_ms INTEGER,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_alert_evaluations_rule_id ON alert_evaluations(rule_id);
CREATE INDEX IF NOT EXISTS ix_alert_evaluations_evaluated_at ON alert_evaluations(evaluated_at);

CREATE TABLE IF NOT EXISTS alert_history (
    history_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    alert_id UUID NOT NULL,
    old_status VARCHAR(50),
    new_status VARCHAR(50) NOT NULL,
    changed_by UUID,
    changed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    change_reason TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_alert_history_alert_id ON alert_history(alert_id);
CREATE INDEX IF NOT EXISTS ix_alert_history_changed_at ON alert_history(changed_at);

CREATE TABLE IF NOT EXISTS alert_notification_logs (
    log_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    alert_id UUID NOT NULL,
    channel_id UUID NOT NULL REFERENCES notification_channels(channel_id),
    sent_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    status VARCHAR(50) NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    delivery_time_ms INTEGER,
    error_message TEXT,
    response_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_alert_notification_logs_alert_id ON alert_notification_logs(alert_id);
CREATE INDEX IF NOT EXISTS ix_alert_notification_logs_channel_id ON alert_notification_logs(channel_id);
CREATE INDEX IF NOT EXISTS ix_alert_notification_logs_sent_at ON alert_notification_logs(sent_at);

CREATE TABLE IF NOT EXISTS alert_suppressions (
    suppression_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    bank_id UUID NOT NULL,
    sub_tenant_id UUID NOT NULL,
    suppression_name VARCHAR(255) NOT NULL,
    description TEXT,
    scope_type VARCHAR(50) NOT NULL,
    rule_ids UUID[],
    connection_ids UUID[],
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time TIMESTAMP WITH TIME ZONE NOT NULL,
    is_recurring BOOLEAN NOT NULL DEFAULT false,
    recurrence_pattern JSONB,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_by UUID,
    updated_by UUID,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_alert_suppressions_bank_id ON alert_suppressions(bank_id);
CREATE INDEX IF NOT EXISTS ix_alert_suppressions_sub_tenant_id ON alert_suppressions(sub_tenant_id);
CREATE INDEX IF NOT EXISTS ix_alert_suppressions_start_time ON alert_suppressions(start_time);
CREATE INDEX IF NOT EXISTS ix_alert_suppressions_end_time ON alert_suppressions(end_time);
