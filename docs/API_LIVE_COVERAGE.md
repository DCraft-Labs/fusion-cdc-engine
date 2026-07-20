# Fusion CDC Engine - Control Plane — Live Coverage

- Version: `0.1.0`
- Source: `docs/openapi.json`
- Base URL (live): `http://127.0.0.1:18000`
- Total paths: **138**

- Total operations: **184**
## Operations

### `GET /`

**Summary:** Root

**Operation ID:** `root__get`

API root

**Responses:**

- `200` — Successful Response

---

### `GET /api/v1/alerts`

**Summary:** List Alerts

**Operation ID:** `list_alerts_api_v1_alerts_get`

List alerts with filters

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `alert_type` | query | False | any |
| `severity` | query | False | any |
| `status` | query | False | any |
| `connection_id` | query | False | any |
| `source_id` | query | False | any |
| `destination_id` | query | False | any |
| `search` | query | False | any |
| `page` | query | False | integer |
| `page_size` | query | False | integer |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/alerts/channels`

**Summary:** Create Notification Channel

**Operation ID:** `create_notification_channel_api_v1_alerts_channels_post`

Create a new notification channel

**Request body (`application/json`):**

```
- Schema for creating a notification channel
- `channel_name`: string
- `channel_type`: string
- `description`: any
- `config`: object
  - Channel-specific configuration
- `auth_config`: any
- `is_active`: boolean
- `rate_limit_per_hour`: any
- `rate_limit_per_day`: any
- `tags`: array<string>
```

**Responses:**

- `201` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/alerts/channels`

**Summary:** List Notification Channels

**Operation ID:** `list_notification_channels_api_v1_alerts_channels_get`

List notification channels with filters

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `channel_type` | query | False | any |
| `is_active` | query | False | any |
| `is_verified` | query | False | any |
| `search` | query | False | any |
| `page` | query | False | integer |
| `page_size` | query | False | integer |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/alerts/channels/{channel_id}`

**Summary:** Get Notification Channel

**Operation ID:** `get_notification_channel_api_v1_alerts_channels__channel_id__get`

Get notification channel details

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `channel_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `PATCH /api/v1/alerts/channels/{channel_id}`

**Summary:** Update Notification Channel

**Operation ID:** `update_notification_channel_api_v1_alerts_channels__channel_id__patch`

Update notification channel

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `channel_id` | path | True | string |

**Request body (`application/json`):**

```
- Schema for updating a notification channel
- `channel_name`: any
- `description`: any
- `config`: any
- `auth_config`: any
- `is_active`: any
- `rate_limit_per_hour`: any
- `rate_limit_per_day`: any
- `tags`: any
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `DELETE /api/v1/alerts/channels/{channel_id}`

**Summary:** Delete Notification Channel

**Operation ID:** `delete_notification_channel_api_v1_alerts_channels__channel_id__delete`

Delete notification channel (soft delete)

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `channel_id` | path | True | string |
| `force` | query | False | boolean |

**Responses:**

- `204` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/alerts/channels/{channel_id}/test`

**Summary:** Test Notification Channel

**Operation ID:** `test_notification_channel_api_v1_alerts_channels__channel_id__test_post`

Test notification channel

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `channel_id` | path | True | string |

**Request body (`application/json`):**

```
- Schema for testing a notification channel
- `test_message`: any
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/alerts/dashboard`

**Summary:** Get Alert Dashboard

**Operation ID:** `get_alert_dashboard_api_v1_alerts_dashboard_get`

Get alert dashboard data

**Responses:**

- `200` — Successful Response

---

### `POST /api/v1/alerts/rules`

**Summary:** Create Alert Rule

**Operation ID:** `create_alert_rule_api_v1_alerts_rules_post`

Create a new alert rule

**Request body (`application/json`):**

```
- Schema for creating an alert rule
- `rule_name`: string
- `description`: any
- `alert_type`: string
- `severity`: string
- `scope_type`: string
- `connection_id`: any
- `source_id`: any
- `destination_id`: any
- `stream_id`: any
- `condition_type`: string
- `condition_definition`: object
  - Condition definition
- `evaluation_interval_minutes`: integer
- `evaluation_window_minutes`: integer
- `consecutive_failures`: integer
- `auto_resolve`: boolean
- `auto_resolve_after_minutes`: any
- `group_by`: array<string>
- `suppression_window_minutes`: any
- `is_active`: boolean
- `tags`: array<string>
- `custom_labels`: object
  - Custom labels
- `notification_channel_ids`: array<string>
```

**Responses:**

- `201` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/alerts/rules`

**Summary:** List Alert Rules

**Operation ID:** `list_alert_rules_api_v1_alerts_rules_get`

List alert rules with filters

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `alert_type` | query | False | any |
| `severity` | query | False | any |
| `scope_type` | query | False | any |
| `connection_id` | query | False | any |
| `is_active` | query | False | any |
| `search` | query | False | any |
| `page` | query | False | integer |
| `page_size` | query | False | integer |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/alerts/rules/test`

**Summary:** Test Alert Rule

**Operation ID:** `test_alert_rule_api_v1_alerts_rules_test_post`

Test alert rule evaluation

**Request body (`application/json`):**

```
- Schema for testing an alert rule
- `condition_type`: string
- `condition_definition`: object
  - Condition definition
- `connection_id`: any
- `use_sample_data`: boolean
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/alerts/rules/{rule_id}`

**Summary:** Get Alert Rule

**Operation ID:** `get_alert_rule_api_v1_alerts_rules__rule_id__get`

Get alert rule details

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `rule_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `PATCH /api/v1/alerts/rules/{rule_id}`

**Summary:** Update Alert Rule

**Operation ID:** `update_alert_rule_api_v1_alerts_rules__rule_id__patch`

Update alert rule

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `rule_id` | path | True | string |

**Request body (`application/json`):**

```
- Schema for updating an alert rule
- `rule_name`: any
- `description`: any
- `severity`: any
- `condition_definition`: any
- `evaluation_interval_minutes`: any
- `evaluation_window_minutes`: any
- `consecutive_failures`: any
- `auto_resolve`: any
- `auto_resolve_after_minutes`: any
- `group_by`: any
- `suppression_window_minutes`: any
- `is_active`: any
- `tags`: any
- `custom_labels`: any
- `notification_channel_ids`: any
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `DELETE /api/v1/alerts/rules/{rule_id}`

**Summary:** Delete Alert Rule

**Operation ID:** `delete_alert_rule_api_v1_alerts_rules__rule_id__delete`

Delete alert rule (soft delete)

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `rule_id` | path | True | string |

**Responses:**

- `204` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/alerts/rules/{rule_id}/evaluations`

**Summary:** List Rule Evaluations

**Operation ID:** `list_rule_evaluations_api_v1_alerts_rules__rule_id__evaluations_get`

List evaluation history for an alert rule

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `rule_id` | path | True | string |
| `page` | query | False | integer |
| `page_size` | query | False | integer |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/alerts/statistics`

**Summary:** Get Alert Statistics

**Operation ID:** `get_alert_statistics_api_v1_alerts_statistics_get`

Get alert statistics

**Responses:**

- `200` — Successful Response

---

### `POST /api/v1/alerts/suppressions`

**Summary:** Create Alert Suppression

**Operation ID:** `create_alert_suppression_api_v1_alerts_suppressions_post`

Create alert suppression

**Request body (`application/json`):**

```
- Schema for creating an alert suppression
- `suppression_name`: string
- `description`: any
- `scope_type`: string
- `rule_ids`: any
- `connection_ids`: any
- `start_time`: string
- `end_time`: string
- `is_recurring`: boolean
- `recurrence_pattern`: any
```

**Responses:**

- `201` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/alerts/suppressions`

**Summary:** List Alert Suppressions

**Operation ID:** `list_alert_suppressions_api_v1_alerts_suppressions_get`

List alert suppressions

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `is_active` | query | False | any |
| `scope_type` | query | False | any |
| `page` | query | False | integer |
| `page_size` | query | False | integer |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `PATCH /api/v1/alerts/suppressions/{suppression_id}`

**Summary:** Update Alert Suppression

**Operation ID:** `update_alert_suppression_api_v1_alerts_suppressions__suppression_id__patch`

Update alert suppression

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `suppression_id` | path | True | string |

**Request body (`application/json`):**

```
- Schema for updating an alert suppression
- `suppression_name`: any
- `description`: any
- `end_time`: any
- `is_active`: any
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `DELETE /api/v1/alerts/suppressions/{suppression_id}`

**Summary:** Delete Alert Suppression

**Operation ID:** `delete_alert_suppression_api_v1_alerts_suppressions__suppression_id__delete`

Delete alert suppression

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `suppression_id` | path | True | string |

**Responses:**

- `204` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/alerts/{alert_id}`

**Summary:** Get Alert

**Operation ID:** `get_alert_api_v1_alerts__alert_id__get`

Get alert details

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `alert_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/alerts/{alert_id}/acknowledge`

**Summary:** Acknowledge Alert

**Operation ID:** `acknowledge_alert_api_v1_alerts__alert_id__acknowledge_post`

Acknowledge an alert

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `alert_id` | path | True | string |

**Request body (`application/json`):**

```
- Schema for acknowledging an alert
- `notes`: any
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/alerts/{alert_id}/history`

**Summary:** Get Alert History

**Operation ID:** `get_alert_history_api_v1_alerts__alert_id__history_get`

Get alert history

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `alert_id` | path | True | string |
| `page` | query | False | integer |
| `page_size` | query | False | integer |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/alerts/{alert_id}/notifications`

**Summary:** Get Alert Notifications

**Operation ID:** `get_alert_notifications_api_v1_alerts__alert_id__notifications_get`

Get alert notification logs

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `alert_id` | path | True | string |
| `page` | query | False | integer |
| `page_size` | query | False | integer |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/alerts/{alert_id}/resolve`

**Summary:** Resolve Alert

**Operation ID:** `resolve_alert_api_v1_alerts__alert_id__resolve_post`

Resolve an alert

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `alert_id` | path | True | string |

**Request body (`application/json`):**

```
- Schema for resolving an alert
- `notes`: any
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/auth/change-password`

**Summary:** Change Password

**Operation ID:** `change_password_api_v1_auth_change_password_post`

Change current user password

Requires current password verification.

**Request body (`application/json`):**

```
- Change password request
- `current_password`: string
- `new_password`: string
```

**Responses:**

- `204` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/auth/login`

**Summary:** Login

**Operation ID:** `login_api_v1_auth_login_post`

Login with username/email and password

Returns access token and refresh token.
Access token expires in 30 minutes, refresh token in 7 days.

**Request body (`application/json`):**

```
- Login request with username/email and password
- `username`: string
- `password`: string
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/auth/logout`

**Summary:** Logout

**Operation ID:** `logout_api_v1_auth_logout_post`

Logout user and revoke refresh token

Revokes the provided refresh token or all tokens for the user.

**Request body (`application/json`):**

```
- Logout request (revoke refresh token)
- `refresh_token`: any
```

**Responses:**

- `204` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/auth/me`

**Summary:** Get Current User Info

**Operation ID:** `get_current_user_info_api_v1_auth_me_get`

Get current authenticated user information

Returns user profile with roles and permissions.

**Responses:**

- `200` — Successful Response

---

### `PATCH /api/v1/auth/me`

**Summary:** Update Current User

**Operation ID:** `update_current_user_api_v1_auth_me_patch`

Update current user profile

Users can update their own email and name.

**Request body (`application/json`):**

```
- Update user request
- `email`: any
- `first_name`: any
- `last_name`: any
- `is_active`: any
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/auth/refresh`

**Summary:** Refresh Token

**Operation ID:** `refresh_token_api_v1_auth_refresh_post`

Refresh access token using refresh token

Exchange a valid refresh token for a new access token.

**Request body (`application/json`):**

```
- Refresh token request
- `refresh_token`: string
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/auth/register`

**Summary:** Register

**Operation ID:** `register_api_v1_auth_register_post`

Register a new user

Creates a new user account with hashed password.
Superadmin privilege required to create superuser accounts.

**Request body (`application/json`):**

```
- Create user request
- `username`: string
- `email`: string
- `first_name`: any
- `last_name`: any
- `password`: string
- `bank_id`: any
- `sub_tenant_id`: any
- `is_superuser`: boolean
```

**Responses:**

- `201` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/connections`

**Summary:** Create Connection

**Operation ID:** `create_connection_api_v1_connections_post`

Create a new connection between source and destination

Requires: connections:create permission

**Request body (`application/json`):**

```
- Schema for creating a new connection
- `connection_name`: string
- `source_id`: string
- `destination_id`: string
- `sync_mode`: string
- `sync_type`: any
- `sync_frequency`: any
- `sync_enabled`: boolean
- `resource_limits`: object
  - Resource constraints (max_memory, max_cpu, max_parallelism)
- `replication_slot`: any
- `publication`: any
- `namespace_definition`: any
- `namespace_format`: any
- `stream_prefix`: any
- `config`: object
  - Additional settings
- `status`: any
- `streams`: any
```

**Responses:**

- `201` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/connections`

**Summary:** List Connections

**Operation ID:** `list_connections_api_v1_connections_get`

List all connections for the current tenant

Supports filtering by:
- status: draft, active, paused, inactive
- sync_mode: cdc, full_refresh, incremental
- source_id: specific source
- destination_id: specific destination
- search: search in connection name

Results are paginated.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `status` | query | False | any |
| `sync_mode` | query | False | any |
| `source_id` | query | False | any |
| `destination_id` | query | False | any |
| `search` | query | False | any |
| `page` | query | False | integer |
| `page_size` | query | False | integer |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/connections/validate`

**Summary:** Validate Connection

**Operation ID:** `validate_connection_api_v1_connections_validate_post`

Validate connection compatibility before creation

Checks:
- Source and destination accessibility
- Connector compatibility
- Sync mode support
- Connection test results

**Request body (`application/json`):**

```
- Schema for validating connection compatibility
- `source_id`: string
- `destination_id`: string
- `sync_mode`: string
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/connections/{connection_id}`

**Summary:** Get Connection

**Operation ID:** `get_connection_api_v1_connections__connection_id__get`

Get connection details by ID

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `PATCH /api/v1/connections/{connection_id}`

**Summary:** Update Connection

**Operation ID:** `update_connection_api_v1_connections__connection_id__patch`

Update connection configuration

Requires: connections:update permission

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |

**Request body (`application/json`):**

```
- Schema for updating a connection
- `connection_name`: any
- `sync_mode`: any
- `sync_frequency`: any
- `sync_enabled`: any
- `resource_limits`: any
- `replication_slot`: any
- `publication`: any
- `namespace_definition`: any
- `namespace_format`: any
- `stream_prefix`: any
- `config`: any
- `status`: any
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `DELETE /api/v1/connections/{connection_id}`

**Summary:** Delete Connection

**Operation ID:** `delete_connection_api_v1_connections__connection_id__delete`

Delete connection (soft delete)

Requires: connections:delete permission

Will fail if connection is active unless force=true.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |
| `force` | query | False | boolean |

**Responses:**

- `204` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/connections/{connection_id}/activate`

**Summary:** Activate Connection

**Operation ID:** `activate_connection_api_v1_connections__connection_id__activate_post`

Activate connection and start syncing

Requires: connections:update permission

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |

**Request body (`application/json`):**

```

```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/connections/{connection_id}/health`

**Summary:** Get Connection Health

**Operation ID:** `get_connection_health_api_v1_connections__connection_id__health_get`

Get connection health status with lag and throughput history.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/connections/{connection_id}/initial-load`

**Summary:** Get Initial Load Status

**Operation ID:** `get_initial_load_status_api_v1_connections__connection_id__initial_load_get`

Get initial load checkpoint details per table for a connection.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/connections/{connection_id}/pause`

**Summary:** Pause Connection

**Operation ID:** `pause_connection_api_v1_connections__connection_id__pause_post`

Pause connection (stop scheduled syncs)

Requires: connections:update permission

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/connections/{connection_id}/resume`

**Summary:** Resume Connection

**Operation ID:** `resume_connection_api_v1_connections__connection_id__resume_post`

Resume paused connection

Requires: connections:update permission

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/connections/{connection_id}/runs`

**Summary:** Get Connection Runs

**Operation ID:** `get_connection_runs_api_v1_connections__connection_id__runs_get`

Get sync run history for a connection, including initial load checkpoints.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/connections/{connection_id}/schedule`

**Summary:** Configure Schedule

**Operation ID:** `configure_schedule_api_v1_connections__connection_id__schedule_post`

Configure connection sync schedule

Requires: connections:update permission

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |

**Request body (`application/json`):**

```
- Schema for configuring connection schedule
- `sync_frequency`: string
- `sync_enabled`: boolean
- `timezone`: string
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/connections/{connection_id}/schedule`

**Summary:** Get Schedule

**Operation ID:** `get_schedule_api_v1_connections__connection_id__schedule_get`

Get current schedule configuration

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/connections/{connection_id}/stats`

**Summary:** Get Connection Stats

**Operation ID:** `get_connection_stats_api_v1_connections__connection_id__stats_get`

Get connection usage statistics

Returns sync count, data volume, and performance metrics.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/connections/{connection_id}/streams`

**Summary:** Add Stream

**Operation ID:** `add_stream_api_v1_connections__connection_id__streams_post`

Add a new stream to connection

Requires: connections:update permission

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |

**Request body (`application/json`):**

```
- Schema for creating a new stream
- `stream_name`: string
- `stream_namespace`: any
- `source_table_name`: string
- `source_schema_name`: any
- `destination_table_name`: string
- `destination_schema_name`: any
- `sync_mode`: string
- `cursor_field`: any
- `primary_keys`: array<string>
- `source_schema`: any
- `destination_schema`: any
- `selected_columns`: any
- `column_mapping`: object
  - Column name mappings
- `is_enabled`: boolean
- `transform_steps`: any
```

**Responses:**

- `201` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/connections/{connection_id}/streams`

**Summary:** List Streams

**Operation ID:** `list_streams_api_v1_connections__connection_id__streams_get`

List all streams for a connection

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `PATCH /api/v1/connections/{connection_id}/streams/{stream_id}`

**Summary:** Update Stream

**Operation ID:** `update_stream_api_v1_connections__connection_id__streams__stream_id__patch`

Update stream configuration

Requires: connections:update permission

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |
| `stream_id` | path | True | string |

**Request body (`application/json`):**

```
- Schema for updating a stream
- `stream_name`: any
- `stream_namespace`: any
- `destination_table_name`: any
- `destination_schema_name`: any
- `sync_mode`: any
- `cursor_field`: any
- `primary_keys`: any
- `selected_columns`: any
- `column_mapping`: any
- `transform_steps`: any
- `is_enabled`: any
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `DELETE /api/v1/connections/{connection_id}/streams/{stream_id}`

**Summary:** Delete Stream

**Operation ID:** `delete_stream_api_v1_connections__connection_id__streams__stream_id__delete`

Delete stream from connection

Requires: connections:update permission

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |
| `stream_id` | path | True | string |

**Responses:**

- `204` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/connections/{connection_id}/trigger-sync`

**Summary:** Trigger Manual Sync

**Operation ID:** `trigger_manual_sync_api_v1_connections__connection_id__trigger_sync_post`

Trigger manual sync for connection

Requires: connections:update permission

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |

**Request body (`application/json`):**

```

```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/connector-definitions`

**Summary:** List Connector Definitions

**Operation ID:** `list_connector_definitions_api_v1_connector_definitions_get`

List all available connector definitions.

Returns system-wide list of supported source and destination connectors
with filtering, search, and pagination.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `category` | query | False | any |
| `connector_type` | query | False | any |
| `supports_cdc` | query | False | any |
| `is_active` | query | False | any |
| `search` | query | False | any |
| `page` | query | False | integer |
| `page_size` | query | False | integer |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/connector-definitions`

**Summary:** Create Connector Definition

**Operation ID:** `create_connector_definition_api_v1_connector_definitions_post`

Create a new connector definition.

Requires: connector_definitions:create permission (superadmin only)

**Request body (`application/json`):**

```
- Create connector definition request
- `connector_name`: string
- `connector_type`: string
- `category`: string
- `latest_version`: string
- `default_config`: object
- `required_fields`: array<string>
- `optional_fields`: array<string>
- `default_resource_limits`: object
- `supports_cdc`: boolean
- `supports_full_refresh`: boolean
- `supports_incremental`: boolean
- `documentation_url`: any
- `icon_url`: any
```

**Responses:**

- `201` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/connector-definitions/{connector_id}`

**Summary:** Get Connector Definition

**Operation ID:** `get_connector_definition_api_v1_connector_definitions__connector_id__get`

Get detailed connector definition including configuration schema.

Returns complete connector metadata, capabilities, and configuration requirements.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connector_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `PATCH /api/v1/connector-definitions/{connector_id}`

**Summary:** Update Connector Definition

**Operation ID:** `update_connector_definition_api_v1_connector_definitions__connector_id__patch`

Update connector definition.

Requires: connector_definitions:update permission (superadmin only)

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connector_id` | path | True | string |

**Request body (`application/json`):**

```
- Update connector definition request
- `connector_name`: any
- `latest_version`: any
- `default_config`: any
- `required_fields`: any
- `optional_fields`: any
- `default_resource_limits`: any
- `supports_cdc`: any
- `supports_full_refresh`: any
- `supports_incremental`: any
- `documentation_url`: any
- `icon_url`: any
- `is_active`: any
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `DELETE /api/v1/connector-definitions/{connector_id}`

**Summary:** Delete Connector Definition

**Operation ID:** `delete_connector_definition_api_v1_connector_definitions__connector_id__delete`

Delete connector definition.

Requires: connector_definitions:delete permission (superadmin only)
Cannot delete if connector is in use by any sources or destinations.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connector_id` | path | True | string |

**Responses:**

- `204` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/connector-definitions/{connector_id}/capabilities`

**Summary:** Get Connector Capabilities

**Operation ID:** `get_connector_capabilities_api_v1_connector_definitions__connector_id__capabilities_get`

Get connector capabilities.

Returns supported sync modes and features.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connector_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/connector-definitions/{connector_id}/config-schema`

**Summary:** Get Connector Config Schema

**Operation ID:** `get_connector_config_schema_api_v1_connector_definitions__connector_id__config_schema_get`

Get connector configuration schema.

Returns required fields, optional fields, and default configuration.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connector_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/connector-definitions/{connector_id}/stats`

**Summary:** Get Connector Stats

**Operation ID:** `get_connector_stats_api_v1_connector_definitions__connector_id__stats_get`

Get connector usage statistics.

Returns count of sources, destinations, and connections using this connector.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connector_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/connector-definitions/{connector_id}/usage`

**Summary:** Get Connector Usage

**Operation ID:** `get_connector_usage_api_v1_connector_definitions__connector_id__usage_get`

Get list of sources and destinations using this connector.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connector_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/connector-definitions/{connector_id}/versions`

**Summary:** List Connector Versions

**Operation ID:** `list_connector_versions_api_v1_connector_definitions__connector_id__versions_get`

List all versions for a connector.

Returns version history with release notes and stability status.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connector_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/connector-definitions/{connector_id}/versions`

**Summary:** Create Connector Version

**Operation ID:** `create_connector_version_api_v1_connector_definitions__connector_id__versions_post`

Create a new connector version.

Requires: connector_definitions:create permission (superadmin only)

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connector_id` | path | True | string |

**Request body (`application/json`):**

```
- Create connector version request
- `version`: string
- `release_notes`: any
- `breaking_changes`: array<string>
- `new_features`: array<string>
- `bug_fixes`: array<string>
- `docker_image`: any
- `docker_tag`: any
- `is_stable`: boolean
- `released_at`: string
```

**Responses:**

- `201` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/connector-definitions/{connector_id}/versions/{version_id}`

**Summary:** Get Connector Version

**Operation ID:** `get_connector_version_api_v1_connector_definitions__connector_id__versions__version_id__get`

Get specific connector version details

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connector_id` | path | True | string |
| `version_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `PATCH /api/v1/connector-definitions/{connector_id}/versions/{version_id}`

**Summary:** Update Connector Version

**Operation ID:** `update_connector_version_api_v1_connector_definitions__connector_id__versions__version_id__patch`

Update connector version.

Requires: connector_definitions:update permission (superadmin only)

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connector_id` | path | True | string |
| `version_id` | path | True | string |

**Request body (`application/json`):**

```
- Update connector version request
- `release_notes`: any
- `is_stable`: any
- `deprecated_at`: any
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/data-quality/metrics/connection/{connection_id}`

**Summary:** Get Connection Quality Metrics

**Operation ID:** `get_connection_quality_metrics_api_v1_data_quality_metrics_connection__connection_id__get`

Get quality metrics for a connection or stream

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |
| `stream_id` | query | False | any |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/data-quality/metrics/dashboard`

**Summary:** Get Quality Dashboard

**Operation ID:** `get_quality_dashboard_api_v1_data_quality_metrics_dashboard_get`

Get overall quality dashboard

**Responses:**

- `200` — Successful Response

---

### `POST /api/v1/data-quality/policies`

**Summary:** Create Dq Policy

**Operation ID:** `create_dq_policy_api_v1_data_quality_policies_post`

Create a new data quality policy/rule

**Request body (`application/json`):**

```
- Schema for creating DQ policy
- `policy_name`: string
- `description`: any
- `connection_id`: any
- `stream_id`: any
- `rule_type`: string
- `rule_definition`: object
  - Rule configuration
- `target_columns`: array<string>
- `severity`: string
- `action_on_failure`: string
- `threshold_type`: any
- `threshold_value`: any
- `execution_schedule`: any
- `is_active`: boolean
- `template_id`: any
```

**Responses:**

- `201` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/data-quality/policies`

**Summary:** List Dq Policies

**Operation ID:** `list_dq_policies_api_v1_data_quality_policies_get`

List DQ policies with filtering

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | query | False | any |
| `stream_id` | query | False | any |
| `rule_type` | query | False | any |
| `severity` | query | False | any |
| `is_active` | query | False | any |
| `search` | query | False | any |
| `page` | query | False | integer |
| `page_size` | query | False | integer |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/data-quality/policies/test`

**Summary:** Test Rule

**Operation ID:** `test_rule_api_v1_data_quality_policies_test_post`

Test a rule before saving

**Request body (`application/json`):**

```
- Request to test a rule before saving
- `rule_type`: string
- `rule_definition`: object
- `connection_id`: string
- `stream_id`: any
- `target_columns`: array<string>
- `sample_size`: integer
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/data-quality/policies/{policy_id}`

**Summary:** Get Dq Policy

**Operation ID:** `get_dq_policy_api_v1_data_quality_policies__policy_id__get`

Get DQ policy details

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `policy_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `PATCH /api/v1/data-quality/policies/{policy_id}`

**Summary:** Update Dq Policy

**Operation ID:** `update_dq_policy_api_v1_data_quality_policies__policy_id__patch`

Update DQ policy

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `policy_id` | path | True | string |

**Request body (`application/json`):**

```
- Schema for updating DQ policy
- `policy_name`: any
- `description`: any
- `connection_id`: any
- `stream_id`: any
- `rule_type`: any
- `rule_definition`: any
- `target_columns`: any
- `severity`: any
- `action_on_failure`: any
- `threshold_type`: any
- `threshold_value`: any
- `execution_schedule`: any
- `is_active`: any
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `DELETE /api/v1/data-quality/policies/{policy_id}`

**Summary:** Delete Dq Policy

**Operation ID:** `delete_dq_policy_api_v1_data_quality_policies__policy_id__delete`

Delete DQ policy (soft delete)

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `policy_id` | path | True | string |
| `force` | query | False | boolean |

**Responses:**

- `204` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/data-quality/policies/{policy_id}/execute`

**Summary:** Execute Policy

**Operation ID:** `execute_policy_api_v1_data_quality_policies__policy_id__execute_post`

Execute a specific DQ policy

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `policy_id` | path | True | string |

**Request body (`application/json`):**

```
- Request to execute a specific rule
- `policy_id`: string
- `force_execution`: boolean
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/data-quality/policies/{policy_id}/results`

**Summary:** List Policy Results

**Operation ID:** `list_policy_results_api_v1_data_quality_policies__policy_id__results_get`

List execution results for a policy

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `policy_id` | path | True | string |
| `page` | query | False | integer |
| `page_size` | query | False | integer |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/data-quality/profiling/profile`

**Summary:** Profile Data

**Operation ID:** `profile_data_api_v1_data_quality_profiling_profile_post`

Profile data from a connection/stream

**Request body (`application/json`):**

```
- Request to profile data
- `connection_id`: string
- `stream_id`: any
- `columns`: any
- `sample_size`: integer
- `include_distributions`: boolean
- `include_patterns`: boolean
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/data-quality/scores/by-connection`

**Summary:** Get Scores By Connection

**Operation ID:** `get_scores_by_connection_api_v1_data_quality_scores_by_connection_get`

Return a per-connection quality score summary for the Data Quality dashboard.

**Responses:**

- `200` — Successful Response

---

### `POST /api/v1/data-quality/templates`

**Summary:** Create Rule Template

**Operation ID:** `create_rule_template_api_v1_data_quality_templates_post`

Create a new rule template

**Request body (`application/json`):**

```
- Schema for creating rule template
- `template_name`: string
- `template_type`: string
- `description`: any
- `rule_definition_schema`: object
  - JSON schema for rule configuration
- `default_severity`: string
- `default_action`: string
- `category`: string
- `is_active`: boolean
```

**Responses:**

- `201` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/data-quality/templates`

**Summary:** List Rule Templates

**Operation ID:** `list_rule_templates_api_v1_data_quality_templates_get`

List available rule templates

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `page` | query | False | integer |
| `page_size` | query | False | integer |
| `category` | query | False | any |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/data-quality/violations`

**Summary:** List Violations

**Operation ID:** `list_violations_api_v1_data_quality_violations_get`

List DQ violations with filtering

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | query | False | any |
| `policy_id` | query | False | any |
| `status` | query | False | any |
| `severity` | query | False | any |
| `page` | query | False | integer |
| `page_size` | query | False | integer |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/data-quality/violations/{violation_id}`

**Summary:** Get Violation

**Operation ID:** `get_violation_api_v1_data_quality_violations__violation_id__get`

Get violation details with samples

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `violation_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/data-quality/violations/{violation_id}/resolve`

**Summary:** Resolve Violation

**Operation ID:** `resolve_violation_api_v1_data_quality_violations__violation_id__resolve_post`

Resolve or ignore a violation

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `violation_id` | path | True | string |

**Request body (`application/json`):**

```
- Request to resolve a violation
- `status`: string
- `resolution_notes`: any
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/destinations`

**Summary:** Create Destination

**Operation ID:** `create_destination_api_v1_destinations_post`

Create a new destination configuration

Requires: destinations:create permission

**Request body (`application/json`):**

```
- Schema for creating a new destination
- `destination_name`: string
- `connector_definition_id`: string
- `host`: any
- `port`: any
- `database_name`: any
- `schema_name`: any
- `username`: any
- `password`: any
- `bucket_name`: any
- `region`: any
- `path_prefix`: any
- `ssl_enabled`: boolean
- `ssl_config`: object
  - SSL/TLS config: ssl_mode, ssl_ca, ssl_cert, ssl_key
- `ssh_config`: any
- `output_format`: any
- `compression`: any
- `config`: object
  - Additional connector-specific config
- `connector_version`: string
- `status`: any
```

**Responses:**

- `201` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/destinations`

**Summary:** List Destinations

**Operation ID:** `list_destinations_api_v1_destinations_get`

List all destinations for the current tenant

Supports filtering by:
- status: draft, active, inactive
- connector_type: MySQL Destination, PostgreSQL Destination, etc.
- connector_definition_id: specific connector
- search: search in destination name

Results are paginated.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `status` | query | False | any |
| `connector_type` | query | False | any |
| `connector_definition_id` | query | False | any |
| `search` | query | False | any |
| `page` | query | False | integer |
| `page_size` | query | False | integer |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/destinations/test-tunnel`

**Summary:** Test Destination Tunnel Adhoc

**Operation ID:** `test_destination_tunnel_adhoc_api_v1_destinations_test_tunnel_post`

Ad-hoc SSH tunnel test without a saved destination.
Send: {"ssh_config": {tunnel_host, tunnel_port, tunnel_username, tunnel_auth_method, ...}}

**Request body (`application/json`):**

```

```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/destinations/{destination_id}`

**Summary:** Get Destination

**Operation ID:** `get_destination_api_v1_destinations__destination_id__get`

Get destination details by ID

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `destination_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `PATCH /api/v1/destinations/{destination_id}`

**Summary:** Update Destination

**Operation ID:** `update_destination_api_v1_destinations__destination_id__patch`

Update destination configuration

Requires: destinations:update permission

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `destination_id` | path | True | string |

**Request body (`application/json`):**

```
- Schema for updating a destination
- `destination_name`: any
- `host`: any
- `port`: any
- `database_name`: any
- `schema_name`: any
- `username`: any
- `password`: any
- `bucket_name`: any
- `region`: any
- `path_prefix`: any
- `ssl_enabled`: any
- `ssl_config`: any
- `ssh_config`: any
- `output_format`: any
- `compression`: any
- `config`: any
- `status`: any
- `connector_version`: any
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `DELETE /api/v1/destinations/{destination_id}`

**Summary:** Delete Destination

**Operation ID:** `delete_destination_api_v1_destinations__destination_id__delete`

Delete destination (soft delete)

Requires: destinations:delete permission

Will fail if there are active connections using this destination.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `destination_id` | path | True | string |

**Responses:**

- `204` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/destinations/{destination_id}/batch-settings`

**Summary:** Configure Batch Settings

**Operation ID:** `configure_batch_settings_api_v1_destinations__destination_id__batch_settings_post`

Configure batch write settings

Requires: destinations:update permission

Controls how data is batched and written to the destination.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `destination_id` | path | True | string |

**Request body (`application/json`):**

```
- Schema for configuring batch write settings
- `batch_size`: integer
- `batch_timeout_seconds`: integer
- `max_parallel_batches`: integer
- `max_retries`: integer
- `retry_delay_seconds`: integer
- `continue_on_error`: boolean
- `error_threshold_percent`: number
- `enable_compression`: boolean
- `buffer_memory_mb`: integer
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/destinations/{destination_id}/batch-settings`

**Summary:** Get Batch Settings

**Operation ID:** `get_batch_settings_api_v1_destinations__destination_id__batch_settings_get`

Get current batch settings

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `destination_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/destinations/{destination_id}/schema-mapping`

**Summary:** Configure Schema Mapping

**Operation ID:** `configure_schema_mapping_api_v1_destinations__destination_id__schema_mapping_post`

Configure schema mapping for destination table

Requires: destinations:update permission

Maps source columns to destination columns with optional transformations.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `destination_id` | path | True | string |

**Request body (`application/json`):**

```
- Schema for configuring destination schema mapping
- `table_name`: string
- `column_mappings`: array<object>
- `enable_auto_mapping`: boolean
- `case_sensitive`: boolean
- `drop_unmapped_columns`: boolean
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/destinations/{destination_id}/schema-mapping/{table_name}`

**Summary:** Get Schema Mapping

**Operation ID:** `get_schema_mapping_api_v1_destinations__destination_id__schema_mapping__table_name__get`

Get schema mapping for specific table

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `destination_id` | path | True | string |
| `table_name` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/destinations/{destination_id}/stats`

**Summary:** Get Destination Stats

**Operation ID:** `get_destination_stats_api_v1_destinations__destination_id__stats_get`

Get destination usage statistics

Returns connection count, sync statistics, and performance metrics.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `destination_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/destinations/{destination_id}/test-connection`

**Summary:** Test Destination Connection

**Operation ID:** `test_destination_connection_api_v1_destinations__destination_id__test_connection_post`

Test destination connectivity

Can provide override parameters for testing without saving them.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `destination_id` | path | True | string |

**Request body (`application/json`):**

```

```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/destinations/{destination_id}/test-tunnel`

**Summary:** Test Destination Tunnel

**Operation ID:** `test_destination_tunnel_api_v1_destinations__destination_id__test_tunnel_post`

Test only the SSH tunnel for a saved destination.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `destination_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/destinations/{destination_id}/validate-write-permissions`

**Summary:** Validate Write Permissions

**Operation ID:** `validate_write_permissions_api_v1_destinations__destination_id__validate_write_permissions_post`

Validate destination has write permissions

Checks if the destination credentials can:
- Create tables (if required)
- Insert data
- Update data (for upsert mode)

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `destination_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/destinations/{destination_id}/write-mode`

**Summary:** Configure Write Mode

**Operation ID:** `configure_write_mode_api_v1_destinations__destination_id__write_mode_post`

Configure write mode for destination

Requires: destinations:update permission

Write modes:
- append: Add new records to existing data
- replace: Drop/truncate and reload all data
- upsert: Insert new records, update existing based on primary keys
- merge: Complex merge logic with custom rules

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `destination_id` | path | True | string |

**Request body (`application/json`):**

```
- Schema for configuring write behavior
- `write_mode`: string
- `primary_keys`: any
- `update_columns`: any
- `partition_by`: any
- `partition_type`: any
- `create_table_if_not_exists`: boolean
- `drop_table_before_load`: boolean
- `truncate_before_load`: boolean
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/destinations/{destination_id}/write-mode`

**Summary:** Get Write Mode Config

**Operation ID:** `get_write_mode_config_api_v1_destinations__destination_id__write_mode_get`

Get current write mode configuration

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `destination_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/dlq/connections`

**Summary:** Get Connection Summaries

**Operation ID:** `get_connection_summaries_api_v1_dlq_connections_get`

One row per connection that has DLQ events.

**Responses:**

- `200` — Successful Response

---

### `POST /api/v1/dlq/purge-expired`

**Summary:** Purge Expired Global

**Operation ID:** `purge_expired_global_api_v1_dlq_purge_expired_post`

Hard-delete all expired (TTL exceeded or max retries reached) events.

**Responses:**

- `200` — Successful Response

---

### `POST /api/v1/dlq/retry-all`

**Summary:** Retry All Global

**Operation ID:** `retry_all_global_api_v1_dlq_retry_all_post`

Retry ALL pending DLQ events across all connections.

**Responses:**

- `200` — Successful Response

---

### `GET /api/v1/dlq/stats`

**Summary:** Get Stats

**Operation ID:** `get_stats_api_v1_dlq_stats_get`

Global DLQ statistics.

**Responses:**

- `200` — Successful Response

---

### `POST /api/v1/dlq/{connection_id}/delete`

**Summary:** Delete Selected

**Operation ID:** `delete_selected_api_v1_dlq__connection_id__delete_post`

Hard-delete specific event IDs.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |

**Request body (`application/json`):**

```
- `event_ids`: array<string>
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/dlq/{connection_id}/events`

**Summary:** List Events

**Operation ID:** `list_events_api_v1_dlq__connection_id__events_get`

Paginated list of DLQ events for a connection.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |
| `page` | query | False | integer |
| `page_size` | query | False | integer |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `DELETE /api/v1/dlq/{connection_id}/purge`

**Summary:** Purge Connection

**Operation ID:** `purge_connection_api_v1_dlq__connection_id__purge_delete`

Delete ALL DLQ events (resolved or not) for a connection.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |

**Responses:**

- `204` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/dlq/{connection_id}/retry`

**Summary:** Retry Selected

**Operation ID:** `retry_selected_api_v1_dlq__connection_id__retry_post`

Retry a specific list of event IDs.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |

**Request body (`application/json`):**

```
- `event_ids`: array<string>
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/dlq/{connection_id}/retry-all`

**Summary:** Retry All For Connection

**Operation ID:** `retry_all_for_connection_api_v1_dlq__connection_id__retry_all_post`

Retry all pending events for a specific connection.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/dlq/{connection_id}/{event_id}`

**Summary:** Get Event Detail

**Operation ID:** `get_event_detail_api_v1_dlq__connection_id___event_id__get`

Single DLQ event with full payload and retry history.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |
| `event_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `DELETE /api/v1/dlq/{connection_id}/{event_id}`

**Summary:** Delete Event

**Operation ID:** `delete_event_api_v1_dlq__connection_id___event_id__delete`

Hard-delete a single DLQ event.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |
| `event_id` | path | True | string |

**Responses:**

- `204` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/dlq/{connection_id}/{event_id}/retry`

**Summary:** Retry Single

**Operation ID:** `retry_single_api_v1_dlq__connection_id___event_id__retry_post`

Retry a single DLQ event, optionally with an edited payload.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |
| `event_id` | path | True | string |

**Request body (`application/json`):**

```

```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/graphql`

**Summary:** Graphql Rest Endpoint

**Operation ID:** `graphql_rest_endpoint_api_v1_graphql_post`

Execute a GraphQL query via REST (standard JSON body).

**Request body (`application/json`):**

```
- `query`: string
- `variables`: any
- `operationName`: any
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/internal/checkpoints/batch`

**Summary:** Batch-upsert checkpoint state from worker

**Operation ID:** `upsert_checkpoints_api_v1_internal_checkpoints_batch_post`

Accept a batch of checkpoint records from a CDC worker and upsert them
into the `checkpoint_state` table.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `x-worker-token` | header | True | string |

**Request body (`application/json`):**

```
- `worker_id`: string
- `source_id`: string
- `checkpoints`: array<object>
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/internal/checkpoints/{source_id}`

**Summary:** Get latest checkpoints for a source

**Operation ID:** `get_checkpoints_api_v1_internal_checkpoints__source_id__get`

Return a dict of {"{schema_name}.{table_name}": lsn} for the given source_id.
Workers call this on startup to resume from the last committed position.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `source_id` | path | True | string |
| `x-worker-token` | header | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/internal/connections/{connection_id}/run-complete`

**Summary:** Airflow DAG reports that a batch CDC job has completed

**Operation ID:** `run_complete_api_v1_internal_connections__connection_id__run_complete_post`

Called by the Airflow `notify_run_complete` task after the Spark batch job finishes.
Updates the Connection status and records a run-history entry.

Spec §5 (P5-7): 'After Spark job completes, call back to control plane to update
connection run status and last-successful-sync timestamp.'

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |
| `x-worker-token` | header | True | string |

**Request body (`application/json`):**

```
- `connection_id`: string
- `status`: string
- `rows_synced`: integer
- `dag_run_id`: any
- `error_message`: any
```

**Responses:**

- `202` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/internal/dq-violations`

**Summary:** Spark consumer reports DQ violations for audit logging

**Operation ID:** `ingest_dq_violations_api_v1_internal_dq_violations_post`

Persists DQ violations to audit_logs so they are queryable alongside other audit events.
Spec §5 (P5-5): 'DQ violations must be written to the audit log, not only emitted to Prometheus.'

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `x-worker-token` | header | True | string |

**Request body (`application/json`):**

```
- `tenant`: string
- `connection_id`: string
- `violations`: array<object>
- `recorded_at`: any
```

**Responses:**

- `202` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/internal/report-ddl-change`

**Summary:** Worker reports a DDL change detected in the database log

**Operation ID:** `report_ddl_change_api_v1_internal_report_ddl_change_post`

Called by CDC workers (MySQL/Postgres) when a DDL event is detected
in the binary log or WAL.

Creates a pending SchemaChangeEvent, which operators can approve/reject.
For connections with AUTO_APPLY policy the normal report endpoint handles
actual application; this endpoint is the low-level ingress from workers.

Spec §1 (MySQL): 'Handles DDL events...by invalidating the schema cache
and sending schema change notifications to the control plane.'
Spec §1 (Postgres): 'Detects DDL messages...and notifies the control plane
to trigger schema re-introspection.'

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `x-worker-token` | header | True | string |

**Request body (`application/json`):**

```
- `source_id`: string
- `schema_name`: string
- `table_name`: string
- `ddl_query`: string
- `change_type`: string
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/internal/resync-request`

**Summary:** Worker signals that a collection needs a full resync (e.g. ChangeStreamHistoryLost)

**Operation ID:** `request_resync_api_v1_internal_resync_request_post`

Called by connectors (e.g. MongoDB) when the change stream history is lost
and a full resync is required.

Records a SchemaChangeEvent of type 'resync_required' in MANUAL_APPROVAL
status so an operator can review and trigger the resync.
Spec §1 (MongoDB): 'Handles errors like ChangeStreamHistoryLost by emitting an alert
and requiring a resync'.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `x-worker-token` | header | True | string |

**Request body (`application/json`):**

```
- `source_id`: string
- `schema_name`: string
- `table_name`: string
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/internal/workers/heartbeat`

**Summary:** Receive worker heartbeat

**Operation ID:** `worker_heartbeat_api_v1_internal_workers_heartbeat_post`

Upsert a worker heartbeat record.  Workers call this every HEARTBEAT_INTERVAL
seconds to signal liveness and report metrics.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `x-worker-token` | header | True | string |

**Request body (`application/json`):**

```
- `worker_id`: string
- `worker_type`: any
- `status`: string
- `ts_ms`: any
- `events_processed`: integer
- `errors_count`: integer
- `last_error`: any
- `cpu_usage_percent`: any
- `memory_usage_mb`: any
- `hostname`: any
- `pod_name`: any
- `worker_metadata`: object
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/internal/workers/{worker_id}/routing`

**Summary:** Get routing table for a worker

**Operation ID:** `get_worker_routing_api_v1_internal_workers__worker_id__routing_get`

Return the routing table: list of (schema, table, bank_id, tenant_id, source_id)
tuples that tell the worker which Redis stream key to use for each DB table.

Currently returns one entry per active source with schema wildcard ("*").
A future enhancement would store per-table routing in the DB.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `worker_id` | path | True | string |
| `x-worker-token` | header | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/internal/workers/{worker_id}/sources`

**Summary:** Get sources assigned to a worker

**Operation ID:** `get_worker_sources_api_v1_internal_workers__worker_id__sources_get`

Return all active sources whose config field contains
{"assigned_worker_id": worker_id}.

If no sources are assigned to this worker, returns all active sources
(single-worker / dev mode).

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `worker_id` | path | True | string |
| `x-worker-token` | header | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/monitoring/connections/{connection_id}/checkpoints`

**Summary:** List Checkpoints

**Operation ID:** `list_checkpoints_api_v1_monitoring_connections__connection_id__checkpoints_get`

List checkpoint states for a connection's source.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/monitoring/connections/{connection_id}/health`

**Summary:** Connection Health

**Operation ID:** `connection_health_api_v1_monitoring_connections__connection_id__health_get`

Health summary for a specific connection — latest heartbeat + lag.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/monitoring/connections/{connection_id}/lag`

**Summary:** Connection Lag

**Operation ID:** `connection_lag_api_v1_monitoring_connections__connection_id__lag_get`

Latest CDC lag metrics for a connection.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/monitoring/connections/{connection_id}/throughput`

**Summary:** Connection Throughput

**Operation ID:** `connection_throughput_api_v1_monitoring_connections__connection_id__throughput_get`

Event throughput for a connection (latest sample).

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/monitoring/health`

**Summary:** System Health

**Operation ID:** `system_health_api_v1_monitoring_health_get`

System health check — pings DB and Redis.

**Responses:**

- `200` — Successful Response

---

### `GET /api/v1/monitoring/logs/{pod_name}`

**Summary:** Get Pod Logs

**Operation ID:** `get_pod_logs_api_v1_monitoring_logs__pod_name__get`

Fetch the last N log lines from a pod in the fusion namespace.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `pod_name` | path | True | string |
| `lines` | query | False | integer |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/monitoring/pods`

**Summary:** List Pods

**Operation ID:** `list_pods_api_v1_monitoring_pods_get`

List all pods in the fusion namespace via the k8s API.

**Responses:**

- `200` — Successful Response

---

### `GET /api/v1/monitoring/resource-usage`

**Summary:** Resource Usage

**Operation ID:** `resource_usage_api_v1_monitoring_resource_usage_get`

Aggregate resource usage across workers.

**Responses:**

- `200` — Successful Response

---

### `GET /api/v1/monitoring/workers`

**Summary:** List Workers

**Operation ID:** `list_workers_api_v1_monitoring_workers_get`

List all active worker heartbeats (tenant-scoped via connection_id FK not yet enforced).

**Responses:**

- `200` — Successful Response

---

### `GET /api/v1/schema-evolution/changes`

**Summary:** List All Schema Events

**Operation ID:** `list_all_schema_events_api_v1_schema_evolution_changes_get`

List all schema-change events across every connection (for the Schema Evolution dashboard).

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `status` | query | False | any |
| `page` | query | False | integer |
| `page_size` | query | False | integer |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/schema-evolution/connections/{connection_id}/json-flatten-rules`

**Summary:** Create Json Flatten Rule

**Operation ID:** `create_json_flatten_rule_api_v1_schema_evolution_connections__connection_id__json_flatten_rules_post`

Create a JSON flattening rule for a stream.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |

**Request body (`application/json`):**

```
- `source_column`: string
- `flatten_strategy`: string
- `target_columns`: array<any>
- `json_path_expressions`: object
- `column_prefix`: any
- `separator`: string
```

**Responses:**

- `201` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/schema-evolution/connections/{connection_id}/json-flatten-rules`

**Summary:** List Json Flatten Rules

**Operation ID:** `list_json_flatten_rules_api_v1_schema_evolution_connections__connection_id__json_flatten_rules_get`

List JSON flattening rules for a connection.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/schema-evolution/connections/{connection_id}/json-schemas`

**Summary:** List Json Schemas

**Operation ID:** `list_json_schemas_api_v1_schema_evolution_connections__connection_id__json_schemas_get`

List cached JSON schemas for a connection's source.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/schema-evolution/connections/{connection_id}/schema-changes`

**Summary:** List Schema Changes

**Operation ID:** `list_schema_changes_api_v1_schema_evolution_connections__connection_id__schema_changes_get`

List detected schema changes for a connection, optionally filtered by status.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |
| `status` | query | False | any |
| `page` | query | False | integer |
| `page_size` | query | False | integer |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/schema-evolution/connections/{connection_id}/schema-changes`

**Summary:** Report Schema Change

**Operation ID:** `report_schema_change_api_v1_schema_evolution_connections__connection_id__schema_changes_post`

Record a newly detected schema change event (spec §3 §3.1).

Called by:
- CDC workers when they detect column/table changes during CDC streaming.
- Introspection background jobs after periodic re-introspection.

AUTO_APPLY logic:
  If the parent connection's schema_evolution_policy is AUTO_APPLY, the event
  is immediately auto-approved and the Spark consumer is notified to reload.
  For MANUAL_APPROVAL, the event stays in 'pending' status for human review.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |

**Request body (`application/json`):**

```
- Payload sent by CDC workers or introspection jobs when a schema change is detected.
- `table_name`: string
- `schema_name`: any
- `change_type`: string
- `old_schema`: any
- `new_schema`: object
- `schema_diff`: object
- `detected_by`: string
- `is_breaking`: boolean
- `stream_id`: any
```

**Responses:**

- `201` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/schema-evolution/connections/{connection_id}/schema-changes/{change_id}`

**Summary:** Get Schema Change

**Operation ID:** `get_schema_change_api_v1_schema_evolution_connections__connection_id__schema_changes__change_id__get`

Get a single schema change event.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |
| `change_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/schema-evolution/connections/{connection_id}/schema-changes/{change_id}/approve`

**Summary:** Approve Schema Change

**Operation ID:** `approve_schema_change_api_v1_schema_evolution_connections__connection_id__schema_changes__change_id__approve_post`

Approve a pending schema change and notify the Spark consumer to reload its schema.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |
| `change_id` | path | True | string |

**Request body (`application/json`):**

```

```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/schema-evolution/connections/{connection_id}/schema-changes/{change_id}/reject`

**Summary:** Reject Schema Change

**Operation ID:** `reject_schema_change_api_v1_schema_evolution_connections__connection_id__schema_changes__change_id__reject_post`

Reject a pending schema change.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |
| `change_id` | path | True | string |

**Request body (`application/json`):**

```

```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/schema-evolution/events`

**Summary:** List All Schema Events

**Operation ID:** `list_all_schema_events_api_v1_schema_evolution_events_get`

List all schema-change events across every connection (for the Schema Evolution dashboard).

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `status` | query | False | any |
| `page` | query | False | integer |
| `page_size` | query | False | integer |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/schema-evolution/events/{change_id}/approve`

**Summary:** Approve Schema Event

**Operation ID:** `approve_schema_event_api_v1_schema_evolution_events__change_id__approve_post`

Approve a schema-change event by its ID (without knowing the connection).

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `change_id` | path | True | string |

**Request body (`application/json`):**

```

```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/schema-evolution/events/{change_id}/reject`

**Summary:** Reject Schema Event

**Operation ID:** `reject_schema_event_api_v1_schema_evolution_events__change_id__reject_post`

Reject a schema-change event by its ID (without knowing the connection).

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `change_id` | path | True | string |

**Request body (`application/json`):**

```

```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/settings/spark-config`

**Summary:** Get Spark Config

**Operation ID:** `get_spark_config_api_v1_settings_spark_config_get`

Return structured Spark configuration from system_config table.

**Responses:**

- `200` — Successful Response

---

### `PUT /api/v1/settings/spark-config`

**Summary:** Update Spark Config

**Operation ID:** `update_spark_config_api_v1_settings_spark_config_put`

Bulk-upsert all Spark config keys into system_config table.

**Request body (`application/json`):**

```
- `master`: string
- `deploy_mode`: string
- `namespace`: string
- `image_pull_policy`: string
- `driver_cores`: string
- `driver_memory`: string
- `executor_cores`: string
- `executor_memory`: string
- `executor_instances`: string
- `dynamic_allocation_enabled`: boolean
- `dynamic_allocation_min`: string
- `dynamic_allocation_max`: string
- `checkpoint_dir`: string
- `extra_conf`: object
- `service_account`: string
- `image_registry`: string
- `image_tag`: string
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/settings/system-config`

**Summary:** List System Config

**Operation ID:** `list_system_config_api_v1_settings_system_config_get`

Return all system config entries. Sensitive values are masked.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `category` | query | False | any |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/settings/system-config`

**Summary:** Create System Config

**Operation ID:** `create_system_config_api_v1_settings_system_config_post`

**Request body (`application/json`):**

```
- `key`: string
- `value`: string
- `value_type`: string
- `description`: any
- `category`: any
- `is_sensitive`: boolean
```

**Responses:**

- `201` — Successful Response
- `422` — Validation Error

---

### `PUT /api/v1/settings/system-config/{key}`

**Summary:** Update System Config

**Operation ID:** `update_system_config_api_v1_settings_system_config__key__put`

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `key` | path | True | string |

**Request body (`application/json`):**

```
- `value`: string
- `description`: any
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `DELETE /api/v1/settings/system-config/{key}`

**Summary:** Delete System Config

**Operation ID:** `delete_system_config_api_v1_settings_system_config__key__delete`

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `key` | path | True | string |

**Responses:**

- `204` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/sources`

**Summary:** Create Source

**Operation ID:** `create_source_api_v1_sources_post`

Create a new source connection

Requires: sources:create permission

**Request body (`application/json`):**

```
- Schema for creating a new source
- `source_name`: string
- `connector_definition_id`: string
- `connector_version`: string
- `host`: string
- `port`: integer
- `database_name`: string
- `username`: any
- `password`: any
- `ssl_enabled`: boolean
- `ssl_config`: any
- `ssh_config`: any
- `config`: any
```

**Responses:**

- `201` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/sources`

**Summary:** List Sources

**Operation ID:** `list_sources_api_v1_sources_get`

List sources with filtering and pagination

Automatically filtered by user's tenant

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `status` | query | False | any |
| `connector_type` | query | False | any |
| `connector_definition_id` | query | False | any |
| `search` | query | False | any |
| `page` | query | False | integer |
| `page_size` | query | False | integer |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/sources/test-tunnel`

**Summary:** Test Tunnel Adhoc

**Operation ID:** `test_tunnel_adhoc_api_v1_sources_test_tunnel_post`

Test an SSH tunnel connection without needing a saved source.
Accepts a JSON body with a ssh_config object (tunnel_host, tunnel_port,
tunnel_username, tunnel_auth_method, tunnel_password / tunnel_private_key,
tunnel_passphrase).  Also accepts the old format where ssl_config carried
the tunnel fields.

**Request body (`application/json`):**

```

```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/sources/{source_id}`

**Summary:** Get Source

**Operation ID:** `get_source_api_v1_sources__source_id__get`

Get source details by ID

Automatically filtered by user's tenant

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `source_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `PATCH /api/v1/sources/{source_id}`

**Summary:** Update Source

**Operation ID:** `update_source_api_v1_sources__source_id__patch`

Update source configuration

Requires: sources:update permission

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `source_id` | path | True | string |

**Request body (`application/json`):**

```
- Schema for updating an existing source
- `source_name`: any
- `connector_version`: any
- `host`: any
- `port`: any
- `database_name`: any
- `username`: any
- `password`: any
- `ssl_enabled`: any
- `ssl_config`: any
- `ssh_config`: any
- `config`: any
- `status`: any
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `DELETE /api/v1/sources/{source_id}`

**Summary:** Delete Source

**Operation ID:** `delete_source_api_v1_sources__source_id__delete`

Soft delete source

Requires: sources:delete permission

Cannot delete source if it has active connections

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `source_id` | path | True | string |

**Responses:**

- `204` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/sources/{source_id}/cdc-config`

**Summary:** Configure Cdc

**Operation ID:** `configure_cdc_api_v1_sources__source_id__cdc_config_post`

Configure CDC (Change Data Capture) for a source

Requires: sources:update permission

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `source_id` | path | True | string |

**Request body (`application/json`):**

```
- Schema for CDC configuration request
- `enable_cdc`: boolean
- `replication_method`: string
- `replication_config`: object
  - Replication-specific config (slot_name, server_id, etc.)
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/sources/{source_id}/cdc-config`

**Summary:** Get Cdc Config

**Operation ID:** `get_cdc_config_api_v1_sources__source_id__cdc_config_get`

Get current CDC configuration for a source

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `source_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/sources/{source_id}/discover`

**Summary:** Discover Source Schemas

**Operation ID:** `discover_source_schemas_api_v1_sources__source_id__discover_post`

Discover available schemas and tables from source database

Results are cached in the source for faster subsequent queries

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `source_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/sources/{source_id}/discover-schemas`

**Summary:** Discover Source Schemas

**Operation ID:** `discover_source_schemas_api_v1_sources__source_id__discover_schemas_post`

Discover available schemas and tables from source database

Results are cached in the source for faster subsequent queries

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `source_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/sources/{source_id}/schemas`

**Summary:** Get Source Schemas

**Operation ID:** `get_source_schemas_api_v1_sources__source_id__schemas_get`

Get cached discovered schemas/tables for a source.
Returns a flat list of tables for use in connection wizard.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `source_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/sources/{source_id}/stats`

**Summary:** Get Source Stats

**Operation ID:** `get_source_stats_api_v1_sources__source_id__stats_get`

Get usage statistics for a source

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `source_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/sources/{source_id}/table-schema`

**Summary:** Get Table Schema

**Operation ID:** `get_table_schema_api_v1_sources__source_id__table_schema_post`

Get detailed schema for a specific table including columns and indexes

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `source_id` | path | True | string |

**Request body (`application/json`):**

```
- Schema for requesting table schema details
- `schema_name`: string
- `table_name`: string
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/sources/{source_id}/test-connection`

**Summary:** Test Source Connection

**Operation ID:** `test_source_connection_api_v1_sources__source_id__test_connection_post`

Test source database connectivity, routing through SSH tunnel if configured.

Can optionally override connection parameters for testing without saving

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `source_id` | path | True | string |

**Request body (`application/json`):**

```

```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/sources/{source_id}/test-tunnel`

**Summary:** Test Source Tunnel

**Operation ID:** `test_source_tunnel_api_v1_sources__source_id__test_tunnel_post`

Test the SSH tunnel configured on an existing source.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `source_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/streams/connections/{connection_id}/streams`

**Summary:** List all streams for a connection

**Operation ID:** `list_streams_api_v1_streams_connections__connection_id__streams_get`

Return all table-level stream configs for a connection.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `PUT /api/v1/streams/connections/{connection_id}/streams/{stream_id}`

**Summary:** Update stream configuration (cursor, PK, transforms, sync mode)

**Operation ID:** `update_stream_api_v1_streams_connections__connection_id__streams__stream_id__put`

Update sync mode, cursor field, primary key overrides, or transform overrides.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |
| `stream_id` | path | True | string |

**Request body (`application/json`):**

```
- `sync_mode`: any
- `cursor_field`: any
- `primary_keys`: any
- `column_mapping`: any
- `selected_columns`: any
- `transform_steps`: any
- `is_enabled`: any
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/streams/connections/{connection_id}/streams/{stream_id}/disable`

**Summary:** Disable a stream (pause CDC for this table)

**Operation ID:** `disable_stream_api_v1_streams_connections__connection_id__streams__stream_id__disable_post`

Mark the stream as disabled so the CDC worker skips this table.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |
| `stream_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/streams/connections/{connection_id}/streams/{stream_id}/enable`

**Summary:** Enable a stream (resume CDC for this table)

**Operation ID:** `enable_stream_api_v1_streams_connections__connection_id__streams__stream_id__enable_post`

Mark the stream as enabled so the CDC worker starts (or resumes) capturing events.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `connection_id` | path | True | string |
| `stream_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/transformations`

**Summary:** Create Transformation

**Operation ID:** `create_transformation_api_v1_transformations_post`

Create a new transformation pipeline (version 1).

**Request body (`application/json`):**

```
- Schema for creating a new transformation pipeline
- `pipeline_name`: string
- `description`: any
- `pipeline_type`: string
- `transformation_code`: string
- `language`: string
- `input_streams`: array<string>
- `output_stream`: string
- `execution_mode`: string
- `spark_config`: object
```

**Responses:**

- `201` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/transformations`

**Summary:** List Transformations

**Operation ID:** `list_transformations_api_v1_transformations_get`

List transformation pipelines for the current tenant with optional filters.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `page` | query | False | integer |
| `page_size` | query | False | integer |
| `pipeline_type` | query | False | any |
| `language` | query | False | any |
| `is_active` | query | False | any |
| `search` | query | False | any |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/transformations/{pipeline_id}`

**Summary:** Get Transformation

**Operation ID:** `get_transformation_api_v1_transformations__pipeline_id__get`

Get a single transformation pipeline by ID.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `pipeline_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `PUT /api/v1/transformations/{pipeline_id}`

**Summary:** Update Transformation

**Operation ID:** `update_transformation_api_v1_transformations__pipeline_id__put`

Update transformation pipeline fields. Increments version on code change.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `pipeline_id` | path | True | string |

**Request body (`application/json`):**

```
- Schema for updating an existing transformation pipeline
- `pipeline_name`: any
- `description`: any
- `pipeline_type`: any
- `transformation_code`: any
- `language`: any
- `input_streams`: any
- `output_stream`: any
- `execution_mode`: any
- `spark_config`: any
- `is_published`: any
- `is_active`: any
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `DELETE /api/v1/transformations/{pipeline_id}`

**Summary:** Delete Transformation

**Operation ID:** `delete_transformation_api_v1_transformations__pipeline_id__delete`

Soft-delete a transformation pipeline.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `pipeline_id` | path | True | string |

**Responses:**

- `204` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/transformations/{pipeline_id}/preview`

**Summary:** Preview Transformation

**Operation ID:** `preview_transformation_api_v1_transformations__pipeline_id__preview_post`

Preview transformation output on sample rows.

Applies transform steps natively in Python for fast feedback.
Steps requiring Spark (math_op, expression, udf, json_flatten_*)
are noted in the errors list but skipped — full execution requires Spark.

UI should display both transformed_rows and any step-skip notices.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `pipeline_id` | path | True | string |

**Request body (`application/json`):**

```
- Sample rows to apply transform steps to.
- `sample_rows`: array<object>
- `transform_spec`: any
- `connection_id`: any
- `live_sample_count`: integer
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/transformations/{pipeline_id}/validate`

**Summary:** Validate Transformation

**Operation ID:** `validate_transformation_api_v1_transformations__pipeline_id__validate_post`

Validate the transformation code syntax and update validation status.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `pipeline_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `POST /api/v1/udfs`

**Summary:** Register Udf

**Operation ID:** `register_udf_api_v1_udfs_post`

Register a new UDF in the catalog.

**Request body (`application/json`):**

```
- Schema for registering a new UDF
- `udf_name`: string
- `description`: any
- `function_code`: string
- `language`: string
- `return_type`: string
- `parameters`: array<object>
- `category`: any
- `tags`: array<string>
```

**Responses:**

- `201` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/udfs`

**Summary:** List Udfs

**Operation ID:** `list_udfs_api_v1_udfs_get`

List UDFs for the current tenant with optional filters.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `page` | query | False | integer |
| `page_size` | query | False | integer |
| `language` | query | False | any |
| `category` | query | False | any |
| `search` | query | False | any |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /api/v1/udfs/{udf_id}`

**Summary:** Get Udf

**Operation ID:** `get_udf_api_v1_udfs__udf_id__get`

Get a UDF by ID.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `udf_id` | path | True | string |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `PATCH /api/v1/udfs/{udf_id}`

**Summary:** Update Udf

**Operation ID:** `update_udf_api_v1_udfs__udf_id__patch`

Update a UDF. Code changes reset validation status.

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `udf_id` | path | True | string |

**Request body (`application/json`):**

```
- Schema for updating a UDF
- `description`: any
- `function_code`: any
- `language`: any
- `return_type`: any
- `parameters`: any
- `category`: any
- `tags`: any
- `is_active`: any
```

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `DELETE /api/v1/udfs/{udf_id}`

**Summary:** Delete Udf

**Operation ID:** `delete_udf_api_v1_udfs__udf_id__delete`

Deactivate a UDF (soft delete via is_active=False).

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `udf_id` | path | True | string |

**Responses:**

- `204` — Successful Response
- `422` — Validation Error

---

### `GET /graphql`

**Summary:** Handle Http Get

**Operation ID:** `handle_http_get_graphql_get`

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `request` | query | True | any |
| `db` | query | False | any |

**Responses:**

- `200` — The GraphiQL integrated development environment.
- `404` — Not found if GraphiQL or query via GET are not enabled.
- `422` — Validation Error

---

### `POST /graphql`

**Summary:** Handle Http Post

**Operation ID:** `handle_http_post_graphql_post`

**Query / path params:**

| Name | In | Required | Schema |
|------|-----|----------|--------|
| `request` | query | True | any |
| `db` | query | False | any |

**Responses:**

- `200` — Successful Response
- `422` — Validation Error

---

### `GET /health`

**Summary:** Health Check

**Operation ID:** `health_check_health_get`

Basic health check

**Responses:**

- `200` — Successful Response

---

### `GET /health/live`

**Summary:** Liveness Check

**Operation ID:** `liveness_check_health_live_get`

Kubernetes liveness probe

**Responses:**

- `200` — Successful Response

---

### `GET /health/ready`

**Summary:** Readiness Check

**Operation ID:** `readiness_check_health_ready_get`

Kubernetes readiness probe

**Responses:**

- `200` — Successful Response

---

