# fusion-cdc-engine — UI/UX Design Specification

> **Version:** 1.0  
> **Date:** 1 May 2026  
> **Platform:** React + TypeScript (Vite) — targets `localhost:5173`  
> **Design System:** Shadcn/ui + Tailwind CSS  
> **State:** Zustand + TanStack Query  
> **Routing:** React Router v6  

---

## Table of Contents

1. [Design Principles](#1-design-principles)
2. [Information Architecture — Site Map](#2-information-architecture--site-map)
3. [Global Layout & Navigation](#3-global-layout--navigation)
4. [Screen Inventory (Complete)](#4-screen-inventory-complete)
5. [Authentication Screens](#5-authentication-screens)
6. [Dashboard — Home](#6-dashboard--home)
7. [Connectors (Definitions)](#7-connectors-definitions)
8. [Sources](#8-sources)
9. [Destinations](#9-destinations)
10. [Connections](#10-connections)
11. [Streams (per Connection)](#11-streams-per-connection)
12. [Transformations](#12-transformations)
13. [UDFs (User-Defined Functions)](#13-udfs-user-defined-functions)
14. [Data Quality](#14-data-quality)
15. [Alerts & Notifications](#15-alerts--notifications)
16. [Schema Evolution](#16-schema-evolution)
17. [Monitoring & Observability](#17-monitoring--observability)
18. [Settings & Administration](#18-settings--administration)
19. [Dead Letter Queue (DLQ)](#19-dead-letter-queue-dlq)
20. [Spark Jobs & Applications](#20-spark-jobs--applications)
21. [JSON Flatten Rules](#21-json-flatten-rules)
22. [Shared Components Library](#22-shared-components-library)
23. [User Flows (Step-by-Step)](#23-user-flows-step-by-step)
24. [Responsive & Accessibility Requirements](#24-responsive--accessibility-requirements)
25. [API Integration Map](#25-api-integration-map)

---

## 1. Design Principles

| Principle | Application |
|-----------|-------------|
| **Progressive disclosure** | Show summary first; details on drill-down. Complex config behind "Advanced" sections. |
| **Wizard for creation** | Multi-step forms for Sources, Destinations, Connections. Prevents cognitive overload. |
| **Status-first** | Every entity shows a colored badge (green/yellow/red) immediately visible. |
| **Minimal clicks to action** | Key actions (pause, resume, trigger sync) accessible from list views. |
| **Consistent patterns** | Every resource follows: List → Detail → Create/Edit flow. |
| **Tenant isolation** | UI automatically scopes to logged-in user's bank/tenant. Superadmins see a tenant switcher. |

---

## 2. Information Architecture — Site Map

```
/login
/register
/forgot-password

/ (redirect → /dashboard)
/dashboard                          ← Home overview

/connectors                         ← Connector Definitions list
/connectors/:id                     ← Connector detail + versions

/sources                            ← Sources list
/sources/new                        ← Create Source wizard (4 steps)
/sources/:id                        ← Source detail (tabs: Overview, Schema, CDC Config, Stats)
/sources/:id/edit                   ← Edit source

/destinations                       ← Destinations list
/destinations/new                   ← Create Destination wizard (3 steps)
/destinations/:id                   ← Destination detail (tabs: Overview, Write Mode, Batch, Stats)
/destinations/:id/edit              ← Edit destination

/connections                        ← Connections list
/connections/new                    ← Create Connection wizard (5 steps)
/connections/:id                    ← Connection detail (tabs: Overview, Streams, Transforms, DQ, Schema, Runs, Health)
/connections/:id/edit               ← Edit connection

/transformations                    ← Transform pipelines list
/transformations/new                ← Create transform
/transformations/:id                ← Transform detail + code editor
/transformations/:id/edit           ← Edit transform

/udfs                               ← UDF catalog list
/udfs/new                           ← Register UDF
/udfs/:id                           ← UDF detail + code viewer
/udfs/:id/edit                      ← Edit UDF

/data-quality                       ← DQ dashboard
/data-quality/policies              ← DQ policies list
/data-quality/policies/new          ← Create DQ policy
/data-quality/policies/:id          ← Policy detail + execution history
/data-quality/violations            ← Violations list
/data-quality/violations/:id        ← Violation detail
/data-quality/templates             ← Rule templates list
/data-quality/profiling             ← Data profiling

/alerts                             ← Alerts list (triggered alerts)
/alerts/:id                         ← Alert detail + history
/alerts/rules                       ← Alert rules list
/alerts/rules/new                   ← Create alert rule
/alerts/rules/:id                   ← Alert rule detail
/alerts/channels                    ← Notification channels list
/alerts/channels/new                ← Create channel
/alerts/channels/:id                ← Channel detail
/alerts/suppressions                ← Suppression rules

/schema-evolution                   ← Schema changes overview
/schema-evolution/:connectionId     ← Per-connection schema changes

/monitoring                         ← System monitoring dashboard
/monitoring/workers                 ← Worker status list
/monitoring/connections/:id         ← Per-connection health/lag/throughput

/settings                           ← Account settings
/settings/profile                   ← User profile
/settings/password                  ← Change password
/settings/users                     ← User management (admin only)
/settings/roles                     ← Role management (admin only)
/settings/audit-logs                ← Audit trail (admin only)
/settings/system                    ← System config (superadmin only)
/settings/feature-flags             ← Feature flags (superadmin only)
/settings/maintenance-windows       ← Maintenance windows (superadmin only)
/settings/resource-quotas           ← Tenant resource quotas (superadmin only)

/dlq                                ← Dead Letter Queue dashboard
/dlq/:connectionId                  ← DLQ events for a connection
/dlq/:connectionId/:eventId         ← DLQ event detail + retry

/spark-jobs                         ← Spark job queue & applications
/spark-jobs/:id                     ← Spark job detail + executors
```

---

## 3. Global Layout & Navigation

### Layout Structure

```
┌────────────────────────────────────────────────────────────────────┐
│  TOP BAR (h-14)                                                     │
│  ┌─────┬───────────────────────────────────────────────┬──────────┐│
│  │Logo │  Breadcrumb / Search (⌘K)                     │ 🔔 👤    ││
│  └─────┴───────────────────────────────────────────────┴──────────┘│
├─────────┬──────────────────────────────────────────────────────────┤
│ SIDEBAR │  MAIN CONTENT (scrollable)                                │
│ (w-64)  │                                                           │
│         │  ┌────────────────────────────────────────────────────┐   │
│ 📊 Dash │  │  Page Header                                       │   │
│ 🔌 Conn │  │  ──────────────────────────────────────────────── │   │
│ 📥 Src  │  │  Content Area                                      │   │
│ 📤 Dest │  │                                                    │   │
│ 🔗 Link │  │                                                    │   │
│ 🔄 Trans│  │                                                    │   │
│ 🧪 DQ   │  │                                                    │   │
│ 🔔 Alert│  │                                                    │   │
│ 📊 Mon  │  │                                                    │   │
│ ⚙️ Set  │  │                                                    │   │
│         │  └────────────────────────────────────────────────────┘   │
│         │                                                           │
│ ─────── │                                                           │
│ Tenant: │                                                           │
│ [Ahli▾] │                                                           │
└─────────┴──────────────────────────────────────────────────────────┘
```

### Top Bar Components

| Component | Spec |
|-----------|------|
| **Logo** | `Fusion` text logo, links to `/dashboard` |
| **Breadcrumb** | Dynamic based on route: `Dashboard > Connections > ahli_pay_orders` |
| **Global Search** | Command palette (⌘K / Ctrl+K) — searches sources, connections, alerts |
| **Notifications bell** | Badge with unread alert count; dropdown shows last 5 alerts |
| **User avatar** | Dropdown: Profile, Preferences, Logout |
| **Tenant switcher** | (Superadmin only) Dropdown to switch bank/tenant context |

### Sidebar Navigation

```
MAIN
  Dashboard                    /dashboard
  Connectors                   /connectors

DATA PIPELINE
  Sources                      /sources
  Destinations                 /destinations
  Connections                  /connections

PROCESSING
  Transformations              /transformations
  UDFs                         /udfs

QUALITY & ALERTS
  Data Quality                 /data-quality
  Alerts                       /alerts
  Schema Evolution             /schema-evolution

OPERATIONS
  Monitoring                   /monitoring
  Spark Jobs                   /spark-jobs
  Dead Letter Queue            /dlq

ADMIN (role-gated)
  Settings                     /settings
```

**Sidebar states:**
- Collapsed: icons only (w-16)
- Expanded: icons + labels (w-64)
- Active item: highlighted bg + left border accent

---

## 4. Screen Inventory (Complete)

| # | Screen | Route | API Endpoints Used |
|---|--------|-------|-------------------|
| 1 | Login | `/login` | `POST /auth/login` |
| 2 | Register | `/register` | `POST /auth/register` |
| 3 | Dashboard | `/dashboard` | `GET /monitoring/health`, `GET /alerts/summary`, `GET /connections?status=active`, `GET /data-quality/metrics/dashboard` |
| 4 | Connectors List | `/connectors` | `GET /connector-definitions` |
| 5 | Connector Detail | `/connectors/:id` | `GET /connector-definitions/:id`, versions sub-list |
| 6 | Create Connector | modal | `POST /connector-definitions` |
| 7 | Sources List | `/sources` | `GET /sources` |
| 8 | Create Source Wizard | `/sources/new` | `POST /sources`, `POST /sources/:id/test-connection`, `POST /sources/:id/discover-schemas` |
| 9 | Source Detail | `/sources/:id` | `GET /sources/:id`, `GET /sources/:id/stats`, `GET /sources/:id/cdc-config` |
| 10 | Destinations List | `/destinations` | `GET /destinations` |
| 11 | Create Destination Wizard | `/destinations/new` | `POST /destinations`, `POST /destinations/:id/test-connection` |
| 12 | Destination Detail | `/destinations/:id` | `GET /destinations/:id`, `GET /destinations/:id/stats`, `GET /destinations/:id/write-mode`, `GET /destinations/:id/batch-settings` |
| 13 | Connections List | `/connections` | `GET /connections` |
| 14 | Create Connection Wizard | `/connections/new` | `POST /connections/validate`, `POST /connections`, `POST /connections/:id/streams`, `POST /connections/:id/schedule`, `POST /connections/:id/activate` |
| 15 | Connection Detail | `/connections/:id` | `GET /connections/:id`, `GET /connections/:id/stats`, `GET /connections/:id/schedule`, streams, health, runs |
| 16 | Streams List (tab) | `/connections/:id` (Streams tab) | `GET /streams/connections/:id/streams` |
| 17 | Transformations List | `/transformations` | `GET /transformations` |
| 18 | Transform Editor | `/transformations/:id` | `GET /transformations/:id`, `POST /transformations/:id/validate`, `POST /transformations/:id/preview` |
| 19 | UDF List | `/udfs` | `GET /udfs` |
| 20 | UDF Editor | `/udfs/:id` | `GET /udfs/:id` |
| 21 | DQ Dashboard | `/data-quality` | `GET /data-quality/metrics/dashboard` |
| 22 | DQ Policies List | `/data-quality/policies` | `GET /data-quality/policies` |
| 23 | DQ Policy Detail | `/data-quality/policies/:id` | `GET /data-quality/policies/:id`, `GET /data-quality/policies/:id/results` |
| 24 | DQ Violations | `/data-quality/violations` | `GET /data-quality/violations` |
| 25 | DQ Violation Detail | `/data-quality/violations/:id` | `GET /data-quality/violations/:id` |
| 26 | DQ Profiling | `/data-quality/profiling` | `POST /data-quality/profiling/profile` |
| 27 | Alerts List | `/alerts` | `GET /alerts`, `GET /alerts/summary` |
| 28 | Alert Detail | `/alerts/:id` | `GET /alerts/:id`, `GET /alerts/:id/history` |
| 29 | Alert Rules List | `/alerts/rules` | `GET /alerts/rules` |
| 30 | Create Alert Rule | `/alerts/rules/new` | `POST /alerts/rules` |
| 31 | Notification Channels | `/alerts/channels` | `GET /alerts/channels` |
| 32 | Create Channel | `/alerts/channels/new` | `POST /alerts/channels`, `POST /alerts/channels/:id/test` |
| 33 | Suppressions | `/alerts/suppressions` | `GET /alerts/suppressions` |
| 34 | Schema Evolution | `/schema-evolution` | `GET /schema-evolution/connections/:id/schema-changes` |
| 35 | Schema Change Detail | modal/page | approve/reject actions |
| 36 | Monitoring Dashboard | `/monitoring` | `GET /monitoring/health`, `GET /monitoring/workers`, `GET /monitoring/resource-usage` |
| 37 | Worker List | `/monitoring/workers` | `GET /monitoring/workers` |
| 38 | Connection Health | `/monitoring/connections/:id` | `GET /monitoring/connections/:id/health`, `/lag`, `/throughput`, `/checkpoints` |
| 39 | Settings - Profile | `/settings/profile` | `GET /auth/me`, `PATCH /auth/me` |
| 40 | Settings - Password | `/settings/password` | `POST /auth/change-password` |
| 41 | Settings - Users | `/settings/users` | user management APIs |
| 42 | Settings - Roles | `/settings/roles` | role/permission APIs |
| 43 | Settings - Audit Logs | `/settings/audit-logs` | audit log listing |
| 44 | Settings - System | `/settings/system` | `SystemConfig` CRUD APIs |
| 45 | Settings - Feature Flags | `/settings/feature-flags` | `FeatureFlag` CRUD APIs |
| 46 | Settings - Maintenance Windows | `/settings/maintenance-windows` | `MaintenanceWindow` CRUD APIs |
| 47 | Settings - Resource Quotas | `/settings/resource-quotas` | `ResourceQuotaViolation`, `TenantDailyUsage` APIs |
| 48 | DLQ Dashboard | `/dlq` | `EventDeadLetterQueue` listing, summary counts |
| 49 | DLQ Connection Events | `/dlq/:connectionId` | DLQ events filtered by connection |
| 50 | DLQ Event Detail | `/dlq/:connectionId/:eventId` | Event payload, error info, `EventDLQRetryHistory` |
| 51 | Spark Jobs List | `/spark-jobs` | `SparkJobQueue`, `SparkApplication` listing |
| 52 | Spark Job Detail | `/spark-jobs/:id` | `SparkApplication`, `SparkExecutor`, `SparkExecutorHistory` |
| 53 | Alert Suppressions | `/alerts/suppressions` | `AlertSuppression` CRUD |
| 54 | Alert Rule Evaluations | `/alerts/rules/:id` (tab) | `GET /alerts/rules/:id/evaluations` |
| 55 | JSON Flatten Rules | `/schema-evolution/:connectionId` (tab) | `GET/POST /schema-evolution/connections/:id/json-flatten-rules` |
| 56 | Transform Execution Log | `/transformations/:id` (tab) | `TransformationLog` listing |

---

## 5. Authentication Screens

### 5.1 Login (`/login`)

```
┌──────────────────────────────────────────────┐
│                                              │
│            ┌─────────────────────┐           │
│            │    🌐 Fusion    │           │
│            │    CDC Platform     │           │
│            └─────────────────────┘           │
│                                              │
│  ┌────────────────────────────────────────┐  │
│  │  Username or Email                     │  │
│  │  ┌──────────────────────────────────┐  │  │
│  │  │ admin@example.com             │  │  │
│  │  └──────────────────────────────────┘  │  │
│  │                                        │  │
│  │  Password                              │  │
│  │  ┌──────────────────────────────────┐  │  │
│  │  │ ••••••••••                   👁  │  │  │
│  │  └──────────────────────────────────┘  │  │
│  │                                        │  │
│  │  [✓] Remember me    Forgot password?   │  │
│  │                                        │  │
│  │  ┌──────────────────────────────────┐  │  │
│  │  │         Sign In                  │  │  │
│  │  └──────────────────────────────────┘  │  │
│  │                                        │  │
│  │  ───── OR ─────                        │  │
│  │                                        │  │
│  │  ┌──────────────────────────────────┐  │  │
│  │  │     Sign in with SSO (OIDC)      │  │  │
│  │  └──────────────────────────────────┘  │  │
│  │                                        │  │
│  │  Don't have an account? Register       │  │
│  └────────────────────────────────────────┘  │
│                                              │
└──────────────────────────────────────────────┘
```

**Component spec:**
| Element | Component | Validation | Error state |
|---------|-----------|------------|-------------|
| Username/Email | `Input` type="text" | Required, min 3 chars | Red border + "Required" |
| Password | `Input` type="password" + eye toggle | Required | Red border |
| Remember me | `Checkbox` | — | — |
| Sign In button | `Button` variant="primary" | Disabled until form valid | Loading spinner on submit |
| SSO button | `Button` variant="outline" | — | Shown only if `OIDC_ISSUER` configured |
| Error toast | `Alert` variant="destructive" | — | "Invalid credentials" / "Account locked for 30min" |

**States:**
- Default (empty form)
- Filled (button enabled)
- Loading (spinner, inputs disabled)
- Error (shake animation, error message below form)
- Locked ("Account locked. Try again in X minutes.")

---

### 5.2 Register (`/register`)

```
┌────────────────────────────────────────────────┐
│  Create Account                                │
│                                                │
│  First Name          Last Name                 │
│  ┌───────────────┐  ┌───────────────────────┐  │
│  │               │  │                       │  │
│  └───────────────┘  └───────────────────────┘  │
│                                                │
│  Username                                      │
│  ┌────────────────────────────────────────┐    │
│  │                                        │    │
│  └────────────────────────────────────────┘    │
│                                                │
│  Email                                         │
│  ┌────────────────────────────────────────┐    │
│  │                                        │    │
│  └────────────────────────────────────────┘    │
│                                                │
│  Password                                      │
│  ┌────────────────────────────────────────┐    │
│  │                                    👁  │    │
│  └────────────────────────────────────────┘    │
│  ░░░░░░░░░░ Strength: Weak                    │
│                                                │
│  Confirm Password                              │
│  ┌────────────────────────────────────────┐    │
│  │                                        │    │
│  └────────────────────────────────────────┘    │
│                                                │
│  ┌────────────────────────────────────────┐    │
│  │         Create Account                 │    │
│  └────────────────────────────────────────┘    │
│                                                │
│  Already have an account? Sign in              │
└────────────────────────────────────────────────┘
```

**Validation rules:**
- Username: 3-50 chars, alphanumeric + underscore
- Email: valid email format
- Password: min 8 chars, 1 uppercase, 1 number, 1 special char
- Confirm password: must match

---

## 6. Dashboard — Home (`/dashboard`)

### Layout

```
┌────────────────────────────────────────────────────────────────────────┐
│  Dashboard                                            [Last 24h ▾] 🔄  │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐              │
│  │  Active  │  │  Events  │  │   CDC    │  │  DQ      │              │
│  │  Conn.   │  │  /sec    │  │   Lag    │  │  Score   │              │
│  │   12     │  │  4,231   │  │  < 2s    │  │  98.5%   │              │
│  │  ▲ +2    │  │  ▲ +15%  │  │  ✓ Good  │  │  ▼ -0.3% │              │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘              │
│                                                                        │
├────────────────────────────────────────────┬───────────────────────────┤
│  Pipeline Status                           │  Recent Alerts            │
│  ┌────────────────────────────────────┐    │  ┌────────────────────┐  │
│  │  [======█████████=====     ]       │    │  │ 🔴 CDC lag >60s    │  │
│  │  12 active  2 paused  1 error      │    │  │    CIM orders      │  │
│  │                                    │    │  │    5 min ago        │  │
│  │  Connection         Status  Lag    │    │  │                    │  │
│  │  ─────────────────────────────── │    │  │ 🟡 DQ violation    │  │
│  │  ahli_pay_orders    ● Active 1.2s │    │  │    wps_kit_details │  │
│  │  ahli_pay_kit       ● Active 0.8s │    │  │    12 min ago      │  │
│  │  cim_accounts       ⏸ Paused  —   │    │  │                    │  │
│  │  alizz_customers    ⚠ Error   —   │    │  │ 🟢 Resolved        │  │
│  │                                    │    │  │    Worker restart   │  │
│  │  View all →                        │    │  │    1 hour ago       │  │
│  └────────────────────────────────────┘    │  │                    │  │
│                                            │  │  View all →         │  │
├────────────────────────────────────────────┤  └────────────────────┘  │
│  Throughput (last 6 hours)                 │                          │
│  ┌────────────────────────────────────┐    │  System Health           │
│  │  📈 Events/sec line chart          │    │  ┌────────────────────┐  │
│  │     ╱─╲    ╱──╲                    │    │  │ ● PostgreSQL  OK   │  │
│  │    ╱   ╲──╱    ╲──                 │    │  │ ● Redis       OK   │  │
│  │   ╱                ╲──            │    │  │ ● Workers (3) OK   │  │
│  │  ╱                    ╲           │    │  │ ● Spark       OK   │  │
│  └────────────────────────────────────┘    │  └────────────────────┘  │
│                                            │                          │
└────────────────────────────────────────────┴──────────────────────────┘
```

### Dashboard Components

| Component | Props | API |
|-----------|-------|-----|
| `StatCard` | title, value, trend (up/down/neutral), icon, color | Computed from aggregated data |
| `PipelineStatusBar` | active, paused, error counts | `GET /connections` with group by status |
| `ConnectionsTable` (mini) | Shows top 5 connections by lag | `GET /connections?status=active&page_size=5` |
| `AlertsFeed` | Last 5 alerts, severity badge, relative time | `GET /alerts?page_size=5` |
| `ThroughputChart` | Line chart (Recharts/Tremor), 6h window, events/sec | `GET /monitoring/connections/:id/throughput` (aggregated) |
| `SystemHealth` | Service name + status dot (green/red) | `GET /monitoring/health` |
| `TimeRangeSelector` | Dropdown: 1h, 6h, 24h, 7d | Controls chart window |

---

## 7. Connectors (Definitions) (`/connectors`)

### 7.1 Connectors List

```
┌────────────────────────────────────────────────────────────────────────┐
│  Connector Definitions                            [+ New Connector]    │
├────────────────────────────────────────────────────────────────────────┤
│  ┌─────────┐ ┌───────────────┐ ┌────────────┐ ┌────────────────────┐ │
│  │All (8)  │ │Sources (5)    │ │Dest. (3)   │ │ 🔍 Search...       │ │
│  └─────────┘ └───────────────┘ └────────────┘ └────────────────────┘ │
│                                                                        │
│  ┌─────────────────────────────────────────────────────────────────┐  │
│  │ 🐬  MySQL Source                                                │  │
│  │      Type: mysql  │  Category: Source  │  v2.1.0               │  │
│  │      CDC ✓  │  Full Refresh ✓  │  Incremental ✓               │  │
│  │      Used by: 5 sources                                        │  │
│  ├─────────────────────────────────────────────────────────────────┤  │
│  │ 🍃  MongoDB Source                                              │  │
│  │      Type: mongodb  │  Category: Source  │  v1.3.0             │  │
│  │      CDC ✓  │  Full Refresh ✓  │  Incremental ✗               │  │
│  │      Used by: 3 sources                                        │  │
│  ├─────────────────────────────────────────────────────────────────┤  │
│  │ 🐘  PostgreSQL Destination                                      │  │
│  │      Type: postgres  │  Category: Destination  │  v1.0.0       │  │
│  │      SCD1 ✓  │  SCD2 ✓                                        │  │
│  │      Used by: 8 destinations                                   │  │
│  └─────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────┘
```

### 7.2 Connector Detail (`/connectors/:id`)

**Tabs:** Overview | Versions | Config Schema | Usage

**Overview tab:**
- Icon, name, type, category
- Capabilities badges (CDC, Full Refresh, Incremental)
- Documentation URL link
- Required fields list
- Default config (JSON viewer)

**Versions tab:**
- Table: version, release date, stable badge, breaking changes badge
- Expand row → release notes, new features, bug fixes

---

## 8. Sources (`/sources`)

### 8.1 Sources List

```
┌────────────────────────────────────────────────────────────────────────┐
│  Sources                                               [+ New Source]  │
├────────────────────────────────────────────────────────────────────────┤
│  Filters: [Status ▾] [Connector Type ▾]    🔍 Search sources...       │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  ┌──┬──────────────────────┬────────┬────────┬────────────┬─────────┐ │
│  │  │ Name                 │ Type   │ Status │ Last Test  │ Actions │ │
│  ├──┼──────────────────────┼────────┼────────┼────────────┼─────────┤ │
│  │🐬│ Ahli Pay MySQL       │ mysql  │ ●Active│ ✓ 2h ago   │ ⋯      │ │
│  │🐬│ CIM MySQL Primary    │ mysql  │ ●Active│ ✓ 1h ago   │ ⋯      │ │
│  │🍃│ Alizz MongoDB        │ mongodb│ ●Active│ ✓ 3h ago   │ ⋯      │ │
│  │🐬│ WPS MySQL            │ mysql  │ ⚠Draft │ ✗ Failed   │ ⋯      │ │
│  └──┴──────────────────────┴────────┴────────┴────────────┴─────────┘ │
│                                                                        │
│  Showing 1-4 of 4                                   [< 1 >]           │
└────────────────────────────────────────────────────────────────────────┘
```

**Row actions (⋯ dropdown):** Test Connection, Discover Schemas, Edit, Delete

### 8.2 Create Source Wizard (`/sources/new`) — 4 Steps

```
Step 1: Select Connector          Step 2: Configure          Step 3: Test          Step 4: Discover
━━━━━●━━━━━━━━━━━━━━━━━━━━━━━━━━○━━━━━━━━━━━━━━━━━━━━━━━━━○━━━━━━━━━━━━━━━━━━━○━━━
```

**Step 1 — Select Connector Type**

```
┌────────────────────────────────────────────────────────────────┐
│  Create New Source — Step 1 of 4                               │
│  Select connector type                                         │
│                                                                │
│  ┌───────────────────┐  ┌───────────────────┐                 │
│  │  🐬               │  │  🍃               │                 │
│  │  MySQL            │  │  MongoDB          │                 │
│  │  Binlog CDC       │  │  Change Streams   │                 │
│  │  v2.1.0           │  │  v1.3.0           │                 │
│  │  ✓ selected       │  │                   │                 │
│  └───────────────────┘  └───────────────────┘                 │
│                                                                │
│  ┌───────────────────┐  ┌───────────────────┐                 │
│  │  🐘               │  │  🔄               │                 │
│  │  PostgreSQL       │  │  Polling (Generic)│                 │
│  │  Logical Repl.    │  │  Timestamp-based  │                 │
│  │  v1.0.0           │  │  v1.0.0           │                 │
│  └───────────────────┘  └───────────────────┘                 │
│                                                                │
│                                          [Cancel]  [Next →]   │
└────────────────────────────────────────────────────────────────┘
```

**Step 2 — Connection Configuration**

```
┌────────────────────────────────────────────────────────────────┐
│  Create New Source — Step 2 of 4                               │
│  Configure MySQL connection                                    │
│                                                                │
│  Source Name *                                                 │
│  ┌────────────────────────────────────────────────────┐        │
│  │ Ahli Pay MySQL (Shared Qatar)                      │        │
│  └────────────────────────────────────────────────────┘        │
│                                                                │
│  Host *                            Port *                      │
│  ┌──────────────────────────┐     ┌──────────────────┐        │
│  │ mysql-router.mysql-route │     │ 7557             │        │
│  └──────────────────────────┘     └──────────────────┘        │
│                                                                │
│  Database Name *                                               │
│  ┌────────────────────────────────────────────────────┐        │
│  │ AHLIIPAY                                           │        │
│  └────────────────────────────────────────────────────┘        │
│                                                                │
│  Username *                        Password *                  │
│  ┌──────────────────────────┐     ┌──────────────────┐        │
│  │ cdc_user                 │     │ ••••••••••   👁  │        │
│  └──────────────────────────┘     └──────────────────┘        │
│                                                                │
│  ▼ Advanced (SSL, Connection Pool)                             │
│  ┌────────────────────────────────────────────────────┐        │
│  │  [✓] Enable SSL                                    │        │
│  │  CA Certificate:  [Upload file]                    │        │
│  │  Client Cert:     [Upload file]                    │        │
│  │  Client Key:      [Upload file]                    │        │
│  └────────────────────────────────────────────────────┘        │
│                                                                │
│                                  [← Back] [Cancel] [Next →]   │
└────────────────────────────────────────────────────────────────┘
```

**Step 3 — Test Connection**

```
┌────────────────────────────────────────────────────────────────┐
│  Create New Source — Step 3 of 4                               │
│  Test connectivity                                             │
│                                                                │
│  ┌────────────────────────────────────────────────────┐        │
│  │                                                    │        │
│  │    ┌───┐          ┌───┐          ┌───┐            │        │
│  │    │ ✓ │ ──────── │ ✓ │ ──────── │ ⟳ │            │        │
│  │    └───┘          └───┘          └───┘            │        │
│  │   Network        Auth          Binlog             │        │
│  │   Connected      Authenticated  Checking...       │        │
│  │                                                    │        │
│  └────────────────────────────────────────────────────┘        │
│                                                                │
│  Test Results:                                                 │
│  ✓ TCP connection to mysql-router:7557 successful              │
│  ✓ Authentication successful (user: cdc_user)                  │
│  ⟳ Checking binlog format...                                   │
│                                                                │
│  [Test Again]                    [← Back] [Cancel] [Next →]   │
└────────────────────────────────────────────────────────────────┘
```

**Step 4 — Discover & Select Tables**

```
┌────────────────────────────────────────────────────────────────┐
│  Create New Source — Step 4 of 4                               │
│  Select tables to sync                                         │
│                                                                │
│  Database: AHLIIPAY                          [Select All]      │
│                                                                │
│  🔍 Filter tables...                                           │
│                                                                │
│  ┌──┬──────────────────────────┬──────────┬────────────────┐   │
│  │☑ │ Table                    │ Rows     │ PK             │   │
│  ├──┼──────────────────────────┼──────────┼────────────────┤   │
│  │☑ │ address_customer         │ 45,231   │ pkey           │   │
│  │☑ │ business_custom_field    │ 12,100   │ pkey           │   │
│  │☑ │ business_entity          │ 8,420    │ pkey           │   │
│  │☑ │ channel                  │ 156      │ pkey           │   │
│  │☑ │ kit                      │ 67,890   │ pkey           │   │
│  │☐ │ audit_log_internal       │ 2.1M     │ id             │   │
│  │☐ │ _migrations              │ 45       │ version        │   │
│  └──┴──────────────────────────┴──────────┴────────────────┘   │
│                                                                │
│  5 tables selected                                             │
│                                                                │
│                         [← Back] [Cancel] [Create Source]     │
└────────────────────────────────────────────────────────────────┘
```

### 8.3 Source Detail (`/sources/:id`)

**Tabs:** Overview | Schema Discovery | CDC Config | Statistics

**Overview tab:**
- Status badge (Active/Draft/Inactive)
- Connection info (host, port, database, connector type)
- Last test result + timestamp
- Quick actions: Test Connection, Discover Schemas, Edit, Delete

**Schema Discovery tab:**
- Tree view of database → schemas → tables → columns
- Column details: name, type, nullable, is_pk
- "Refresh Schema" button

**CDC Config tab:**
- Binlog format (ROW/MIXED/STATEMENT)
- Server ID
- GTID mode
- Replication lag

**Statistics tab:**
- Events captured (chart)
- Tables monitored count
- Errors in last 24h
- Uptime percentage

---

## 9. Destinations (`/destinations`)

### 9.1 Destinations List

Same pattern as Sources list but showing destination-specific info:
- Destination name, type (postgres/iceberg), status, last test

### 9.2 Create Destination Wizard (`/destinations/new`) — 3 Steps

**Step 1 — Select Destination Type**
- Cards: PostgreSQL, Apache Iceberg, (future: Snowflake, BigQuery)

**Step 2 — Configure**

For PostgreSQL:
```
┌────────────────────────────────────────────────────────────────┐
│  Destination Name *                                            │
│  ┌────────────────────────────────────────────────────┐        │
│  │ Ahli Pay Reports (PostgreSQL)                      │        │
│  └────────────────────────────────────────────────────┘        │
│                                                                │
│  Connection String (DSN) *                                     │
│  ┌────────────────────────────────────────────────────┐        │
│  │ host=10.51.7.7 port=5665 dbname=ahliipay_reports   │        │
│  └────────────────────────────────────────────────────┘        │
│                                                                │
│  Default Schema *                                              │
│  ┌────────────────────────────────────────────────────┐        │
│  │ dw_dbx                                             │        │
│  └────────────────────────────────────────────────────┘        │
│                                                                │
│  Write Mode                                                    │
│  ○ SCD Type 1 (Upsert — latest value wins)                    │
│  ○ SCD Type 2 (History tracking — valid_from/valid_to)         │
│  ○ Append Only (Insert, no dedup)                              │
└────────────────────────────────────────────────────────────────┘
```

For Iceberg:
```
┌────────────────────────────────────────────────────────────────┐
│  Catalog Type: [Nessie ▾]                                      │
│  Catalog Name: ┌────────────────────────────────┐              │
│                │ nessie                          │              │
│                └────────────────────────────────┘              │
│  Namespace:    ┌────────────────────────────────┐              │
│                │ raw_indusind                    │              │
│                └────────────────────────────────┘              │
│                                                                │
│  Storage:                                                      │
│  ○ Azure Blob   ○ AWS S3   ○ GCS                              │
│                                                                │
│  Container / Bucket *                                          │
│  ┌────────────────────────────────────────────────────┐        │
│  │ fusion-iceberg-prod                            │        │
│  └────────────────────────────────────────────────────┘        │
└────────────────────────────────────────────────────────────────┘
```

**Step 3 — Test Connection**
- Same pattern as Source test (network → auth → write permission check)

### 9.3 Destination Detail (`/destinations/:id`)

**Tabs:** Overview | Write Mode | Schema Mapping | Batch Settings | Statistics

---

## 10. Connections (`/connections`)

### 10.1 Connections List

```
┌────────────────────────────────────────────────────────────────────────┐
│  Connections                                      [+ New Connection]   │
├────────────────────────────────────────────────────────────────────────┤
│  Filters: [Status ▾] [Sync Mode ▾] [Source ▾] [Destination ▾]        │
│  🔍 Search connections...                                              │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  ┌──────────────────────────────────────────────────────────────────┐ │
│  │ ahli_pay_address_customer                                         │ │
│  │ 🐬 Ahli Pay MySQL ────→ 🐘 Ahli Pay Reports PG                   │ │
│  │ Mode: CDC  │  Status: ● Active  │  Lag: 1.2s  │  Events: 4.2K/h │ │
│  │                                    [⏸ Pause] [⟳ Sync] [⋯]       │ │
│  ├──────────────────────────────────────────────────────────────────┤ │
│  │ cim_accounts_sync                                                 │ │
│  │ 🐬 CIM MySQL ────→ 🐘 CIM Reports PG                             │ │
│  │ Mode: CDC  │  Status: ⏸ Paused  │  Lag: —   │  Events: —        │ │
│  │                                   [▶ Resume] [⟳ Sync] [⋯]       │ │
│  ├──────────────────────────────────────────────────────────────────┤ │
│  │ indusind_upi_iceberg                                              │ │
│  │ 🍃 IndusInd MongoDB ────→ 🧊 Iceberg (Nessie)                    │ │
│  │ Mode: CDC  │  Status: ⚠ Error   │  Lag: 45s │  Events: 120/h    │ │
│  │                                    [🔍 View Error] [⟳ Retry] [⋯] │ │
│  └──────────────────────────────────────────────────────────────────┘ │
│                                                                        │
│  Showing 1-3 of 15                              [< 1  2  3  4  5 >]  │
└────────────────────────────────────────────────────────────────────────┘
```

**Connection card components:**
- Source icon + name → arrow → Destination icon + name
- Status badge (Active: green dot, Paused: grey pause icon, Error: red warning)
- Lag metric (real-time from `/monitoring/connections/:id/lag`)
- Throughput metric
- Inline action buttons

### 10.2 Create Connection Wizard (`/connections/new`) — 5 Steps

```
Step 1: Source    Step 2: Destination    Step 3: Streams    Step 4: Config    Step 5: Review
━━━━━●━━━━━━━━━━━━━━━━━━○━━━━━━━━━━━━━━━━━━━━━━○━━━━━━━━━━━━━━━━━○━━━━━━━━━━━━━○━━━
```

**Step 1 — Select Source**
- Radio list of existing sources (grouped by connector type)
- Each source shows: name, host, database, status badge

**Step 2 — Select Destination**
- Radio list of existing destinations
- Shows destination type, name, database/catalog

**Step 3 — Configure Streams (Tables)**
```
┌────────────────────────────────────────────────────────────────┐
│  Select streams (tables) to sync                               │
│                                                                │
│  Available Tables from: Ahli Pay MySQL / AHLIIPAY              │
│  ┌──┬──────────────────────┬──────┬──────┬───────────────────┐ │
│  │☑ │ Table                │ Mode │ PK   │ Cursor Field      │ │
│  ├──┼──────────────────────┼──────┼──────┼───────────────────┤ │
│  │☑ │ address_customer     │ CDC▾ │ pkey │ (auto — binlog)   │ │
│  │☑ │ kit                  │ CDC▾ │ pkey │ (auto — binlog)   │ │
│  │☑ │ kit_details          │ CDC▾ │ pkey │ (auto — binlog)   │ │
│  │☐ │ audit_log_internal   │ —    │ id   │ —                 │ │
│  └──┴──────────────────────┴──────┴──────┴───────────────────┘ │
│                                                                │
│  ▼ Advanced: Per-stream overrides                              │
│  ┌────────────────────────────────────────────────────┐        │
│  │  address_customer:                                 │        │
│  │  Destination table name: address_customer          │        │
│  │  PK columns: [pkey]                                │        │
│  │  JSON flatten columns: [metadata]                  │        │
│  └────────────────────────────────────────────────────┘        │
└────────────────────────────────────────────────────────────────┘
```

**Step 4 — Pipeline Configuration**
```
┌────────────────────────────────────────────────────────────────┐
│  Connection Name *                                             │
│  ┌────────────────────────────────────────────────────┐        │
│  │ ahli_pay_address_customer                          │        │
│  └────────────────────────────────────────────────────┘        │
│                                                                │
│  Sync Mode:  ● CDC (real-time)  ○ Incremental  ○ Full Refresh │
│                                                                │
│  Schedule:                                                     │
│  ● Continuous (streaming)                                      │
│  ○ Scheduled: [Cron expression: ________]                      │
│    └ [✓] Generate Airflow DAG automatically                    │
│  ○ Manual trigger only                                         │
│                                                                │
│  ── Transform Pipeline (optional) ──                           │
│  [ None selected ▾ ]  or  [+ Create new]                      │
│                                                                │
│  ── Data Quality Policy (optional) ──                          │
│  [ None selected ▾ ]  or  [+ Create new]                      │
│                                                                │
│  ── Schema Evolution Policy ──                                 │
│  ● Auto Apply (auto-add new columns)                           │
│  ○ Manual Approval (changes queue for review)                  │
│                                                                │
│  ▼ Resource Limits (optional)                                  │
│  Max events/sec: ┌────────┐  Max memory: ┌────────┐           │
│                   │ 10000  │              │ 2048MB │           │
│                   └────────┘              └────────┘           │
└────────────────────────────────────────────────────────────────┘
```

**Step 5 — Review & Create**
```
┌────────────────────────────────────────────────────────────────┐
│  Review Connection                                             │
│                                                                │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Connection: ahli_pay_address_customer                  │   │
│  │                                                         │   │
│  │  Source:      Ahli Pay MySQL (mysql-router:7557)        │   │
│  │  Destination: Ahli Pay Reports PG (10.51.7.7:5665)     │   │
│  │  Sync Mode:   CDC (real-time, continuous)               │   │
│  │  Tables:      3 streams                                 │   │
│  │               - address_customer (PK: pkey)             │   │
│  │               - kit (PK: pkey)                          │   │
│  │               - kit_details (PK: pkey)                  │   │
│  │  Transform:   None                                      │   │
│  │  DQ Policy:   None                                      │   │
│  │  Schema Evo:  Auto Apply                                │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                │
│  [✓] Activate immediately after creation                       │
│                                                                │
│                        [← Back] [Cancel] [Create & Activate]  │
└────────────────────────────────────────────────────────────────┘
```

### 10.3 Connection Detail (`/connections/:id`)

**Tabs:** Overview | Streams | Transforms | Data Quality | Schema Evolution | Sync Runs | Health

**Overview tab:**
```
┌────────────────────────────────────────────────────────────────────────┐
│  ahli_pay_address_customer                    ● Active                 │
│  ─────────────────────────────────────────────────────────────────── │
│                                                                        │
│  ┌────────────┐      ┌──────────────┐      ┌────────────────┐        │
│  │ 🐬 MySQL   │ ───→ │   Pipeline   │ ───→ │ 🐘 PostgreSQL  │        │
│  │ AHLIIPAY   │      │ Transform+DQ │      │ dw_dbx         │        │
│  └────────────┘      └──────────────┘      └────────────────┘        │
│                                                                        │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐    │
│  │ Events/hr   │ │ Lag         │ │ Uptime      │ │ Last Sync   │    │
│  │ 4,231       │ │ 1.2s        │ │ 99.9%       │ │ 2 min ago   │    │
│  └─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘    │
│                                                                        │
│  Actions: [⏸ Pause] [⟳ Trigger Sync] [✏️ Edit] [🗑 Delete]           │
│                                                                        │
│  Quick Config:                                                         │
│  Sync Mode:       CDC                                                  │
│  Schedule:        Continuous                                           │
│  Schema Policy:   Auto Apply                                           │
│  Created:         2026-04-15 10:23:00 UTC                              │
│  Created by:      admin@example.com                                 │
└────────────────────────────────────────────────────────────────────────┘
```

**Streams tab:**
- Table listing all streams with enable/disable toggle
- Per-stream: schema_name, table_name, sync_mode, cursor_field, PK

**Sync Runs tab:**
```
┌────────────────────────────────────────────────────────────────┐
│ Run #  │ Trigger  │ Status    │ Records  │ Duration │ Time     │
├────────┼──────────┼───────────┼──────────┼──────────┼──────────┤
│ #142   │ CDC      │ ● Success │ 1,245    │ 2.3s     │ 2 min ago│
│ #141   │ CDC      │ ● Success │ 892      │ 1.8s     │ 7 min ago│
│ #140   │ Manual   │ ● Success │ 45,231   │ 45s      │ 1h ago   │
│ #139   │ CDC      │ ✗ Failed  │ 0        │ 0.5s     │ 2h ago   │
└────────┼──────────┼───────────┼──────────┼──────────┼──────────┘
```

**Health tab:**
- Real-time lag chart (sparkline)
- Throughput chart (events/sec over time)
- Worker status (pod name, CPU, memory)
- Checkpoint info (binlog file, position)

---

## 11. Streams (per Connection)

Embedded within Connection Detail > Streams tab. Also accessible via inline edit.

**Stream row (expanded):**
```
┌────────────────────────────────────────────────────────────────┐
│ ☑ address_customer                                   [Enabled] │
│ ────────────────────────────────────────────────────────────── │
│ Schema: AHLIIPAY  │  Sync: CDC  │  PK: [pkey]                 │
│                                                                │
│ ▼ Configuration                                                │
│   Destination table: address_customer                          │
│   JSON columns: [metadata, extra_fields]                       │
│   Transform overrides: None                                    │
│   Cursor field: (N/A — CDC mode)                               │
│                                           [Edit] [Disable]     │
└────────────────────────────────────────────────────────────────┘
```

---

## 12. Transformations (`/transformations`)

### 12.1 Transformations List

```
┌────────────────────────────────────────────────────────────────────────┐
│  Transform Pipelines                             [+ New Transform]     │
├────────────────────────────────────────────────────────────────────────┤
│  Filters: [Type ▾] [Language ▾] [Status ▾]                            │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  ┌──────────────────────┬──────┬────────┬─────┬───────────┬─────────┐ │
│  │ Name                 │ Type │Language│ Ver │ Status    │ Actions │ │
│  ├──────────────────────┼──────┼────────┼─────┼───────────┼─────────┤ │
│  │ null_byte_strip      │ spark│ python │ v3  │ Published │ ⋯       │ │
│  │ lowercase_columns    │ sql  │ sql    │ v1  │ Published │ ⋯       │ │
│  │ audit_timestamp_add  │ spark│ python │ v2  │ Draft     │ ⋯       │ │
│  └──────────────────────┴──────┴────────┴─────┴───────────┴─────────┘ │
└────────────────────────────────────────────────────────────────────────┘
```

### 12.2 Transform Editor (`/transformations/:id`)

```
┌────────────────────────────────────────────────────────────────────────┐
│  null_byte_strip                    v3  │ Published ●  │ [Validate] │  │
├───────────────────────────────────────┬────────────────────────────────┤
│  Code Editor (Monaco)                 │  Output Preview               │
│  ┌────────────────────────────────┐   │  ┌───────────────────────────┐│
│  │ 1 │ def transform(df):        │   │  │ Input:                    ││
│  │ 2 │   from pyspark.sql import │   │  │ name     │ value          ││
│  │ 3 │     functions as F        │   │  │ "Al\x00i"│ "test\x00"    ││
│  │ 4 │                           │   │  │                           ││
│  │ 5 │   for col_name in         │   │  │ Output:                   ││
│  │ 6 │     df.columns:           │   │  │ name     │ value          ││
│  │ 7 │     if df.schema[         │   │  │ "Ali"    │ "test"         ││
│  │ 8 │       col_name].dataType  │   │  │                           ││
│  │ 9 │       == StringType():    │   │  │ ✓ Validation passed       ││
│  │10 │       df = df.withColumn( │   │  │ 2 columns transformed     ││
│  │11 │         col_name,         │   │  │                           ││
│  │12 │         F.regexp_replace( │   │  │                           ││
│  │13 │           col_name,       │   │  │                           ││
│  │14 │           '\\x00|\\u0000',│   │  │                           ││
│  │15 │           ''              │   │  │                           ││
│  │16 │         )                 │   │  │                           ││
│  │17 │       )                   │   │  │                           ││
│  │18 │   return df               │   │  │                           ││
│  └────────────────────────────────┘   │  └───────────────────────────┘│
│                                       │                               │
│  Language: Python  │  Type: Spark     │  [▶ Run Preview]             │
│                                       │                               │
├───────────────────────────────────────┴───────────────────────────────┤
│  Linked to connections: ahli_pay_address_customer, wps_kit_details    │
│  Last executed: 2 hours ago  │  Execution mode: Streaming             │
│                                                                        │
│  [Save Draft]  [Validate]  [Preview]  [Publish]                       │
└────────────────────────────────────────────────────────────────────────┘
```

### 12.3 Transform Step Types (Visual Builder Mode)

The transform system supports **10 step types** that can be configured visually (no-code) OR via code editor:

| Step Type | Description | Config Fields |
|-----------|-------------|---------------|
| `cast` | Change column data type | column, target_type (string/int/float/bool/timestamp) |
| `string_op` | String manipulation | column, op (trim/lower/upper/replace/substring) |
| `math_op` | Arithmetic | column, op (add/subtract/multiply/divide), operand |
| `date_op` | Date transformations | column, op (extract_year/month/day, date_add, format) |
| `json_extract` | Extract value from JSON | column, json_path, output_column, output_type |
| `json_flatten_inline` | Flatten JSON into columns | column, paths[] |
| `json_flatten_child` | Expand JSON array to rows | column, child_path |
| `mask` | PII masking | column, mask_char, visible_chars, direction (left/right) |
| `expression` | SEL expression | expression (e.g., `col1 + col2`), output_column |
| `udf` | Call registered UDF | udf_name, input_columns[], output_column |

**Visual Pipeline Builder:**
```
┌────────────────────────────────────────────────────────────────────────┐
│  Transform Pipeline: ahli_pay_cleanup          [Code Mode] [Visual ●]  │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐    │
│  │ Step 1   │ ──→ │ Step 2   │ ──→ │ Step 3   │ ──→ │ Step 4   │    │
│  │ cast     │     │ string_op│     │ mask     │     │ udf      │    │
│  │ amount→  │     │ name→    │     │ email→   │     │ mask_pii │    │
│  │ float    │     │ trim     │     │ ***      │     │          │    │
│  └──────────┘     └──────────┘     └──────────┘     └──────────┘    │
│                                                                        │
│  [+ Add Step]                                                          │
│                                                                        │
│  ── Step 3 Configuration ──                                            │
│  ┌────────────────────────────────────────────────────────────────┐   │
│  │  Type: mask                                                    │   │
│  │  Column: email                                                 │   │
│  │  Mask character: *                                             │   │
│  │  Visible chars: 3                                              │   │
│  │  Direction: right                                              │   │
│  │  Preview: "ahmed@example.com" → "ahm**************"            │   │
│  └────────────────────────────────────────────────────────────────┘   │
│                                                                        │
│  [Save]  [Validate All Steps]  [Preview Output]                        │
└────────────────────────────────────────────────────────────────────────┘
```

### 12.4 Transform Execution Log (Tab in Transform Detail)

**Tabs:** Code Editor | Visual Builder | Execution Log | Dependencies

```
┌────────────────────────────────────────────────────────────────────────┐
│  Execution Log — null_byte_strip                                       │
├────────────────────────────────────────────────────────────────────────┤
│  Filters: [Status ▾] [Connection ▾] [Date Range]                      │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  ┌──────────────────┬──────────────────┬───────┬────────┬───────────┐ │
│  │ Timestamp        │ Connection       │ Rows  │Duration│ Status    │ │
│  ├──────────────────┼──────────────────┼───────┼────────┼───────────┤ │
│  │ 10:23:45 UTC     │ ahli_pay_addr..  │ 5,000 │ 1.2s   │ ● Success │ │
│  │ 10:18:30 UTC     │ ahli_pay_addr..  │ 3,200 │ 0.8s   │ ● Success │ │
│  │ 10:15:12 UTC     │ wps_kit_details  │ 1,500 │ 0.4s   │ ● Success │ │
│  │ 09:45:00 UTC     │ cim_accounts     │ 8,000 │ 2.1s   │ ✗ Error   │ │
│  └──────────────────┴──────────────────┴───────┴────────┴───────────┘ │
│                                                                        │
│  Error details (click to expand):                                      │
│  09:45:00 — TypeError: cannot cast NoneType to float in column "amt"  │
│                                                                        │
│  Stats: 99.2% success rate (last 24h)  │  Avg duration: 1.1s          │
└────────────────────────────────────────────────────────────────────────┘
```

### 12.5 Transform Dependencies

```
┌────────────────────────────────────────────────────────────────────────┐
│  Dependencies — null_byte_strip                                        │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  Depends On (upstream):                                                │
│  • (none — this is a root transform)                                   │
│                                                                        │
│  Depended By (downstream):                                             │
│  • audit_timestamp_add (runs after null_byte_strip)                    │
│                                                                        │
│  Execution Order:                                                      │
│  ┌───────────────┐      ┌──────────────────┐      ┌────────────────┐ │
│  │null_byte_strip│ ───→ │audit_timestamp_add│ ───→ │ (write to dest)│ │
│  │   order: 1    │      │    order: 2       │      │                │ │
│  └───────────────┘      └──────────────────┘      └────────────────┘ │
│                                                                        │
│  [+ Add Dependency]  [Reorder]                                         │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 13. UDFs (User-Defined Functions) (`/udfs`)

### 13.1 UDF List

Same list pattern. Columns: Name, Language (python/scala/java), Category, Return Type, Status.

### 13.2 UDF Editor (`/udfs/:id`)

```
┌────────────────────────────────────────────────────────────────────────┐
│  mask_pii                                   Language: Python           │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  Description: Masks PII fields (email, phone) for GDPR compliance     │
│  Category: Security  │  Return Type: StringType                       │
│                                                                        │
│  Parameters:                                                           │
│  ┌───────────────┬────────────┬──────────────────────────────────┐    │
│  │ Name          │ Type       │ Description                      │    │
│  ├───────────────┼────────────┼──────────────────────────────────┤    │
│  │ value         │ str        │ The input string to mask         │    │
│  │ mask_char     │ str        │ Character to use for masking     │    │
│  │ visible_chars │ int        │ Number of chars to keep visible  │    │
│  └───────────────┴────────────┴──────────────────────────────────┘    │
│                                                                        │
│  Code:                                                                 │
│  ┌────────────────────────────────────────────────────────────────┐   │
│  │ def mask_pii(value: str, mask_char: str = '*',                 │   │
│  │              visible_chars: int = 3) -> str:                    │   │
│  │     if not value or len(value) <= visible_chars:                │   │
│  │         return value                                            │   │
│  │     return value[:visible_chars] + mask_char * (len(value) -    │   │
│  │                                                  visible_chars) │   │
│  └────────────────────────────────────────────────────────────────┘   │
│                                                                        │
│  [Save]  [Validate]  [Delete]                                         │
└────────────────────────────────────────────────────────────────────────┘
```

### 13.3 UDF Execution Stats (Tab in UDF Detail)

**Tabs:** Code | Parameters | Execution Stats

```
┌────────────────────────────────────────────────────────────────────────┐
│  Execution Stats — mask_pii                                            │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐              │
│  │ Total    │  │ Avg Exec │  │ Errors   │  │ Used By  │              │
│  │ Calls    │  │ Time     │  │ (24h)    │  │ Conns    │              │
│  │  12.4M   │  │  0.3ms   │  │   0      │  │   4      │              │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘              │
│                                                                        │
│  Execution Time Trend (last 7 days)                                    │
│  ┌────────────────────────────────────────────────────────────────┐   │
│  │  1ms│                                                          │   │
│  │     │                                                          │   │
│  │0.5ms│────────────────────────────────────────────              │   │
│  │     │                                                          │   │
│  │  0ms│──────────────────────────────────────────────────        │   │
│  └────────────────────────────────────────────────────────────────┘   │
│                                                                        │
│  Used in Connections:                                                  │
│  • ahli_pay_address_customer (step 4 in null_byte_strip pipeline)     │
│  • cim_accounts_sync (step 2 in pii_masking pipeline)                 │
│  • wps_kit_details (step 3 in cleanup pipeline)                       │
│  • alizz_customers (step 1 in masking pipeline)                       │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 14. Data Quality (`/data-quality`)

### 14.1 DQ Dashboard (`/data-quality`)

```
┌────────────────────────────────────────────────────────────────────────┐
│  Data Quality                                                          │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐              │
│  │  Overall │  │  Active  │  │  Open    │  │  Rules   │              │
│  │  Score   │  │  Policies│  │  Viol.   │  │  Passing │              │
│  │  98.5%   │  │   12     │  │   3      │  │  45/47   │              │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘              │
│                                                                        │
│  Quality by Connection                                                 │
│  ┌─────────────────────────────────┬───────┬────────┬─────────────┐   │
│  │ Connection                      │ Score │ Viol.  │ Last Check  │   │
│  ├─────────────────────────────────┼───────┼────────┼─────────────┤   │
│  │ ahli_pay_address_customer       │ 100%  │ 0      │ 5 min ago   │   │
│  │ cim_accounts_sync               │ 96.2% │ 2      │ 12 min ago  │   │
│  │ indusind_upi_iceberg            │ 99.1% │ 1      │ 3 min ago   │   │
│  └─────────────────────────────────┴───────┴────────┴─────────────┘   │
│                                                                        │
│  [View Policies]  [View Violations]  [Data Profiling]                  │
└────────────────────────────────────────────────────────────────────────┘
```

### 14.2 Create DQ Policy (`/data-quality/policies/new`)

```
┌────────────────────────────────────────────────────────────────────────┐
│  Create Data Quality Policy                                            │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  Policy Name *                                                         │
│  ┌────────────────────────────────────────────────────┐                │
│  │ ahli_pay_null_check_pkey                           │                │
│  └────────────────────────────────────────────────────┘                │
│                                                                        │
│  Connection *              Stream (Table) *                             │
│  ┌───────────────────┐    ┌───────────────────────────┐                │
│  │ahli_pay_address..▾│    │ address_customer        ▾│                │
│  └───────────────────┘    └───────────────────────────┘                │
│                                                                        │
│  Rule Type *                                                           │
│  ┌────────────────────────────────────────────────────┐                │
│  │ null_check                                       ▾│                │
│  └────────────────────────────────────────────────────┘                │
│  Available: null_ratio_check, range_check, regex_check,               │
│             freshness_check, row_count_match, enum_check,             │
│             custom_sql, uniqueness, referential_integrity              │
│                                                                        │
│  ── Rule Configuration ──                                              │
│                                                                        │
│  Target Columns *                                                      │
│  ┌────────────────────────────────────────────────────┐                │
│  │ [pkey] [×]  [changed] [×]  [+ Add column]         │                │
│  └────────────────────────────────────────────────────┘                │
│                                                                        │
│  Severity:  ○ Warning  ● Error  ○ Critical                            │
│                                                                        │
│  Action on Failure:                                                    │
│  ○ Log only  ○ Quarantine (send to DLQ)  ● Reject  ○ Alert            │
│                                                                        │
│  Threshold: [Allow up to __5__% violations before failing]             │
│                                                                        │
│  ── Schedule ──                                                        │
│  ○ Every batch (real-time)  ○ Cron: [*/15 * * * *]                    │
│                                                                        │
│  [Cancel]  [Test Rule]  [Create Policy]                                │
└────────────────────────────────────────────────────────────────────────┘
```

### 14.2b DQ Policy Detail (`/data-quality/policies/:id`)

**Tabs:** Overview | Execution Results | Configuration

**Overview tab:**
- Policy name, status (active/disabled), severity badge
- Linked connection + stream
- Rule type + configuration summary
- Quick stats: last run, pass rate, total violations

**Actions bar:** `[Execute Now]` `[Edit]` `[Disable]` `[Delete]`

**Execution Results tab:**
```
┌────────────────────────────────────────────────────────────────────────┐
│  Execution Results — ahli_pay_null_check_pkey                          │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  ┌──────────────────┬──────────┬──────────┬──────────┬──────────────┐ │
│  │ Run Time         │ Status   │ Checked  │ Failed   │ Action Taken │ │
│  ├──────────────────┼──────────┼──────────┼──────────┼──────────────┤ │
│  │ 10:25:00 UTC     │ ● Passed │ 5,000    │ 0        │ —            │ │
│  │ 10:20:00 UTC     │ ● Passed │ 3,200    │ 0        │ —            │ │
│  │ 10:15:00 UTC     │ ✗ Failed │ 4,100    │ 215      │ Rejected     │ │
│  │ 10:10:00 UTC     │ ● Passed │ 2,800    │ 12       │ Below thresh │ │
│  └──────────────────┴──────────┴──────────┴──────────┴──────────────┘ │
│                                                                        │
│  [Execute Now]  [Export Results]                                        │
└────────────────────────────────────────────────────────────────────────┘
```

### 14.3 Violations List & Detail

**List view:** Table with violation_id, policy, connection, detected_at, count, status (Active/Resolved/Ignored), actions

**Detail view:**
- Violation metadata (sample failing records)
- Resolution form (notes + action buttons: Resolve / Ignore)
- Timeline of status changes

---

## 15. Alerts & Notifications (`/alerts`)

### 15.1 Alerts List

```
┌────────────────────────────────────────────────────────────────────────┐
│  Alerts                          Summary: 🔴 2 Critical  🟡 3 Warning  │
├────────────────────────────────────────────────────────────────────────┤
│  Filters: [Severity ▾] [Status ▾] [Connection ▾]  🔍 Search...        │
│  Tab: [Active (5)] [Acknowledged (2)] [Resolved (45)]                  │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  ┌──────────────────────────────────────────────────────────────────┐ │
│  │ 🔴 CDC Lag Exceeded Threshold                                     │ │
│  │    Connection: cim_accounts_sync  │  Lag: 120s (threshold: 60s)  │ │
│  │    Triggered: 5 min ago  │  Rule: high_lag_alert                  │ │
│  │                                        [Acknowledge] [Resolve]    │ │
│  ├──────────────────────────────────────────────────────────────────┤ │
│  │ 🟡 DQ Violation Rate Above 5%                                     │ │
│  │    Connection: wps_kit_details  │  Violation: 7.2%                │ │
│  │    Triggered: 12 min ago  │  Rule: dq_threshold_alert             │ │
│  │                                        [Acknowledge] [Resolve]    │ │
│  └──────────────────────────────────────────────────────────────────┘ │
└────────────────────────────────────────────────────────────────────────┘
```

### 15.2 Alert Detail (`/alerts/:id`)

- Full alert info (title, message, context JSON)
- Timeline/history (triggered → acknowledged → resolved)
- Linked connection (clickable link)
- Related metrics at time of alert (lag chart, error chart)

### 15.3 Alert Rules (`/alerts/rules`)

```
┌────────────────────────────────────────────────────────────────────────┐
│  Alert Rules                                     [+ Create Rule]       │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  ┌──────────────────────┬───────────┬──────────┬─────────┬──────────┐ │
│  │ Rule Name            │ Type      │ Severity │ Scope   │ Status   │ │
│  ├──────────────────────┼───────────┼──────────┼─────────┼──────────┤ │
│  │ high_lag_alert       │ lag       │ Critical │ All conn│ ● Active │ │
│  │ worker_down          │ health    │ Critical │ All     │ ● Active │ │
│  │ dq_threshold         │ quality   │ Warning  │ Per conn│ ● Active │ │
│  │ error_rate_spike     │ error     │ Error    │ All conn│ ○ Muted  │ │
│  └──────────────────────┴───────────┴──────────┴─────────┴──────────┘ │
└────────────────────────────────────────────────────────────────────────┘
```

### 15.4 Create Alert Rule (`/alerts/rules/new`)

```
┌────────────────────────────────────────────────────────────────────────┐
│  Create Alert Rule                                                     │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  Rule Name *: ┌─────────────────────────────────────────┐              │
│               │ high_lag_critical                        │              │
│               └─────────────────────────────────────────┘              │
│                                                                        │
│  Alert Type:  [CDC Lag ▾]                                              │
│  Options: CDC Lag, Worker Health, DQ Violation, Error Rate,            │
│           Throughput Drop, Schema Change, Connection Status             │
│                                                                        │
│  Severity:    ○ Info  ○ Warning  ● Error  ○ Critical                  │
│                                                                        │
│  ── Scope ──                                                           │
│  Apply to: ● All connections  ○ Specific connection: [________]        │
│            ○ Specific source  ○ Specific destination                   │
│                                                                        │
│  ── Condition ──                                                       │
│  Metric:     [lag_seconds ▾]                                           │
│  Operator:   [> ▾]                                                     │
│  Threshold:  [60]                                                      │
│  For:        [5] consecutive minutes                                   │
│                                                                        │
│  ── Notification ──                                                    │
│  Send to channels: [☑ Teams Webhook] [☑ On-Call PagerDuty]            │
│  Cooldown: [30] minutes between re-fires                               │
│  Auto-resolve after: [15] minutes below threshold                      │
│                                                                        │
│  ── Escalation (optional) ──                                           │
│  After [30] min unresolved → escalate to: [☑ PagerDuty Critical]      │
│                                                                        │
│  [Cancel]  [Create Rule]                                               │
└────────────────────────────────────────────────────────────────────────┘
```

### 15.5 Notification Channels (`/alerts/channels`)

List + Create form:

```
┌────────────────────────────────────────────────────────────────┐
│  Create Notification Channel                                   │
│                                                                │
│  Channel Name: ┌──────────────────────────────────────┐        │
│                │ Fusion Teams - Ahli Pay                  │        │
│                └──────────────────────────────────────┘        │
│                                                                │
│  Type: [Microsoft Teams ▾]                                     │
│  Options: Email, Slack, Microsoft Teams, Webhook, PagerDuty    │
│                                                                │
│  ── Microsoft Teams Config ──                                  │
│  Webhook URL *:                                                │
│  ┌────────────────────────────────────────────────────┐        │
│  │ https://example.webhook.office.com/webhook... │        │
│  └────────────────────────────────────────────────────┘        │
│                                                                │
│  Rate limit: [20] per hour  │  [100] per day                  │
│                                                                │
│  [Cancel]  [Test Channel]  [Create]                            │
└────────────────────────────────────────────────────────────────┘
```

### 15.6 Alert Suppressions (`/alerts/suppressions`)

```
┌────────────────────────────────────────────────────────────────────────┐
│  Alert Suppressions                              [+ Create Suppression] │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  Active suppressions temporarily mute alerts during maintenance        │
│  or known-issue periods.                                               │
│                                                                        │
│  ┌──────────────────┬─────────────────┬────────────────┬────────────┐ │
│  │ Name             │ Scope           │ Window         │ Status     │ │
│  ├──────────────────┼─────────────────┼────────────────┼────────────┤ │
│  │ DB Migration     │ cim_accounts    │ 01:00-03:00 UTC│ ● Active   │ │
│  │ Weekend Quiet    │ All connections │ Sat-Sun        │ ⏳ Scheduled│ │
│  │ Testing Phase    │ alizz_customers │ Expired        │ ○ Expired  │ │
│  └──────────────────┴─────────────────┴────────────────┴────────────┘ │
└────────────────────────────────────────────────────────────────────────┘
```

**Create Suppression Form:**
```
┌────────────────────────────────────────────────────────────────┐
│  Create Alert Suppression                                      │
│                                                                │
│  Name *: ┌───────────────────────────────────────────┐         │
│           │ DB Migration - CIM                        │         │
│           └───────────────────────────────────────────┘         │
│                                                                │
│  Scope:                                                        │
│  ○ All alerts  ● Specific connection: [cim_accounts_sync ▾]   │
│  ○ Specific rule: [________]                                   │
│                                                                │
│  Suppress Severities: [☑ Warning] [☑ Error] [☐ Critical]      │
│                                                                │
│  Time Window:                                                  │
│  ○ One-time: From [2026-05-05 01:00] To [2026-05-05 03:00]   │
│  ● Recurring: [Every Sunday] [02:00] to [04:00] UTC            │
│                                                                │
│  Reason *:                                                     │
│  ┌────────────────────────────────────────────────────┐        │
│  │ Scheduled DB migration — expect 2h downtime        │        │
│  └────────────────────────────────────────────────────┘        │
│                                                                │
│  [Cancel]  [Create Suppression]                                │
└────────────────────────────────────────────────────────────────┘
```

### 15.7 Alert Rule Evaluations (Tab in Alert Rule Detail)

```
┌────────────────────────────────────────────────────────────────────────┐
│  high_lag_alert — Evaluations                                          │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  ┌──────────────────┬──────────────────┬──────────┬──────────────────┐│
│  │ Evaluated At     │ Connection       │ Result   │ Metric Value     ││
│  ├──────────────────┼──────────────────┼──────────┼──────────────────┤│
│  │ 10:25:00 UTC     │ cim_accounts     │ 🔴 FIRED │ lag: 120s (>60s) ││
│  │ 10:20:00 UTC     │ cim_accounts     │ 🔴 FIRED │ lag: 95s (>60s)  ││
│  │ 10:15:00 UTC     │ cim_accounts     │ ● OK     │ lag: 12s         ││
│  │ 10:25:00 UTC     │ ahli_pay_addr..  │ ● OK     │ lag: 1.2s        ││
│  │ 10:25:00 UTC     │ wps_kit_details  │ ● OK     │ lag: 0.8s        ││
│  └──────────────────┴──────────────────┴──────────┴──────────────────┘│
│                                                                        │
│  Evaluation Interval: every 5 minutes                                  │
│  Last evaluated: 30 seconds ago                                        │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 16. Schema Evolution (`/schema-evolution`)

### 16.1 Schema Changes Overview

```
┌────────────────────────────────────────────────────────────────────────┐
│  Schema Evolution                                                      │
├────────────────────────────────────────────────────────────────────────┤
│  Pending Approval: 2  │  Auto-Applied (24h): 5  │  Rejected: 0        │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  ┌─────┬──────────────────┬──────────────────┬───────┬──────┬───────┐ │
│  │ St  │ Connection       │ Change           │ Type  │Break?│Action │ │
│  ├─────┼──────────────────┼──────────────────┼───────┼──────┼───────┤ │
│  │ ⏳  │ cim_accounts     │ +email_verified  │ ADD   │ No   │[Rev.] │ │
│  │ ⏳  │ alizz_customers  │ ~phone: varchar  │ TYPE  │ Yes  │[Rev.] │ │
│  │ ✓   │ ahli_pay_kit     │ +metadata_v2     │ ADD   │ No   │ Auto  │ │
│  │ ✓   │ wps_orders       │ +shipping_addr   │ ADD   │ No   │ Auto  │ │
│  └─────┴──────────────────┴──────────────────┴───────┴──────┴───────┘ │
└────────────────────────────────────────────────────────────────────────┘
```

### 16.2 Schema Change Review Modal

```
┌────────────────────────────────────────────────────────────────┐
│  Review Schema Change                                          │
│  ─────────────────────────────────────────────────────────────│
│                                                                │
│  Connection: cim_accounts_sync                                 │
│  Table: accounts                                               │
│  Detected: 2026-05-01 08:23:00 UTC                             │
│  Type: COLUMN_ADDED                                            │
│  Breaking: No                                                  │
│                                                                │
│  Diff:                                                         │
│  ┌────────────────────────────────────────────────────┐        │
│  │  + email_verified  BOOLEAN  DEFAULT false          │        │
│  └────────────────────────────────────────────────────┘        │
│                                                                │
│  Impact Assessment:                                            │
│  • 0 transforms affected                                      │
│  • 0 DQ rules affected                                        │
│  • Destination table will get new nullable column              │
│                                                                │
│  Review Notes:                                                 │
│  ┌────────────────────────────────────────────────────┐        │
│  │ Approved — safe to add nullable boolean column     │        │
│  └────────────────────────────────────────────────────┘        │
│                                                                │
│  [Reject]                                [Approve & Apply]     │
└────────────────────────────────────────────────────────────────┘
```

---

## 17. Monitoring & Observability (`/monitoring`)

### 17.1 Monitoring Dashboard

```
┌────────────────────────────────────────────────────────────────────────┐
│  System Monitoring                                                     │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  System Health                                                         │
│  ┌────────────────────────────────────────────────────────────────┐   │
│  │  ● PostgreSQL: Healthy (response: 2ms)                         │   │
│  │  ● Redis: Healthy (response: 1ms, memory: 245MB/1GB)          │   │
│  │  ● Control Plane: Healthy (uptime: 14d 3h)                     │   │
│  │  ● Workers: 3/3 healthy                                        │   │
│  │  ● Spark Consumer: 2/2 healthy                                 │   │
│  └────────────────────────────────────────────────────────────────┘   │
│                                                                        │
│  Resource Usage                                                        │
│  ┌────────────────────────┐  ┌────────────────────────┐               │
│  │  CPU                   │  │  Memory                │               │
│  │  ████████░░░ 67%       │  │  ██████░░░░ 52%        │               │
│  │  workers: 45%          │  │  workers: 1.2GB        │               │
│  │  spark: 22%            │  │  spark: 800MB          │               │
│  └────────────────────────┘  └────────────────────────┘               │
│                                                                        │
│  Active Workers                                                        │
│  ┌────────────────┬────────────┬──────────┬────────┬─────────────────┐│
│  │ Worker ID      │ Type       │ Status   │ CPU    │ Connections     ││
│  ├────────────────┼────────────┼──────────┼────────┼─────────────────┤│
│  │ worker-abc123  │ mysql-cdc  │ ● Active │ 23%    │ 3 sources       ││
│  │ worker-def456  │ mongodb-cdc│ ● Active │ 15%    │ 2 sources       ││
│  │ worker-ghi789  │ mysql-cdc  │ ● Active │ 31%    │ 4 sources       ││
│  └────────────────┴────────────┴──────────┴────────┴─────────────────┘│
└────────────────────────────────────────────────────────────────────────┘
```

### 17.2 Connection Health Detail (`/monitoring/connections/:id`)

```
┌────────────────────────────────────────────────────────────────────────┐
│  Connection Health: ahli_pay_address_customer                          │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  CDC Lag (real-time)                                     [1h ▾]        │
│  ┌────────────────────────────────────────────────────────────────┐   │
│  │  2s │                                                          │   │
│  │     │      ╱╲                                                  │   │
│  │  1s │─────╱──╲──────────────────────────────────────────       │   │
│  │     │                                                          │   │
│  │  0s │──────────────────────────────────────────────────        │   │
│  │     └──────────────────────────────────────────────────────    │   │
│  │      10:00   10:15   10:30   10:45   11:00                     │   │
│  └────────────────────────────────────────────────────────────────┘   │
│                                                                        │
│  Throughput                                                            │
│  ┌────────────────────────────────────────────────────────────────┐   │
│  │  Events/sec: ████████████████ 4,200                            │   │
│  │  Bytes/sec:  ██████████░░░░░░ 2.1 MB                           │   │
│  └────────────────────────────────────────────────────────────────┘   │
│                                                                        │
│  Checkpoint State                                                      │
│  ┌────────────────────────────────────────────────────┐               │
│  │  Worker: worker-abc123                             │               │
│  │  Binlog file: mysql-bin.000042                     │               │
│  │  Binlog position: 154732                           │               │
│  │  GTID: 3E11FA47-71CA-11E1-9E33-C80AA9429562:1-42  │               │
│  │  Last checkpoint: 30 seconds ago                   │               │
│  └────────────────────────────────────────────────────┘               │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 18. Settings & Administration (`/settings`)

### 18.1 User Profile (`/settings/profile`)

- Edit first name, last name, email
- Avatar upload
- Preferences (theme: light/dark, timezone, notification preferences)

### 18.2 Change Password (`/settings/password`)

- Current password
- New password (with strength indicator)
- Confirm new password

### 18.3 User Management (`/settings/users`) — Admin Only

```
┌────────────────────────────────────────────────────────────────────────┐
│  User Management                                    [+ Invite User]    │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  ┌───────────────────┬──────────────────┬──────────┬──────┬─────────┐ │
│  │ User              │ Email            │ Role     │Active│ Actions │ │
│  ├───────────────────┼──────────────────┼──────────┼──────┼─────────┤ │
│  │ Rishikesh S.      │ rishi@example.com    │SuperAdmin│ ● Yes│ ⋯       │ │
│  │ Santhosh S.       │ santhosh@example.com │TenantAdm │ ● Yes│ ⋯       │ │
│  │ Ahmed K.          │ ahmed@example.com    │ Operator │ ● Yes│ ⋯       │ │
│  │ John D.           │ john@example.com     │ Viewer   │ ○ No │ ⋯       │ │
│  └───────────────────┴──────────────────┴──────────┴──────┴─────────┘ │
└────────────────────────────────────────────────────────────────────────┘
```

**Actions:** Edit roles, Deactivate, Reset password

### 18.4 Role Management (`/settings/roles`)

- List all roles with level, description, user count
- Create/edit role: name, level, assign permissions (checkboxes)
- Permission matrix (resources × actions)

### 18.5 Audit Logs (`/settings/audit-logs`)

```
┌────────────────────────────────────────────────────────────────────────┐
│  Audit Logs                                                            │
├────────────────────────────────────────────────────────────────────────┤
│  Filters: [User ▾] [Action ▾] [Resource ▾] [Date Range]              │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  ┌─────────────────┬──────────┬─────────────┬──────────────┬────────┐ │
│  │ Timestamp       │ User     │ Action      │ Resource     │ Status │ │
│  ├─────────────────┼──────────┼─────────────┼──────────────┼────────┤ │
│  │ 10:23:45 UTC    │ admin    │ CREATE      │ connection/  │ ✓ OK   │ │
│  │                 │          │             │ ahli_pay_kit │        │ │
│  │ 10:20:12 UTC    │ operator │ PAUSE       │ connection/  │ ✓ OK   │ │
│  │                 │          │             │ cim_accounts │        │ │
│  │ 10:15:00 UTC    │ admin    │ UPDATE      │ source/      │ ✓ OK   │ │
│  │                 │          │             │ ahli_mysql   │        │ │
│  └─────────────────┴──────────┴─────────────┴──────────────┴────────┘ │
│                                                                        │
│  [Export CSV]                                 [< 1  2  3 ... 50 >]    │
└────────────────────────────────────────────────────────────────────────┘
```

### 18.6 System Config (`/settings/system`) — Superadmin Only

```
┌────────────────────────────────────────────────────────────────────────┐
│  System Configuration                                                  │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  ┌──────────────────────────┬──────────────────────────┬────────────┐ │
│  │ Key                      │ Value                    │ Actions    │ │
│  ├──────────────────────────┼──────────────────────────┼────────────┤ │
│  │ max_connections_per_src  │ 10                       │ [Edit]     │ │
│  │ default_batch_size       │ 5000                     │ [Edit]     │ │
│  │ checkpoint_interval_sec  │ 30                       │ [Edit]     │ │
│  │ worker_heartbeat_ttl_sec │ 60                       │ [Edit]     │ │
│  │ max_lag_threshold_sec    │ 120                      │ [Edit]     │ │
│  │ dlq_retention_days       │ 30                       │ [Edit]     │ │
│  └──────────────────────────┴──────────────────────────┴────────────┘ │
│                                                                        │
│  [+ Add Config Key]                                                    │
└────────────────────────────────────────────────────────────────────────┘
```

### 18.7 Feature Flags (`/settings/feature-flags`) — Superadmin Only

```
┌────────────────────────────────────────────────────────────────────────┐
│  Feature Flags                                      [+ New Flag]       │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  ┌──────────────────────────┬────────┬───────────┬──────┬───────────┐ │
│  │ Flag                     │ Scope  │ Enabled   │ %    │ Actions   │ │
│  ├──────────────────────────┼────────┼───────────┼──────┼───────────┤ │
│  │ iceberg_writer_v2        │ Global │ [●═══○]   │ 100% │ [⋯]      │ │
│  │ graphql_subscriptions    │ Global │ [○═══●]   │  0%  │ [⋯]      │ │
│  │ scd2_postgres            │ Tenant │ [●═══○]   │ 60%  │ [⋯]      │ │
│  │ polling_connector        │ Global │ [●═══○]   │ 100% │ [⋯]      │ │
│  └──────────────────────────┴────────┴───────────┴──────┴───────────┘ │
│                                                                        │
│  Create Flag:                                                          │
│  Name: [________]  Scope: [Global ▾]  Rollout: [100]%                 │
│  Description: [________________________________________]               │
│  [Create]                                                              │
└────────────────────────────────────────────────────────────────────────┘
```

### 18.8 Maintenance Windows (`/settings/maintenance-windows`) — Superadmin Only

```
┌────────────────────────────────────────────────────────────────────────┐
│  Maintenance Windows                          [+ Schedule Maintenance]  │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  Active:  ● None currently                                             │
│  Upcoming: 1                                                           │
│                                                                        │
│  ┌────────────────┬─────────────────────┬──────────┬─────────────────┐│
│  │ Window         │ Time                │ Scope    │ Status          ││
│  ├────────────────┼─────────────────────┼──────────┼─────────────────┤│
│  │ Weekly Patch   │ Sun 02:00-04:00 UTC │ All      │ ⏳ Scheduled    ││
│  │ DB Migration   │ 2026-05-05 01:00    │ CIM only │ ⏳ Scheduled    ││
│  │ Infra Upgrade  │ 2026-04-28 03:00    │ All      │ ✓ Completed     ││
│  └────────────────┴─────────────────────┴──────────┴─────────────────┘│
│                                                                        │
│  During maintenance:                                                   │
│  • CDC pipelines auto-pause (with checkpoint)                          │
│  • Alerts suppressed                                                   │
│  • UI shows maintenance banner                                         │
└────────────────────────────────────────────────────────────────────────┘
```

### 18.9 Resource Quotas (`/settings/resource-quotas`) — Superadmin Only

```
┌────────────────────────────────────────────────────────────────────────┐
│  Resource Quotas & Usage                                               │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  Tenant Usage (today)                                                  │
│  ┌───────────────┬───────────┬──────────┬───────────┬────────────────┐│
│  │ Tenant        │ Events    │ Storage  │ Workers   │ Quota Status   ││
│  ├───────────────┼───────────┼──────────┼───────────┼────────────────┤│
│  │ Ahli Pay      │ 1.2M/5M  │ 45GB/100 │ 2/3       │ ● Within       ││
│  │ CIM           │ 3.8M/5M  │ 78GB/100 │ 3/3       │ ⚠ Near limit   ││
│  │ Alizz         │ 0.5M/5M  │ 12GB/100 │ 1/3       │ ● Within       ││
│  │ IndusInd      │ 5.1M/5M  │ 92GB/100 │ 3/3       │ 🔴 Exceeded    ││
│  └───────────────┴───────────┴──────────┴───────────┴────────────────┘│
│                                                                        │
│  Quota Violations (last 7 days)                                        │
│  ┌────────────────┬──────────────────┬────────────────┬──────────┐    │
│  │ Tenant         │ Resource         │ Limit          │ Actual   │    │
│  ├────────────────┼──────────────────┼────────────────┼──────────┤    │
│  │ IndusInd       │ events_per_day   │ 5,000,000      │ 5,124,003│    │
│  │ CIM            │ storage_gb       │ 100            │ 98.2     │    │
│  └────────────────┴──────────────────┴────────────────┴──────────┘    │
│                                                                        │
│  [Edit Quotas]  [View History]                                         │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 19. Dead Letter Queue (DLQ) (`/dlq`)

### 19.1 DLQ Dashboard

```
┌────────────────────────────────────────────────────────────────────────┐
│  Dead Letter Queue                                                     │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐              │
│  │  Total   │  │ Pending  │  │ Retried  │  │ Expired  │              │
│  │  Events  │  │  Retry   │  │ Success  │  │ (TTL)    │              │
│  │   142    │  │   23     │  │   98     │  │   21     │              │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘              │
│                                                                        │
│  By Connection                                                         │
│  ┌──────────────────────────┬────────┬────────┬──────────┬──────────┐ │
│  │ Connection               │ Failed │ Reason │ Oldest   │ Actions  │ │
│  ├──────────────────────────┼────────┼────────┼──────────┼──────────┤ │
│  │ cim_accounts_sync        │ 15     │ DQ     │ 2h ago   │ [View]   │ │
│  │ indusind_upi_iceberg     │ 5      │ Write  │ 30m ago  │ [View]   │ │
│  │ ahli_pay_address_customer│ 3      │ DQ     │ 1h ago   │ [View]   │ │
│  └──────────────────────────┴────────┴────────┴──────────┴──────────┘ │
│                                                                        │
│  [Retry All Pending]  [Purge Expired]                                  │
└────────────────────────────────────────────────────────────────────────┘
```

### 19.2 DLQ Events for Connection (`/dlq/:connectionId`)

```
┌────────────────────────────────────────────────────────────────────────┐
│  DLQ — cim_accounts_sync                          [Retry All] [Purge]  │
├────────────────────────────────────────────────────────────────────────┤
│  Filters: [Reason ▾] [Status ▾] [Date Range]                          │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  ┌──────┬───────────────┬──────────┬──────────┬────────┬────────────┐ │
│  │ ☐    │ Event ID      │ Table    │ Reason   │ Retries│ Failed At  │ │
│  ├──────┼───────────────┼──────────┼──────────┼────────┼────────────┤ │
│  │ ☐    │ evt_abc123    │ accounts │ DQ: null │ 0/3    │ 2h ago     │ │
│  │ ☐    │ evt_def456    │ accounts │ DQ: range│ 1/3    │ 1.5h ago   │ │
│  │ ☐    │ evt_ghi789    │ accounts │ Write err│ 2/3    │ 45m ago    │ │
│  └──────┴───────────────┴──────────┴──────────┴────────┴────────────┘ │
│                                                                        │
│  Selected: 0  │  [Retry Selected]  [Delete Selected]                   │
└────────────────────────────────────────────────────────────────────────┘
```

### 19.3 DLQ Event Detail (`/dlq/:connectionId/:eventId`)

```
┌────────────────────────────────────────────────────────────────────────┐
│  DLQ Event: evt_abc123                                                 │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  Connection: cim_accounts_sync                                         │
│  Table: accounts                                                       │
│  Operation: UPDATE                                                     │
│  Failed At: 2026-05-01 08:15:23 UTC                                    │
│  Reason: DQ Violation — null_check on column "email"                   │
│  Retry Count: 0 / 3                                                    │
│                                                                        │
│  ── Event Payload ──                                                   │
│  ┌────────────────────────────────────────────────────────────────┐   │
│  │ {                                                              │   │
│  │   "op": "u",                                                   │   │
│  │   "before": { "id": 42, "email": "test@x.com", ... },         │   │
│  │   "after": { "id": 42, "email": null, "name": "Ahmed" },      │   │
│  │   "ts_ms": 1714550123456,                                      │   │
│  │   "source": { "table": "accounts", "db": "CIM_PROD" }         │   │
│  │ }                                                              │   │
│  └────────────────────────────────────────────────────────────────┘   │
│                                                                        │
│  ── Retry History ──                                                   │
│  ┌──────────────────┬──────────┬──────────────────────────────────┐   │
│  │ Attempt          │ Status   │ Error                            │   │
│  ├──────────────────┼──────────┼──────────────────────────────────┤   │
│  │ (no retries yet) │          │                                  │   │
│  └──────────────────┴──────────┴──────────────────────────────────┘   │
│                                                                        │
│  [Retry Now]  [Edit & Retry]  [Delete]  [← Back to List]              │
└────────────────────────────────────────────────────────────────────────┘
```

**"Edit & Retry" flow:** Opens JSON editor to fix the event payload before retrying (e.g., fill in the null email).

---

## 20. Spark Jobs & Applications (`/spark-jobs`)

### 20.1 Spark Jobs List

```
┌────────────────────────────────────────────────────────────────────────┐
│  Spark Jobs                                                            │
├────────────────────────────────────────────────────────────────────────┤
│  Tabs: [Queue (5)] [Running (2)] [Completed (120)] [Failed (3)]        │
│  Filters: [Connection ▾] [Type ▾]  🔍 Search...                       │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  ┌──────────┬─────────────────────┬──────────┬────────┬──────────────┐│
│  │ Job ID   │ Connection          │ Type     │ Status │ Submitted    ││
│  ├──────────┼─────────────────────┼──────────┼────────┼──────────────┤│
│  │ job-001  │ ahli_pay_address..  │ Batch    │ ● Run  │ 5 min ago    ││
│  │ job-002  │ cim_accounts_sync   │ Backfill │ ● Run  │ 12 min ago   ││
│  │ job-003  │ wps_kit_details     │ Batch    │ ⏳Queue│ 1 min ago    ││
│  │ job-004  │ alizz_customers     │ Batch    │ ✗ Fail │ 1h ago       ││
│  └──────────┴─────────────────────┴──────────┴────────┴──────────────┘│
│                                                                        │
│  [Cancel Selected]                                                     │
└────────────────────────────────────────────────────────────────────────┘
```

### 20.2 Spark Job Detail (`/spark-jobs/:id`)

```
┌────────────────────────────────────────────────────────────────────────┐
│  Spark Job: job-001                              ● Running              │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  Connection: ahli_pay_address_customer                                 │
│  Type: Batch (scheduled)                                               │
│  Submitted: 2026-05-01 10:15:00 UTC                                    │
│  Started: 2026-05-01 10:15:02 UTC                                      │
│  Duration: 4m 32s (running)                                            │
│                                                                        │
│  ── Application Info ──                                                │
│  App ID: spark-app-abc123                                              │
│  Master: k8s://https://cluster.local                                   │
│  Driver Memory: 2GB  │  Executor Memory: 4GB                          │
│                                                                        │
│  ── Executors ──                                                       │
│  ┌────────────────┬──────────┬───────┬────────┬──────────────────────┐│
│  │ Executor ID    │ Status   │ CPU   │ Memory │ Tasks Completed      ││
│  ├────────────────┼──────────┼───────┼────────┼──────────────────────┤│
│  │ exec-0         │ ● Active │ 45%   │ 1.2GB  │ 234 / 500            ││
│  │ exec-1         │ ● Active │ 38%   │ 0.9GB  │ 212 / 500            ││
│  └────────────────┴──────────┴───────┴────────┴──────────────────────┘│
│                                                                        │
│  ── Progress ──                                                        │
│  Records processed: 45,231 / ~120,000                                  │
│  [████████████████░░░░░░░░░░░░░░] 38%                                  │
│                                                                        │
│  ── Logs (last 20 lines) ──                                            │
│  ┌────────────────────────────────────────────────────────────────┐   │
│  │ 10:19:32 INFO  Processing partition 3/8                        │   │
│  │ 10:19:31 INFO  Transform: null_byte_strip applied (2.1ms)     │   │
│  │ 10:19:30 INFO  DQ: null_check passed (0 violations)           │   │
│  │ 10:19:28 INFO  Read batch: 5000 events from Redis             │   │
│  └────────────────────────────────────────────────────────────────┘   │
│                                                                        │
│  [Cancel Job]  [View Full Logs]  [← Back]                              │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 21. JSON Flatten Rules (`/schema-evolution/:connectionId` — Tab)

Embedded in Schema Evolution per-connection view as a new tab: **Schema Changes | JSON Flatten Rules | JSON Schemas**

### 21.1 JSON Flatten Rules List

```
┌────────────────────────────────────────────────────────────────────────┐
│  JSON Flatten Rules — ahli_pay_address_customer     [+ Add Rule]       │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  These rules auto-extract nested JSON columns into flat destination    │
│  columns during CDC processing.                                        │
│                                                                        │
│  ┌────────────────┬──────────────┬─────────────────┬──────┬─────────┐ │
│  │ Source Column  │ JSON Path    │ Dest Column     │ Type │ Actions │ │
│  ├────────────────┼──────────────┼─────────────────┼──────┼─────────┤ │
│  │ metadata       │ $.address    │ meta_address    │ str  │ [⋯]    │ │
│  │ metadata       │ $.phone      │ meta_phone      │ str  │ [⋯]    │ │
│  │ extra_fields   │ $.kyc_status │ extra_kyc_stat  │ str  │ [⋯]    │ │
│  │ extra_fields   │ $.limit      │ extra_limit     │ int  │ [⋯]    │ │
│  └────────────────┴──────────────┴─────────────────┴──────┴─────────┘ │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

### 21.2 Create JSON Flatten Rule (modal)

```
┌────────────────────────────────────────────────────────────────┐
│  Add JSON Flatten Rule                                         │
│                                                                │
│  Source Column *  (JSON column in source table)                │
│  ┌────────────────────────────────────────────────────┐        │
│  │ metadata                                         ▾│        │
│  └────────────────────────────────────────────────────┘        │
│                                                                │
│  JSON Path *  (JSONPath expression)                            │
│  ┌────────────────────────────────────────────────────┐        │
│  │ $.address.city                                     │        │
│  └────────────────────────────────────────────────────┘        │
│                                                                │
│  Destination Column Name *                                     │
│  ┌────────────────────────────────────────────────────┐        │
│  │ meta_address_city                                  │        │
│  └────────────────────────────────────────────────────┘        │
│                                                                │
│  Cast Type:  [String ▾]                                        │
│  Options: String, Integer, Float, Boolean, Timestamp           │
│                                                                │
│  [Cancel]  [Preview (sample 5 rows)]  [Create Rule]            │
└────────────────────────────────────────────────────────────────┘
```

---

## 22. Shared Components Library

### Design System Components

| Component | Use Case | Props |
|-----------|----------|-------|
| `StatusBadge` | Everywhere | `status: 'active'|'paused'|'error'|'draft'|'inactive'`, `size: 'sm'|'md'` |
| `ConnectorIcon` | Source/Dest lists | `type: 'mysql'|'mongodb'|'postgres'|'iceberg'` |
| `StatCard` | Dashboard, detail pages | `title, value, trend, icon, color` |
| `DataTable` | All list views | `columns, data, sorting, filtering, pagination, rowActions` |
| `WizardStepper` | Create flows | `steps: [{title, component}], currentStep, onNext, onBack` |
| `SearchInput` | List filters | `placeholder, value, onChange, debounceMs` |
| `FilterBar` | Above tables | `filters: [{name, options, value}]` |
| `CodeEditor` | Transform/UDF editors | `language, value, onChange, readOnly, height` (Monaco Editor) |
| `JSONViewer` | Config displays | `data, collapsed, copyable` |
| `ConfirmDialog` | Delete/destructive actions | `title, message, confirmText, variant` |
| `EmptyState` | No-data scenarios | `icon, title, description, actionButton` |
| `LoadingSkeleton` | Loading states | `rows, columns, variant: 'table'|'card'|'text'` |
| `Breadcrumb` | Navigation | Auto-generated from route |
| `CommandPalette` | Global search (⌘K) | Search across all entities |
| `NotificationDropdown` | Top bar | Recent alerts with severity icons |
| `TenantSwitcher` | Sidebar (superadmin) | Bank/tenant dropdown |
| `TimeAgo` | Timestamps | `date → "5 min ago"` |
| `TrendIndicator` | Stat cards | `direction: 'up'|'down'|'neutral', percentage` |
| `SparklineChart` | Inline mini charts | `data: number[], color, height` |
| `PipelineDiagram` | Connection overview | Source → Pipeline → Destination visual |
| `DiffViewer` | Schema evolution | `old, new, language: 'sql'` |
| `PermissionGate` | Role-based UI hiding | `permission: string, children, fallback` |
| `FormField` | All forms | `label, error, required, helpText, children` |
| `PasswordInput` | Auth forms | `value, onChange, showStrength` |
| `TagInput` | Multi-value fields | `tags, onAdd, onRemove, suggestions` |
| `CronInput` | Schedule config | `value, onChange, presets: [{label, cron}]` |
| `DateRangePicker` | Monitoring, audit logs | `start, end, presets: ['1h','6h','24h','7d']` |

### Design Tokens (Tailwind)

```css
/* Status colors */
--status-active: #10b981;     /* green-500 */
--status-paused: #6b7280;     /* gray-500 */
--status-error: #ef4444;      /* red-500 */
--status-warning: #f59e0b;    /* amber-500 */
--status-draft: #8b5cf6;      /* violet-500 */

/* Severity colors */
--severity-critical: #dc2626; /* red-600 */
--severity-error: #ef4444;    /* red-500 */
--severity-warning: #f59e0b;  /* amber-500 */
--severity-info: #3b82f6;     /* blue-500 */

/* Background */
--bg-page: #f9fafb;           /* gray-50 */
--bg-card: #ffffff;
--bg-sidebar: #111827;        /* gray-900 */
--bg-sidebar-active: #1f2937; /* gray-800 */
```

---

## 23. User Flows (Step-by-Step)

### Flow 1: First-Time Setup (New Tenant Onboarding)

```
Login → Dashboard (empty state) → 
  "Set up your first pipeline" CTA →
  Create Source wizard (4 steps) →
  Create Destination wizard (3 steps) →
  Create Connection wizard (5 steps) →
  Connection activates → 
  Dashboard shows first live data
```

**Empty state on Dashboard:**
```
┌────────────────────────────────────────────────────────────────┐
│                                                                │
│              🚀 Welcome to Fusion                          │
│                                                                │
│      Set up your first CDC pipeline in minutes.                │
│                                                                │
│   1. Add a Source (MySQL, MongoDB, PostgreSQL)                 │
│   2. Add a Destination (PostgreSQL, Iceberg)                   │
│   3. Create a Connection to start syncing                      │
│                                                                │
│              [Get Started — Add Source →]                       │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

---

### Flow 2: Create Full Pipeline (experienced user)

```
Sources → New Source → Step 1 (select MySQL) → Step 2 (fill config) → 
  Step 3 (test → all green) → Step 4 (select tables) → [Create]
  
Destinations → New Destination → Step 1 (select PG) → Step 2 (config) →
  Step 3 (test → green) → [Create]

Connections → New Connection → 
  Step 1 (select source) → Step 2 (select destination) → 
  Step 3 (configure streams/tables) → Step 4 (sync mode, transforms, DQ) →
  Step 5 (review) → [Create & Activate]
  
Connection activates → real-time events flowing
```

---

### Flow 3: Monitor & Troubleshoot

```
Dashboard → see "CDC Lag" card is red (> 60s) →
  Click lag metric → /monitoring/connections/:id →
  See lag chart spike → Check worker status →
  Worker memory at 95% → Click connection name →
  Connection detail → [Pause] → investigate →
  Fix config → [Resume] → Lag drops back to normal
```

---

### Flow 4: Handle Schema Change (Manual Approval mode)

```
Dashboard → Alert badge (1 pending schema change) →
  /schema-evolution → See pending change (COLUMN_ADDED) →
  Click [Review] → Modal with diff →
  See impact assessment → Type review notes →
  [Approve & Apply] → Change applied to destination →
  Alert clears
```

---

### Flow 5: Data Quality Violation Response

```
Alerts feed → 🟡 DQ Violation alert →
  Click → /data-quality/violations/:id →
  See failing records sample → 
  Determine it's bad source data →
  [Resolve] with notes → 
  Navigate to DQ Policy → 
  Adjust threshold → [Save]
```

---

### Flow 6: Connection Lifecycle

```
Create (draft) → Test Connection → Activate →
  Running (events flowing) →
  [Pause] (maintenance) → Paused →
  [Resume] → Running again →
  [Trigger Manual Sync] (backfill) →
  Sync run completes → 
  [Delete] → Confirm dialog → Soft deleted
```

---

### Flow 7: Alert Rule Creation

```
/alerts/rules → [+ Create Rule] → 
  Fill: name, type (CDC Lag), severity, scope →
  Configure condition: lag > 60s for 5 min →
  Select channels: Teams + PagerDuty →
  Set cooldown: 30 min →
  Add escalation: after 30 min → PagerDuty Critical →
  [Create Rule] → 
  Rule active → will fire when condition met
```

---

## 24. Responsive & Accessibility Requirements

### Breakpoints

| Breakpoint | Width | Layout changes |
|-----------|-------|---------------|
| Desktop | ≥1280px | Full sidebar + content |
| Tablet | 768-1279px | Collapsed sidebar (icons only) |
| Mobile | <768px | Bottom nav, stacked cards |

### Accessibility (WCAG 2.1 AA)

- All interactive elements: keyboard navigable (Tab/Enter/Escape)
- Color contrast ratio ≥ 4.5:1 for text
- Status indicators: never color-only (always icon + text)
- Screen reader labels on all icons and buttons
- Focus rings visible on all interactive elements
- Form errors linked to fields via `aria-describedby`
- Tables: proper `<th>` with `scope`, `aria-sort` on sortable columns
- Modals: trap focus, close on Escape
- Loading states: `aria-live="polite"` for dynamic content

### Performance Targets

- First Contentful Paint: < 1.5s
- Time to Interactive: < 3s
- List views: virtual scrolling for > 100 items
- Charts: lazy-loaded (only render when visible)
- API responses: skeleton loading placeholders

---

## 25. API Integration Map

### State Management Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  Zustand Stores                                                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  authStore                    uiStore                               │
│  ├─ user                      ├─ sidebarCollapsed                  │
│  ├─ tokens                    ├─ theme (light/dark)                │
│  ├─ isAuthenticated           ├─ commandPaletteOpen                │
│  ├─ permissions[]             └─ notifications[]                   │
│  ├─ login()                                                        │
│  ├─ logout()                                                       │
│  └─ refreshToken()                                                 │
│                                                                     │
├─────────────────────────────────────────────────────────────────────┤
│  TanStack Query Keys (server state)                                 │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ['sources']                   → GET /sources                       │
│  ['sources', id]               → GET /sources/:id                   │
│  ['sources', id, 'stats']      → GET /sources/:id/stats             │
│  ['destinations']              → GET /destinations                   │
│  ['destinations', id]          → GET /destinations/:id              │
│  ['connections']               → GET /connections                    │
│  ['connections', id]           → GET /connections/:id               │
│  ['connections', id, 'stats']  → GET /connections/:id/stats         │
│  ['connections', id, 'streams']→ GET /streams/connections/:id/...   │
│  ['connections', id, 'health'] → GET /monitoring/connections/:id/..  │
│  ['connectors']                → GET /connector-definitions          │
│  ['transforms']                → GET /transformations               │
│  ['udfs']                      → GET /udfs                          │
│  ['dq-policies']               → GET /data-quality/policies         │
│  ['dq-violations']             → GET /data-quality/violations       │
│  ['dq-dashboard']              → GET /data-quality/metrics/dashboard│
│  ['alerts']                    → GET /alerts                        │
│  ['alert-rules']               → GET /alerts/rules                  │
│  ['alert-channels']            → GET /alerts/channels               │
│  ['alert-suppressions']        → GET /alerts/suppressions           │
│  ['schema-changes', connId]    → GET /schema-evolution/conn/:id/... │
│  ['json-flatten-rules', connId]→ GET /schema-evolution/conn/:id/json-flatten-rules │
│  ['monitoring-health']         → GET /monitoring/health             │
│  ['monitoring-workers']        → GET /monitoring/workers            │
│  ['dlq-events']                → GET (DLQ listing endpoint)         │
│  ['dlq-events', connId]        → GET (DLQ per-connection)           │
│  ['spark-jobs']                → GET (SparkJobQueue listing)        │
│  ['spark-jobs', id]            → GET (SparkApplication detail)      │
│  ['audit-logs']                → GET (audit log endpoint)           │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### GraphQL API (Alternative)

The backend also exposes a **Strawberry GraphQL** endpoint at `/graphql` that supports:
- **Queries:** sources, destinations, connections, streams, dq_policies (with filtering/pagination)
- **Mutations:** create/update/delete for all entities
- **Subscriptions:** (future) real-time alert and metric pushes

The frontend can use GraphQL as an **alternative** to REST for complex views that need data from multiple entities in a single request (e.g., Dashboard that needs connections + alerts + health in one round-trip).

```typescript
// Example: TanStack Query with GraphQL
const { data } = useQuery({
  queryKey: ['dashboard-overview'],
  queryFn: () => graphqlClient.request(DASHBOARD_QUERY),
});
```

### API Client Configuration

```typescript
// src/lib/api.ts
const api = axios.create({
  baseURL: import.meta.env.VITE_API_URL || 'http://localhost:8000/api/v1',
  headers: { 'Content-Type': 'application/json' },
});

// Request interceptor: attach Bearer token
// Response interceptor: 401 → refresh token flow → retry
// Response interceptor: 403 → redirect to dashboard with toast
```

### Real-Time Updates (WebSocket / SSE)

For live metrics (lag, throughput, alerts):
- **Option A:** Polling via TanStack Query `refetchInterval: 5000` for monitoring pages
- **Option B:** Server-Sent Events for alerts (recommended)
- Connection health dashboard: poll every 5s when page is visible

---

## Appendix: Page → Component → API Summary Table

| Page | Key Components | Create/Mutate APIs | Read APIs |
|------|---------------|-------------------|-----------|
| Login | LoginForm, PasswordInput | POST /auth/login | — |
| Register | RegisterForm | POST /auth/register | — |
| Dashboard | StatCard×4, PipelineBar, ConnectionsMini, AlertsFeed, ThroughputChart, SystemHealth | — | /monitoring/health, /alerts/summary, /connections, /dq/metrics/dashboard |
| Connectors List | DataTable, FilterBar, ConnectorIcon | — | GET /connector-definitions |
| Connector Detail | Tabs, JSONViewer, VersionTable | — | GET /connector-definitions/:id |
| Sources List | DataTable, StatusBadge, FilterBar | — | GET /sources |
| Create Source | WizardStepper, ConnectorCard, FormFields, TestProgress, TableSelector | POST /sources, POST /sources/:id/test-connection, POST /sources/:id/discover-schemas | GET /connector-definitions?category=source |
| Source Detail | Tabs, SchemaTree, CDCConfigPanel, StatsCharts | — | GET /sources/:id, /stats, /cdc-config |
| Destinations List | DataTable, StatusBadge | — | GET /destinations |
| Create Destination | WizardStepper, FormFields, TestProgress | POST /destinations, POST /:id/test-connection | GET /connector-definitions?category=destination |
| Destination Detail | Tabs, WriteModeConfig, BatchSettings | — | GET /destinations/:id, /write-mode, /batch-settings, /stats |
| Connections List | ConnectionCard, FilterBar, InlineActions | POST /connections/:id/pause, /resume, /trigger-sync | GET /connections |
| Create Connection | WizardStepper×5, SourceSelector, DestSelector, StreamConfig, PipelineConfig, ReviewPanel | POST /connections/validate, POST /connections, POST /:id/streams, POST /:id/schedule, POST /:id/activate | GET /sources, /destinations |
| Connection Detail | Tabs, PipelineDiagram, StatCard, StreamsTable, RunsTable, HealthCharts | POST /pause, /resume, /trigger-sync | GET /:id, /stats, /schedule, /streams, /health |
| Transforms List | DataTable | — | GET /transformations |
| Transform Editor | CodeEditor (Monaco), PreviewPanel, ValidationStatus | PUT /transformations/:id, POST /:id/validate, POST /:id/preview | GET /transformations/:id |
| UDF List | DataTable | — | GET /udfs |
| UDF Editor | CodeEditor, ParameterTable | PATCH /udfs/:id | GET /udfs/:id |
| DQ Dashboard | StatCard×4, QualityTable | — | GET /dq/metrics/dashboard |
| DQ Policies | DataTable, CreatePolicyForm | POST /dq/policies, PATCH, DELETE | GET /dq/policies |
| DQ Violations | DataTable, ResolutionForm | POST /:id/resolve | GET /dq/violations |
| DQ Policy Detail | Tabs, ExecutionResultsTable, StatCards | POST /:id/execute | GET /dq/policies/:id, /:id/results |
| Alerts List | AlertCard, FilterBar, Tabs | POST /:id/acknowledge, /:id/resolve | GET /alerts, /summary |
| Alert Detail | AlertTimeline, MetricsSnapshot | POST /acknowledge, /resolve | GET /alerts/:id, /:id/history |
| Alert Rules | DataTable, CreateRuleForm, EvaluationsTab | POST /alerts/rules, PATCH, DELETE | GET /alerts/rules, /:id/evaluations |
| Channels | DataTable, CreateChannelForm, TestButton | POST /alerts/channels, /:id/test | GET /alerts/channels |
| Alert Suppressions | DataTable, CreateSuppressionForm | POST /alerts/suppressions | GET /alerts/suppressions |
| Schema Evolution | ChangeTable, ReviewModal, DiffViewer, FlattenRulesTab | POST /approve, /reject, /json-flatten-rules | GET /schema-changes, /json-flatten-rules, /json-schemas |
| Monitoring | SystemHealth, ResourceGauges, WorkerTable | — | GET /monitoring/health, /workers, /resource-usage |
| Connection Health | LagChart, ThroughputChart, CheckpointPanel | — | GET /monitoring/connections/:id/* |
| DLQ Dashboard | StatCard×4, ConnectionDLQTable | POST /retry-all, /purge | GET /dlq (listing) |
| DLQ Event Detail | JSONViewer, RetryHistoryTable | POST /retry, /edit-retry, /delete | GET /dlq/:connId/:eventId |
| Spark Jobs | DataTable, Tabs, ProgressBar | POST /cancel | GET /spark-jobs |
| Spark Job Detail | ExecutorTable, LogViewer, ProgressBar | POST /cancel | GET /spark-jobs/:id, /executors |
| User Profile | ProfileForm | PATCH /auth/me | GET /auth/me |
| Change Password | PasswordChangeForm | POST /auth/change-password | — |
| User Management | DataTable, RoleAssign | admin APIs | admin APIs |
| Roles | RoleTable, PermissionMatrix | admin APIs | admin APIs |
| Audit Logs | DataTable, DateRangePicker, ExportButton | — | GET audit-logs |
| System Config | KeyValueTable, EditModal | PUT /system-config | GET /system-config |
| Feature Flags | FlagTable, ToggleSwitch, RolloutSlider | POST/PATCH /feature-flags | GET /feature-flags |
| Maintenance Windows | ScheduleTable, CreateForm | POST /maintenance-windows | GET /maintenance-windows |
| Resource Quotas | TenantUsageTable, QuotaViolations, EditQuotaForm | PUT /resource-quotas | GET /resource-quotas, /tenant-usage |

---

*End of UI/UX Design Specification*  
*Total screens: 56 | Total unique components: 30 | Total API integrations: 110+*
