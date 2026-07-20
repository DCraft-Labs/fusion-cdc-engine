# User Management Deep Dive

## 1. Purpose

This document is the authoritative reference for how user management works across the Yap Web platform. It documents:

- The end-to-end flow from self-service registration to onboarding and deactivation.
- Platform roles, product roles, and permission scopes, including how they are stored and enforced.
- All data models, database tables, and relationships involved in user lifecycle management.
- Keycloak integration details and hybrid responsibilities between the identity provider and the application database.
- REST APIs, DTOs, and service orchestration for super administrators, bank administrators, and end users.
- Frontend components that deliver user management capabilities and how they consume the backend surface.
- Edge cases, safeguards, and known gaps to guide future development.

The material here reflects the source of truth found in the repository as of 29 November 2025. When code diverges, update this document.

---

## 2. System Overview

### 2.1 Actors & Personas

| Actor | Description | Typical Tasks |
| --- | --- | --- |
| **Super Administrator** | Platform operator with visibility across all banks. Maps to `SUPERADMIN` realm role in Keycloak and `User.UserRole.SUPERADMIN` in the database. | Approve registrations, create/edit/delete any user, assign banks, sub-tenants, and product roles. |
| **Bank Administrator** | Operates within a single bank. Keycloak realm role `ADMIN`. | Manage users for their bank, including creation and status toggles; cannot cross bank boundaries. |
| **Viewer / Product User** | End user consuming licensed products. Keycloak realm role `VIEWER` (or legacy roles). | Access products based on product-role assignments. Cannot manage other users. |
| **Pending User** | Registration request awaiting approval. Enrolled in `pending_users` table until approved or rejected. | Provide justification, wait for onboarding. |

### 2.2 High-Level Architecture

1. **Keycloak (Identity Provider)**
   - Stores master credentials, realm roles (`superadmin`, `admin`, `viewer`, etc.), and issues JWTs.
   - Exposed through Keycloak Admin REST API for user provisioning.
2. **Backend (`fusion-backend/`)**
   - Spring Boot application providing REST APIs.
   - Persists business state (banks, sub-tenants, product roles, user assignments) in PostgreSQL via JPA entities.
   - Manages hybrid logic (Keycloak + database) through services like `ApprovalService` and `UserManagementService`.
3. **Frontend (`src/`)**
   - React + TypeScript application bootstrapped by Vite.
   - Retrieves Keycloak tokens via `KeycloakContext` and maps them to UI roles via `RoleContext`.
   - Pages such as `src/pages/UserManagement.tsx` and `src/pages/AdminApprovals.tsx` orchestrate UI workflows.

### 2.3 Hybrid Responsibility Model

| Capability | Keycloak | Application Database |
| --- | --- | --- |
| User identity & authentication | ✅ | ❌ |
| Realm role (SUPERADMIN/ADMIN/VIEWER) | ✅ | ✅ (mirrored in `users.role`) |
| Bank membership | ❌ | ✅ (`user_banks` table) |
| Sub-tenant membership | ❌ | ✅ (`user_sub_tenants` table) |
| Product access & role permissions | ❌ | ✅ (`user_products` coupled with `product_roles` & `product_role_permissions`) |
| Registration workflow state | ❌ | ✅ (`pending_users` table) |

---

## 3. Data Flow Narratives

### 3.1 Self-Service Registration → Approval

1. **Registration Submission**
   - Frontend page `src/pages/Registration.tsx` (re-exported by `src/pages/Register.tsx`) sends a POST to `/api/v1/public/register` (not shown here) to create a `PendingUser` entry.
   - `PendingUser` entity (see `fusion-backend/.../entity/PendingUser.java`) stores email, requested bank, requested role, and justification. Status defaults to `PENDING`.

2. **Review Queue**
   - `src/pages/AdminApprovals.tsx` fetches pending users via `usePendingUsers` hook (wraps `GET /api/v1/admin/approvals/pending`).
   - Superadmins can inspect justification, requested role, and bank context.

3. **Approval Path**
   - Upon approval, `ApprovalService.approveAndCreateUser` (`fusion-backend/.../service/ApprovalService.java`) executes:
     - Validates at least one bank, sub-tenant, and product assignment.
     - Calls Keycloak Admin API to create a new user (`createKeycloakUser`).
     - Persists `User`, `UserBank`, `UserSubTenant`, `UserProduct` records.
     - Marks the `PendingUser` as `APPROVED` with reviewer metadata.

4. **Rejection Path**
   - As of the latest fix, `rejectUser` deletes the pending entry after logging the rejection reason, returning a `REJECTED` status response.

### 3.2 Direct Provisioning (Platform Ops)

- Superadmins can bypass the approval queue using `POST /api/v1/superadmin/users` (`UserManagementController`).
- Flow mirrors approval but uses `UserManagementService.createUser` which:
  - Creates the Keycloak user with email verification flagged true.
  - Assigns Keycloak realm roles aligned with the `role` property.
  - Persists relational links to banks, sub-tenants, and optionally products/roles.

### 3.3 Bank Admin Management

- Bank admins operate against `/api/v1/admin/users` endpoints (`AdminUserManagementController`).
- JWT must contain `bank_id`; controllers enforce that all operations stay within that bank.
- CRUD operations rely on `UserManagementService` for persistence but always validate bank scoping.

### 3.4 Self Profile Lookup

- Any authenticated user can call `GET /api/v1/users/me` (`UserController`).
- `UserService.getUserProfile` resolves the identity by Keycloak ID (or email fallback) and returns aggregated bank/product assignments plus permissions.

---

## 4. Domain Model & Database Schema

Entities live under `fusion-backend/src/main/java/com/fusion/entity`. All tables use UUID primary keys.

### 4.1 Core Tables

| Table | Key Columns | Description |
| --- | --- | --- |
| `users` | `id` (PK), `keycloak_id`, `email`, `first_name`, `last_name`, `role`, `status`, timestamps | Mirrors Keycloak users and stores platform-specific metadata. `role` is an enum of `User.UserRole`. |
| `user_banks` | `id`, `user_id`, `bank_id`, `assigned_at`, `assigned_by` | Junction table linking users to banks. Unique constraint on `(user_id, bank_id)`. |
| `user_sub_tenants` | `id`, `user_id`, `sub_tenant_id`, `assigned_at`, `assigned_by` | Junction table for sub-tenant access. Unique on `(user_id, sub_tenant_id)`. |
| `user_products` | `id`, `user_id`, `bank_id`, `product_id`, `role_id`, `assigned_at`, `assigned_by` | Captures product access for a user within a bank, including the product role granted. Unique on `(user_id, bank_id, product_id)`. |
| `product_roles` | `id`, `product_id`, `role_name`, `description`, `is_default`, timestamps | Defines product-level roles (e.g., "Report Downloader"). Each role owns a set of permissions. |
| `product_role_permissions` | `id`, `role_id`, `permission` | Enumerated set of product-level permissions (`CREATE`, `READ`, `UPDATE`, `DELETE`, `EXECUTE`, `DOWNLOAD`, `SCHEDULE`, `MANAGE`). Unique per `(role_id, permission)`. |
| `banks` | `id`, `name`, `code`, `logo_url`, `status`, timestamps | Master record for each bank. |
| `sub_tenants` | `id`, `bank_id`, `name`, `code`, `database_name`, `database_host`, `database_port`, `status`, timestamps | Defines each bank's sub-tenant (a.k.a. branch or tenant). Unique `(bank_id, code)` and unique `database_name`. |
| `products` | `id`, `name`, `code`, `icon`, `route`, etc. | Defines products that can be licensed. (See `Product.java` for full schema.) |
| `pending_users` | `id`, `email`, `first_name`, `last_name`, `requested_bank_id`, `requested_role`, `justification`, `status`, `requested_at`, `reviewed_at`, `reviewed_by`, `rejection_reason` | Stores registration requests pending approval. |

### 4.2 Supporting Tables

| Table | Key Columns | Purpose |
| --- | --- | --- |
| `bank_licenses` | `id`, `bank_id`, `product_id`, `status`, `expires_at` | Defines which products are licensed for a bank. Used by approvals to limit product assignment. |
| `product_role_permissions` | See above | Enables fine-grained control per product role. |

### 4.3 Relationships

- `User` 1—N `UserBank`, `UserSubTenant`, `UserProduct`.
- `UserProduct` references `ProductRole`, enabling default role/permission bundling.
- `PendingUser.reviewedBy` optionally references a `User` (the reviewer).
- `SubTenant.bank` ensures sub-tenants are scoped to banks; `ApprovalService` validates assignments accordingly.

### 4.4 Column Reference (Selected Tables)

#### `users`

| Column | Type | Notes |
| --- | --- | --- |
| `id` | UUID | Primary key, generated (`@GeneratedValue(UUID)`). |
| `keycloak_id` | VARCHAR(100) | Unique; stored to link with Keycloak subject. |
| `email` | VARCHAR(255) | Unique; also used for fallback lookup. |
| `role` | ENUM | `SUPERADMIN`, `ADMIN`, `VIEWER`, plus legacy roles (MANAGER, REPORT_ADMIN, etc.). |
| `status` | ENUM | `PENDING`, `ACTIVE`, `INACTIVE`, `SUSPENDED`. Defaults to `PENDING`. |
| `last_login` | TIMESTAMP | Optional audit field. |
| `created_at`, `updated_at` | TIMESTAMP | Managed by Spring Data auditing. |

#### `user_products`

| Column | Type | Notes |
| --- | --- | --- |
| `user_id` | UUID (FK) | References `users.id`. |
| `bank_id` | UUID (FK) | References `banks.id`. |
| `product_id` | UUID (FK) | References `products.id`. |
| `role_id` | UUID (FK) | References `product_roles.id`. Required. |
| Unique Constraint | `(user_id, bank_id, product_id)` | Prevents duplicate assignments of same product for a bank. |

#### `pending_users`

| Column | Type | Notes |
| --- | --- | --- |
| `requested_bank_id` | UUID (FK) | Requested bank for access. |
| `requested_role` | ENUM | `User.UserRole`. |
| `status` | ENUM | `PENDING`, `APPROVED`, `REJECTED`. |
| `rejection_reason` | TEXT | Saved when rejected; entry deleted post rejection in latest implementation. |

(See entity classes for full field list.)

---

## 5. Backend APIs & Services

### 5.1 Approvals (`/api/v1/admin/approvals`)

| Endpoint | Method | Role Guard | Description | References |
| --- | --- | --- | --- | --- |
| `/pending` | GET | `SUPERADMIN` | List all pending registrations. | `ApprovalController.getPendingUsers` |
| `/pending/paginated` | GET | `SUPERADMIN` | Paginated pending list. | `ApprovalController.getPendingUsersPaginated` |
| `/pending/bank/{bankId}` | GET | `SUPERADMIN` | Pending users for a bank. | `ApprovalController.getPendingUsersByBank` |
| `/{pendingUserId}` | POST | `SUPERADMIN` | Approve or reject a request. | `ApprovalController.processApproval`, `ApprovalService.processApproval` |
| `/pending/{pendingUserId}` | DELETE | `SUPERADMIN` | Hard delete pending entry. | `ApprovalController.deletePendingUser` |

**Approval Request Payload (`ApprovalRequest`)**

```json
{
  "approved": true,
  "bankIds": ["uuid"],
  "subTenantIds": ["uuid"],
  "productIds": ["uuid"],
  "productPermissions": [
    {
      "productId": "uuid",
      "permissions": ["CREATE", "READ"]
    }
  ]
}
```

Notes:
- `pendingUserId` is derived from the path (no longer required in payload).
- On rejection, send `{ "approved": false, "rejectionReason": "..." }`.
- Rejections now delete the `pending_users` row to avoid clutter.

### 5.2 Superadmin User Management (`/api/v1/superadmin/users`)

| Endpoint | Method | Description |
| --- | --- | --- |
| `/` | GET | Filterable list (by role/status/bank). |
| `/` | POST | Create user with banks, sub-tenants, optional product assignments. |
| `/{id}` | GET | Detailed view. |
| `/{id}` | PUT | Update user (basic info + assignments). |
| `/{id}/activate` | POST | Set status to `ACTIVE`. |
| `/{id}/deactivate` | POST | Set status to `INACTIVE`. |
| `/{id}` | DELETE | Soft delete (status → INACTIVE). |

All routes call `UserManagementService`.

### 5.3 Bank Admin User Management (`/api/v1/admin/users`)

- Identical surface but operations are scoped to the admin's `bank_id` claim.
- `AdminUserManagementController` rejects cross-bank operations and ensures new assignments only use that bank.

### 5.4 User Self Profile (`/api/v1/users/me`)

- Returns `UserProfileResponse` with aggregated banks, products, role names, and permission strings. Useful for UI gating.

### 5.5 DTO Reference

| DTO | Location | Purpose |
| --- | --- | --- |
| `UserCreateRequest` | `fusion-backend/.../dto` | Payload for user creation (admin/superadmin). |
| `UserUpdateRequest` | DTO | Used for PUT updates. |
| `UserDetailResponse` | DTO | Rich response for user detail operations. |
| `ProductAssignment` | DTO | `bankId` + `productId` + `roleId` triple for product access. |
| `ApprovalRequest` / `ApprovalResponse` | DTO | For approval processing. |

---

## 6. Key Services & Business Logic

### 6.1 `ApprovalService`

- **Responsibility**: Orchestrates approval pipeline.
- **approveUser**: Accepts an `ApprovalRequest`, loads `PendingUser`, routes to `approveAndCreateUser` or `rejectUser`.
- **approveAndCreateUser**:
  - Validates banks, sub-tenants, and products are present.
  - Validates sub-tenants belong to the banks assigned (`validateSubTenantsAssignment`).
  - Creates Keycloak user (default required actions `VERIFY_EMAIL`, `CONFIGURE_TOTP`).
  - Persists `User`, `UserBank`, `UserSubTenant`, `UserProduct` linkages.
  - Marks `PendingUser` as `APPROVED` with reviewer metadata.
- **rejectUser**:
  - Requires `rejectionReason`.
  - Deletes the pending record (post-Nov 2025 change).
  - Returns rejection response for UI feedback.

### 6.2 `UserManagementService`

- **getAllUsers**: Combines repository fetch with manual filtering (status, role). When `bankId` filter is used, fetches via `userRepository.findByBankId`.
- **createUser**: Bypasses approval to create Keycloak user and DB records. Assigns Keycloak realm role corresponding to requested role (lowercase).
- **updateUser**: Handles updates to personal info and associations. Uses helper methods to wipe and rebuild junction tables per request.
- **toggleUserStatus**, **activateUser**, **deactivateUser**: Modify `User.status`. TODO comments indicate Keycloak status sync is pending.

### 6.3 Auxiliary Services

- `UserService`: Consolidates profile data for `/users/me` by merging `User`, `UserBank`, `UserProduct`, and product role permissions.

### 6.4 Product Role Mapping & Permissions

- `ProductRoleService` owns lifecycle operations for product-scoped roles. During creation (`createRole`) it validates requested permissions against `ProductRolePermission.PermissionType`, populates child rows, and marks one role per product as `isDefault` when requested.
- `UserProduct` assignments reference a `ProductRole` record, so the permission list a user receives is always the permissions defined on the linked role at assignment time. Updates to a role cascade because the service rebuilds the role’s permission set atomically.
- `ProductRolePermission` enumerates exactly eight permission variants; table below captures their intent for onboarding conversations and UI copy alignment.

| Permission | Purpose |
| --- | --- |
| `CREATE` | Author or configure new assets (reports, templates, ETL jobs). |
| `READ` | View generated assets and metadata; baseline visibility. |
| `UPDATE` | Modify existing configurations or saved assets without recreating them. |
| `DELETE` | Remove assets or configurations from the tenant product space. |
| `EXECUTE` | Run on-demand workloads such as ad-hoc reports or scripts. |
| `DOWNLOAD` | Export results, files, or generated artifacts to external storage. |
| `SCHEDULE` | Manage recurring jobs, including edits to cadence or recipients. |
| `MANAGE` | Full administrative control: combine all actions plus governance tasks like assigning product roles to other users. |

- When surfacing roles in the UI, always echo the sorted permission list supplied by `ProductRoleResponse`. This keeps the frontend aligned with backend truth even if additional permissions are introduced later (e.g., `ARCHIVE`).

---

## 7. Frontend Architecture

### 7.1 Contexts

| Context | File | Role |
| --- | --- | --- |
| `KeycloakContext` | `src/context/KeycloakContext.tsx` | Instantiates Keycloak JS client, manages authentication lifecycle, token refresh, and exposes token-derived user info (roles, bank metadata). |
| `RoleContext` | `src/context/RoleContext.tsx` | Maps Keycloak realm roles to simplified app roles (`superadmin`, `admin`, `viewer`). Provides convenience booleans. |

### 7.2 Key Pages & Components

| Page | File | Notes |
| --- | --- | --- |
| Admin Approvals | `src/pages/AdminApprovals.tsx` | Uses React Query hooks (`usePendingUsers`, `useApproveUser`, `useRejectUser`) to render a workflow for superadmins. Recent enhancements ensure product-role assignments are mandatory before approval. |
| User Management | `src/pages/UserManagement.tsx` | Currently uses mock data (see placeholder arrays). Future integration should swap to backend calls (`/api/v1/admin/users`). Document highlights this gap for future work. |
| Super Admin Users | `src/pages/SuperAdminUsers.tsx` | Comprehensive user list for superadmins, leveraging forms for product and sub-tenant assignments. |

### 7.3 Hooks & API Clients

- `src/hooks/use-approvals.ts` wraps mutation/query hooks for approvals endpoints.
- `src/api/approvals.api.ts` defines axios calls, including `approveUser(pendingUserId, assignedBanks, assignedSubTenants, assignedProducts, productPermissions)`.
- Additional API clients exist for banks (`src/api/banks.api.ts`), products, product roles, and permission management.

### 7.4 UI Patterns

- Shadcn UI components (e.g., `Select`, `Dialog`, `Checkbox`) provide consistent design across modals.
- Approvals dialog ensures sub-branch and product-role selections are validated before submission; toast notifications provide feedback.

---

## 8. Hybrid User Management Mechanics

### 8.1 Keycloak Integration

- `UserManagementService.createUser` and `ApprovalService.createKeycloakUser` both use Keycloak Admin credentials sourced from `application.properties` (`keycloak.auth-server-url`, `keycloak.realm`). They authenticate against the `master` realm using `admin-cli`.
- Realm role assignment occurs via `realmResource.roles().get(roleName).toRepresentation()`. Failures are logged but do not prevent user creation.
- Tokens issued by Keycloak embed custom claims (`bank_id`, `bank_name`, `sub_bank_ids`, `sub_bank_names`) consumed by the frontend.

### 8.2 Database Synchronization

- Upon registration approval or direct creation, the database mirrors Keycloak identity with additional relational data.
- Future TODOs (see service comments) include syncing activation/deactivation status back to Keycloak to disable logins.

### 8.3 Permission Resolution

- Platform-level gating (e.g., access to approvals) relies on realm role from JWT, mapped via `RoleContext`.
- Product-level permissions rely on the `UserProduct` ↔ `ProductRole` ↔ `ProductRolePermission` chain. `UserService` exposes aggregated `permissions` arrays per product.

### 8.4 Sub-tenant Database Separation

- Every record in `sub_tenants` carries `database_name`, `database_host`, and `database_port`. `database_name` is globally unique and required; host defaults to `localhost` and port to `5432` unless explicitly overridden.
- `SubTenantCreateRequest` enforces naming constraints (`[a-z0-9_]`) and rejects duplicates via `SubTenantRepository.existsByDatabaseName(...)`. This guards against two tenants pointing at the same physical schema.
- `SubTenantManagementService` surfaces create/update flows that lock `database_name` once set (no update path today) but allow ops teams to rotate host/port if the tenant’s database is migrated. Updates retain full audit timestamps so downstream jobs can reconcile.
- No runtime routing currently consumes these fields; the service layer persists them so upcoming reporting microservices can resolve tenant-specific data sources without additional lookups. Until those consumers ship, treat the values as configuration metadata that must stay in sync with legacy cron jobs.
- Operational playbook: when onboarding a new tenant, provision the database/schema first, confirm connectivity, then create the `SubTenant` via the superadmin API including the exact database coordinates. For migrations, deactivate the sub-tenant, update host/port, validate downstream ETLs, then reactivate.

---

## 9. Edge Cases, Safeguards & Known Gaps

| Area | Behavior | Mitigation / Follow-up |
| --- | --- | --- |
| **Rejection Cleanup** | Rejected `PendingUser` entries are deleted to prevent reprocessing. | Ensure audit requirements are met elsewhere (e.g., external logging). |
| **Role Assignment Failures** | If Keycloak realm role assignment fails (role missing), error is logged but user remains created. | After provisioning, verify role presence in Keycloak; consider retry/backfill job. |
| **Admin Bank Scope** | Admin APIs cross-check `bank_id` claim against payload bank IDs. | If JWT lacks `bank_id`, request is rejected with 403. |
| **Product Role Availability** | Approval UI blocks assignment when product lacks roles. Backend expects product IDs; missing roles would lead to assignment without permissions. | Ensure `product_roles` exist for licensed products. |
| **Keycloak Status Sync** | `toggleUserStatus` and activate/deactivate operations currently only update database. | TODO: call Keycloak Admin API to enable/disable user for full parity. |
| **Viewer Legacy Roles** | Database `UserRole` enum still carries legacy values. | When cleaning legacy roles, update `@Pattern` in DTOs and ensure Keycloak realm roles reflect changes. |
| **Frontend Mock Data** | `src/pages/UserManagement.tsx` uses hard-coded arrays for demonstration. | Replace with API integration using React Query hooks to avoid divergence between UI and backend logic. |

---

## 10. Sequence Summaries

### 10.1 Approval Sequence

1. Pending user submits registration.
2. Superadmin opens approval dialog.
3. UI fetches bank sub-tenants (`banksApi.getBankTenants`) and licensed products (`productLicensesApi.getLicenses` + `productRolesApi.getRoles`).
4. Superadmin selects sub-branches and product roles.
5. UI sends `ApproveUser` mutation with selected banks, sub-tenants, products, and role-derived permissions.
6. Backend creates Keycloak user, database records, marks pending entry approved.
7. UI displays success toast and refreshes pending list.

### 10.2 Bank Admin Create User

1. Admin selects **Create User** within bank UI.
2. Payload includes only admin's bank ID.
3. Controller validates bank scope and delegates to `UserManagementService`.
4. Service provisions Keycloak account, assigns bank/sub-tenant/product entries.
5. Response returns `UserDetailResponse`; UI updates list.

---

## 11. Tables & Columns (Detailed)

> ⚠️ Types inferred from JPA annotations; confirm with actual DDL when available.

### 11.1 `users`

| Column | Type | Nullable | Description |
| --- | --- | --- | --- |
| `id` | UUID | No | Primary key. |
| `keycloak_id` | VARCHAR(100) | Yes | Unique Keycloak identifier. |
| `email` | VARCHAR(255) | No | Unique. |
| `first_name` | VARCHAR(100) | No |  |
| `last_name` | VARCHAR(100) | No |  |
| `role` | VARCHAR(50) | No | Enum stored as string. |
| `status` | VARCHAR(20) | No | Enum stored as string; defaults to `PENDING`. |
| `last_login` | TIMESTAMP | Yes | Audit. |
| `created_at` | TIMESTAMP | No | Auto-populated. |
| `updated_at` | TIMESTAMP | No | Auto-populated. |
| `created_by` | UUID | Yes | Approver or creator reference. |
| `updated_by` | UUID | Yes | Updater reference. |

### 11.2 `user_banks`

| Column | Type | Nullable | Description |
| --- | --- | --- | --- |
| `user_id` | UUID | No | FK → `users.id`. |
| `bank_id` | UUID | No | FK → `banks.id`. |
| `assigned_at` | TIMESTAMP | No | Audited assignment timestamp. |
| `assigned_by` | UUID | Yes | Approver/assigner. |

### 11.3 `user_sub_tenants`

| Column | Type | Nullable | Description |
| --- | --- | --- | --- |
| `user_id` | UUID | No | FK → `users.id`. |
| `sub_tenant_id` | UUID | No | FK → `sub_tenants.id`. |
| `assigned_at` | TIMESTAMP | No | Auto timestamp. |
| `assigned_by` | UUID | Yes | Metadata. |

### 11.4 `user_products`

| Column | Type | Nullable | Description |
| --- | --- | --- | --- |
| `user_id` | UUID | No | FK → `users.id`. |
| `bank_id` | UUID | No | FK → `banks.id`. |
| `product_id` | UUID | No | FK → `products.id`. |
| `role_id` | UUID | No | FK → `product_roles.id`. |
| `assigned_at` | TIMESTAMP | No | Auto timestamp. |
| `assigned_by` | UUID | Yes | Metadata. |

### 11.5 `pending_users`

| Column | Type | Nullable | Description |
| --- | --- | --- | --- |
| `email` | VARCHAR(255) | No | Unique. |
| `requested_bank_id` | UUID | No | FK → `banks.id`. |
| `requested_role` | VARCHAR(50) | No | Enum string. |
| `justification` | TEXT | Yes | Applicant rationale. |
| `status` | VARCHAR(20) | No | Enum string. |
| `requested_at` | TIMESTAMP | No | Auto timestamp. |
| `reviewed_at` | TIMESTAMP | Yes | Set when processed. |
| `reviewed_by` | UUID | Yes | FK → `users.id`. |
| `rejection_reason` | TEXT | Yes | Provided during rejection. |

---

## 12. Configuration & Environment

- **Keycloak Settings**
  - Controlled via environment variables `VITE_KEYCLOAK_URL`, `VITE_KEYCLOAK_REALM`, `VITE_KEYCLOAK_CLIENT_ID` on the frontend.
  - Backend expects `keycloak.auth-server-url`, `keycloak.realm`, `keycloak.resource`, etc. (See Spring configuration files.)
- **Token Storage**
  - Frontend stores access/refresh tokens in `localStorage` (`fusion_access_token`, `fusion_refresh_token`) for axios interceptors.
- **JWT Claims**
  - `bank_id`, `bank_name`, `sub_bank_ids`, `sub_bank_names` are expected to be present for admins; ensure Keycloak mapper configuration aligns.

---

## 13. Testing & Validation Considerations

1. **Approval Happy Path**
   - Submit registration → approve via UI → verify Keycloak user creation and DB entries across `users`, junction tables.
2. **Rejection Cleanup**
   - Submit registration → reject → ensure `pending_users` no longer contains the record and API returns `REJECTED` response.
3. **Admin Scope Enforcement**
   - Use admin JWT without `bank_id` → expect 403 on `/admin/users` endpoints.
   - Try assigning different bank ID → expect 403.
4. **Product Role Integrity**
   - Approve user when product has no roles → UI should block (frontend). Backend validation expects product IDs; ensure there is at least one role defined (create via `/products/{id}/roles`).
5. **Keycloak Role Sync**
   - After user creation, confirm realm role exists in Keycloak; if missing, logs flag the issue.

---

## 14. Future Enhancements

- **Keycloak Status Synchronization**: Implement `users().get(keycloakId).update(...)` calls when toggling status to ensure disabled users cannot log in.
- **Audit Trails**: Persist rejection events (currently only logged) to satisfy compliance.
- **Frontend User Management Integration**: Replace mock data in `src/pages/UserManagement.tsx` with live data sourced from `/api/v1/admin/users` and incorporate sub-tenant/product assignment dialogues.
- **Bulk Operations**: Improve `BulkUserImportController` to validate bank boundaries similarly to admin controllers.
- **Role Mapper Consistency**: Align database `UserRole` enumeration with Keycloak realm roles to avoid mismatches (drop legacy values or support mapping layer).

---

## 15. File Map Reference

- Backend Entities: `fusion-backend/src/main/java/com/fusion/entity/`
- Backend DTOs: `fusion-backend/src/main/java/com/fusion/dto/`
- Backend Controllers: `fusion-backend/src/main/java/com/fusion/controller/`
- Backend Services: `fusion-backend/src/main/java/com/fusion/service/`
- Frontend Pages:
  - `src/pages/AdminApprovals.tsx`
  - `src/pages/UserManagement.tsx`
  - `src/pages/SuperAdminUsers.tsx`
- Frontend Contexts: `src/context/KeycloakContext.tsx`, `src/context/RoleContext.tsx`
- Frontend Hooks: `src/hooks/use-approvals.ts`
- Frontend API Clients: `src/api/approvals.api.ts`, `src/api/banks.api.ts`, etc.

---

## 16. Glossary

| Term | Definition |
| --- | --- |
| **Hybrid User Management** | Approach where identity (authentication, realm roles) lives in Keycloak while authorization context (banks, sub-tenants, product roles) lives in the application database. |
| **Sub-Tenant** | A subdivision of a bank (branch, region) with potentially distinct database schema or credentials. |
| **Product Role** | Set of permissions tied to a product, stored in `product_roles` and linked to users via `user_products`. |
| **Pending User** | Registration request awaiting manual review. |
| **Realm Role** | Keycloak-wide role applied at realm level (e.g., `superadmin`). |

---

## 17. Role Hierarchy & Capability Matrix

| Capability | Superadmin | Bank Admin | Viewer / Product User |
| --- | --- | --- | --- |
| Approve / reject registrations | ✅ (`ApprovalController`, frontend `AdminApprovals`) | ❌ | ❌ |
| Create users for any bank | ✅ (`UserManagementController`) | ❌ | ❌ |
| Create users within own bank | ✅ | ✅ (`AdminUserManagementController`, scoped by `bank_id`) | ❌ |
| Assign banks | ✅ | ✅ (limited to own bank) | ❌ |
| Assign sub-tenants | ✅ | ✅ (own bank sub-tenants only) | ❌ |
| Assign product roles | ✅ | ✅ (licensed products for own bank) | ❌ |
| Toggle user status | ✅ | ✅ (own bank) | ❌ |
| Access licensed products | ✅ (superset) | ✅ | ✅ (limited by `user_products`) |
| Access approval dashboard | ✅ | ❌ | ❌ |
| Access bank admin dashboard | ✅ | ✅ | ❌ |
| Access viewer dashboards | ✅ | ✅ | ✅ |

Realm roles in Keycloak follow lowercase naming (`superadmin`, `admin`, `viewer`). `RoleContext` maps these to UI-friendly roles while preserving superadmin privilege superset.

Legacy database roles (`MANAGER`, `REPORT_ADMIN`, etc.) persist for backward compatibility. When onboarding new users outside Keycloak (seed data), ensure the realm role name and database enum are aligned.

---

## 18. Detailed Workflow Reference

### 19.1 Registration Rejection & Cleanup

1. Superadmin clicks **Reject** from approvals UI.
2. Frontend calls `rejectUserMutation` (`useRejectUser`) → `POST /api/v1/admin/approvals/{id}` with `{ "approved": false, "rejectionReason": "..." }`.
3. `ApprovalService.processApproval` injects path `pendingUserId` into payload.
4. `rejectUser` validates reason, deletes row from `pending_users`, logs reason.
5. Response returns `status: "REJECTED"`; UI displays toast and refetches pending list.

### 19.2 User Update Lifecycle

1. Superadmin issues `PUT /api/v1/superadmin/users/{id}` with `UserUpdateRequest`.
2. `UserManagementService.updateUser` loads `User`, updates personal data, role, and status.
3. Associations are fully replaced: existing `user_banks`, `user_sub_tenants`, `user_products` are deleted (via repository helpers) then recreated.
4. Product assignments rely on `ProductAssignment` objects containing `bankId`, `productId`, `roleId`.
5. Service persists updates and returns `UserDetailResponse` aggregating latest associations.

### 19.3 Activation & Deactivation

1. `POST /api/v1/superadmin/users/{id}/activate` or `/deactivate` (superadmin) or `PATCH /api/v1/admin/users/{id}/status` (bank admin) updates `User.status`.
2. No Keycloak call yet; TODO flagged in service to sync.
3. Downstream components reference `status` to hide disabled users; ensure UI respects `INACTIVE`.

### 19.4 Product Role Assignment Rules

- Approvals UI enforces selection of at least one product with role; missing roles trigger inline validation.
- Backend expects `assignedProducts` (list of product IDs) and `productPermissions` (inferred from selected role’s `permissions`).
- `UserProduct` rows link user↔bank↔product↔role; uniqueness prevents duplicate assignments.

### 19.5 Bulk Import (Hybrid)

- `BulkUserImportController` enables CSV-driven onboarding.
- Service should observe same validations as manual creation (banks, sub-tenants, licensed products). Review `fusion-backend/src/main/java/com/fusion/controller/BulkUserImportController.java` and related services when enabling.

---

## 19. API Contracts & Sample Payloads

### 20.1 Approve User (Success)

Request:

```bash
curl -X POST \
   "http://localhost:8081/api/v1/admin/approvals/4e2e612b-e84f-40bf-baf6-d9fa9a6a63cb" \
   -H "Authorization: Bearer <token>" \
   -H "Content-Type: application/json" \
   -d '{
            "approved": true,
            "bankIds": ["2ab4106d-..."],
            "subTenantIds": ["7f9fa21b-..."],
            "productIds": ["5f2d13c0-..."],
            "productPermissions": [
               {
                  "productId": "5f2d13c0-...",
                  "permissions": ["READ", "DOWNLOAD"]
               }
            ]
         }'
```

Response:

```json
{
   "userId": "9a6b77ae-...",
   "email": "user@example.com",
   "status": "APPROVED",
   "message": "User approved and account created successfully",
   "keycloakId": "0e2fd34a-..."
}
```

### 20.2 Reject User (Validation)

If `rejectionReason` missing:

```json
{
   "timestamp": "2025-11-28T14:35:40.792741",
   "status": 400,
   "error": "Validation Error",
   "message": "Rejection reason is required",
   "path": "/api/v1/admin/approvals/{id}"
}
```

### 20.3 Create User (Superadmin)

```json
{
   "email": "analyst@bank.com",
   "firstName": "Data",
   "lastName": "Analyst",
   "password": "TempPass@123",
   "role": "ADMIN",
   "bankIds": ["2ab4106d-..."],
   "subTenantIds": ["7f9fa21b-..."],
   "productAssignments": [
      {
         "bankId": "2ab4106d-...",
         "productId": "5f2d13c0-...",
         "roleId": "8c1a0f99-..."
      }
   ]
}
```

Response includes aggregated assignments as `UserDetailResponse`.

### 20.4 Error Codes (Representative)

| Scenario | Status | Message Source |
| --- | --- | --- |
| Missing `bank_id` claim for admin endpoint | 403 | `AdminUserManagementController` guard |
| Assigning tenant outside bank | 400 | `ApprovalService.validateSubTenantsAssignment` |
| Duplicate email | 400 | `UserManagementService.createUser` (BusinessException) |
| Product role missing | 404 | `ProductRoleRepository.findById` throws `ResourceNotFoundException` |

---

## 20. Repository Reference

Located under `fusion-backend/src/main/java/com/fusion/repository/`. Key interfaces:

- `UserRepository`: lookup by email, Keycloak ID, bank.
- `PendingUserRepository`: `findPendingUsersForAdmin`, `countByStatus`, `delete`.
- `UserBankRepository`, `UserSubTenantRepository`, `UserProductRepository`: manage junction tables (delete-by-user helpers used during updates).
- `ProductRoleRepository`: fetch product roles for assignments.
- `BankRepository`, `SubTenantRepository`, `ProductRepository`: ensure referenced entities exist.

Repositories extend Spring Data JPA interfaces, enabling transactional boundaries defined in services.

---

## 21. Frontend State & Query Lifecycle

- React Query keys defined in `src/lib/react-query`. For approvals: `queryKeys.approvals.pending`.
- `usePendingUsers` auto-refetches every 30s to keep approval queue fresh.
- Mutations (`useApproveUser`, `useRejectUser`) invalidate approvals, pending approvals, users, banks, products caches to propagate updates.
- State slices in `AdminApprovals.tsx` manage selected sub-branches/products, with helper `resetApproveDialogState` ensuring clean resets.
- Toast feedback via `react-hot-toast`; consistent error surfaces when network failures occur.

---

## 22. Keycloak Realm Configuration

Minimum configuration requirements:

1. **Realm Roles**: `superadmin`, `admin`, `viewer` (plus optional legacy roles). Ensure client scopes propagate roles to ID token (`realm_access.roles`).
2. **Client**: `report-manager-client` (confidential/public). Configure redirect URIs for `http://localhost:3000/*` and silent check page.
3. **Mappers**: Add protocol mappers to inject custom claims:
    - `bank_id`, `bank_name` (string).
    - `sub_bank_ids`, `sub_bank_names` (multi-valued or comma separated).
4. **Service Account**: For admin integrations (`admin-cli`), supply credentials in backend config.

During provisioning, service hits Keycloak master realm using admin user credentials (see `createKeycloakUser`). For production, replace with service account + client credentials.

---

## 23. Database Constraints & Indexing

- Unique constraints:
   - `users.email`, `users.keycloak_id`.
   - `user_banks (user_id, bank_id)`.
   - `user_sub_tenants (user_id, sub_tenant_id)`.
   - `user_products (user_id, bank_id, product_id)`.
   - `product_roles (product_id, role_name)`.
   - `product_role_permissions (role_id, permission)`.
   - `sub_tenants (bank_id, code)` and `sub_tenants.database_name`.
- Cascades handled via `orphanRemoval = true` in entity definitions; deleting `User` automatically drops junction rows.
- Auditing fields (`created_at`, `updated_at`) require enabling Spring Data JPA auditing (see main application configuration).

---

## 24. Operational Runbook & CLI Snippets

### 25.1 List Pending Users

```bash
curl -H "Authorization: Bearer <token>" \
   http://localhost:8081/api/v1/admin/approvals/pending | jq
```

### 25.2 Delete Pending User (Manual Cleanup)

```bash
curl -X DELETE -H "Authorization: Bearer <token>" \
   http://localhost:8081/api/v1/admin/approvals/pending/<pendingUserId>
```

### 25.3 Toggle User Status

```bash
curl -X PATCH \
   -H "Authorization: Bearer <admin token>" \
   -H "Content-Type: application/json" \
   -d '{"enabled": false}' \
   http://localhost:8081/api/v1/admin/users/<userId>/status
```

### 25.4 Inspect User Profile

```bash
curl -H "Authorization: Bearer <token>" \
   http://localhost:8081/api/v1/users/me | jq
```

---

## 25. Validation & Error Handling

- DTO annotations enforce required fields (`@NotBlank`, `@Email`, etc.).
- `BusinessException` signals domain rule violations (e.g., duplicate email, missing bank assignment).
- `ResourceNotFoundException` used for missing referenced entities; surfaces as 404.
- Global exception handlers (not shown) should convert exceptions to JSON payload with `timestamp`, `status`, `message`, `path` (see existing error output).
- Client-side validations: Approvals UI disables approval button until sub-branches and product roles chosen.

---

## 26. Known TODOs & Technical Debt

- Sync Keycloak status toggles.
- Replace mock data in bank admin UI with real endpoints.
- Persist audit trail for rejections in dedicated table or external log aggregator.
- Parameterize Keycloak admin credentials (avoid plain `admin`/`admin`).
- Strengthen concurrency control on approval flow to avoid double-processing (consider optimistic locking on `pending_users`).
- Document migrations or schema evolution tool (Liquibase/Flyway) once adopted.

---

## 27. Testing Strategy & Automation Hooks

- Unit tests should cover `ApprovalService` happy path and rejection path (assert deletion, product association).
- Integration tests (Spring Boot) recommended for Keycloak-less mode using mock admin client or WireMock.
- Frontend: add Cypress/React Testing Library coverage for approvals dialog validation.
- Smoke checklist:
   1. Submit registration and approve.
   2. Submit registration and reject (verify deletion).
   3. Admin create user with product role and confirm assignments via `/users/me`.
   4. Toggle status and ensure UI hides disabled user once API integrated.

---

## 28. Related Modules

- `fusion-backend/src/main/java/com/fusion/controller/AuthController.java`: Handles login handshake / token refresh for platform services.
- `fusion-backend/src/main/java/com/fusion/service/ProductService.java`: Exposes product permissions for UI; interacts with `UserProduct` assignments.
- `fusion-backend/src/main/java/com/fusion/service/BankService.java`: Supplies bank and sub-tenant metadata leveraged by user management forms.
- Frontend UI library components under `src/components/ui/` provide base building blocks for forms and dialogs.

---

## 29. Change Log

| Date | Change |
| --- | --- |
| 29 Nov 2025 | Initial comprehensive draft documenting backend/frontend architecture, approval fix (pending user deletion), and hybrid model. |
| 29 Nov 2025 | Added role matrix, workflow deep dives, API samples, Keycloak config guidance, repository references, validation notes, and runbook commands. |

---

*Maintainers: update this document alongside any significant code change to ensure operational parity.*
