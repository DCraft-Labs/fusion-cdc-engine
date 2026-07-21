# Changelog

All notable changes to Fusion CDC Engine (private repo) are documented here.
This project follows [Keep a Changelog](https://keepachangelog.com/) and
uses [Semantic Versioning](https://semver.org/).

## [1.2.1] — 2026-07-21

Hotfix release addressing the three hard blockers found when verifying v1.2.0
against the remote (192.168.1.10) deployment, plus the missing Kafka
dependency for the CDC pipeline.

### Fixed
- **CDC `/settings/audit-logs` blank page (BLOCKER).** The frontend had an
  `AuditLogsPage` route (`/settings/audit-logs`) and a Settings card linking
  to it, but the control-plane has no `/api/v1/settings/audit-logs`
  endpoint — the `audit_logs` table exists but no router reads from it.
  Navigating to the page produced a blank/broken UI. Removed the "Audit
  Logs" card from `frontend/src/pages/settings/SettingsPage.tsx` and the
  route + import from `frontend/src/App.tsx`. The `AuditLogsPage.tsx` file
  is retained for the follow-up that adds the backend endpoint.

### Changed
- **Control-plane version string.** `control-plane/app/main.py` initialized
  FastAPI with `version="0.1.0"`, so `/api/openapi.json` reported the wrong
  version. Bumped to `version="1.2.1"`.
- **Private Helm chart version.** `helm/fusion-cdc/Chart.yaml` bumped from
  `2.0.0` to `1.2.1` (both `version` and `appVersion`) to match the public
  chart and the app release.

### Notes
- The Kafka manifest in `kubernetes/base/kafka.yaml` is unchanged in this
  repo — the user-facing fix lives in the public `dcraft-fusion` /
  `fusion-cdc` Helm charts (see that repo's CHANGELOG). The kustomize
  manifest remains the source of truth for the in-cluster broker shape and
  was used as the basis for the new `templates/kafka.yaml` in the public
  chart.
- **Follow-up (not in this release):** implement a minimal
  `/api/v1/settings/audit-logs` endpoint (paginated list, filters by
  user/action/resource/date) reading from the existing `audit_logs` table,
  then re-add the `AuditLogsPage` route + Settings card.
