import { Routes, Route, Navigate } from "react-router-dom";
import { useAuthStore } from "@/stores/auth-store";
import { MainLayout } from "@/components/layout/MainLayout";
import { LoginPage } from "@/pages/auth/LoginPage";
import { RegisterPage } from "@/pages/auth/RegisterPage";
import { ForgotPasswordPage } from "@/pages/auth/ForgotPasswordPage";
import { DashboardPage } from "@/pages/dashboard/DashboardPage";
import { SourcesPage } from "@/pages/sources/SourcesPage";
import { SourceDetailPage } from "@/pages/sources/SourceDetailPage";
import { CreateSourceWizard } from "@/pages/sources/CreateSourceWizard";
import { DestinationsPage } from "@/pages/destinations/DestinationsPage";
import { DestinationDetailPage } from "@/pages/destinations/DestinationDetailPage";
import { CreateDestinationWizard } from "@/pages/destinations/CreateDestinationWizard";
import { ConnectionsPage } from "@/pages/connections/ConnectionsPage";
import { ConnectionDetailPage } from "@/pages/connections/ConnectionDetailPage";
import { CreateConnectionWizard } from "@/pages/connections/CreateConnectionWizard";
import { TransformationsPage } from "@/pages/transformations/TransformationsPage";
import { TransformDetailPage } from "@/pages/transformations/TransformDetailPage";
import { CreateTransformPage } from "@/pages/transformations/CreateTransformPage";
import { UDFsPage } from "@/pages/udfs/UDFsPage";
import { UDFDetailPage } from "@/pages/udfs/UDFDetailPage";
import { CreateUDFPage } from "@/pages/udfs/CreateUDFPage";
import { DataQualityPage } from "@/pages/data-quality/DataQualityPage";
import { DQPoliciesPage } from "@/pages/data-quality/DQPoliciesPage";
import { DQPolicyDetailPage } from "@/pages/data-quality/DQPolicyDetailPage";
import { CreateDQPolicyPage } from "@/pages/data-quality/CreateDQPolicyPage";
import { DQViolationsPage } from "@/pages/data-quality/DQViolationsPage";
import { DQViolationDetailPage } from "@/pages/data-quality/DQViolationDetailPage";
import { DQProfilingPage } from "@/pages/data-quality/DQProfilingPage";
import { AlertsPage } from "@/pages/alerts/AlertsPage";
import { AlertDetailPage } from "@/pages/alerts/AlertDetailPage";
import { AlertRulesPage } from "@/pages/alerts/AlertRulesPage";
import { CreateAlertRulePage } from "@/pages/alerts/CreateAlertRulePage";
import { AlertChannelsPage } from "@/pages/alerts/AlertChannelsPage";
import { CreateAlertChannelPage } from "@/pages/alerts/CreateAlertChannelPage";
import { AlertSuppressionsPage } from "@/pages/alerts/AlertSuppressionsPage";
import { MonitoringPage } from "@/pages/monitoring/MonitoringPage";
import { WorkersPage } from "@/pages/monitoring/WorkersPage";
import { GraphQLPage } from "@/pages/graphql/GraphQLPage";
import { ConnectionHealthPage } from "@/pages/monitoring/ConnectionHealthPage";
import { SchemaEvolutionPage } from "@/pages/schema-evolution/SchemaEvolutionPage";
import { ConnectionSchemaPage } from "@/pages/schema-evolution/ConnectionSchemaPage";
import { SettingsPage } from "@/pages/settings/SettingsPage";
import { ProfilePage } from "@/pages/settings/ProfilePage";
import { UsersPage } from "@/pages/settings/UsersPage";
import { RolesPage } from "@/pages/settings/RolesPage";
import { AuditLogsPage } from "@/pages/settings/AuditLogsPage";
import { SystemConfigPage } from "@/pages/settings/SystemConfigPage";
import { FeatureFlagsPage } from "@/pages/settings/FeatureFlagsPage";
import { MaintenanceWindowsPage } from "@/pages/settings/MaintenanceWindowsPage";
import { ResourceQuotasPage } from "@/pages/settings/ResourceQuotasPage";
import { DLQPage } from "@/pages/dlq/DLQPage";
import { DLQDetailPage } from "@/pages/dlq/DLQDetailPage";
import { DLQEventDetailPage } from "@/pages/dlq/DLQEventDetailPage";
import { ConnectorsPage } from "@/pages/connectors/ConnectorsPage";
import { ConnectorDetailPage } from "@/pages/connectors/ConnectorDetailPage";
import { EditSourcePage } from "@/pages/sources/EditSourcePage";
import { EditDestinationPage } from "@/pages/destinations/EditDestinationPage";
import { EditConnectionPage } from "@/pages/connections/EditConnectionPage";
import { EditTransformPage } from "@/pages/transformations/EditTransformPage";
import { EditUDFPage } from "@/pages/udfs/EditUDFPage";
import { AlertRuleDetailPage } from "@/pages/alerts/AlertRuleDetailPage";
import { AlertChannelDetailPage } from "@/pages/alerts/AlertChannelDetailPage";
import { ChangePasswordPage } from "@/pages/settings/ChangePasswordPage";
import { DQTemplatesPage } from "@/pages/data-quality/DQTemplatesPage";

function ProtectedRoute({ children }: { children: React.ReactNode }) {
  const isAuthenticated = useAuthStore((s) => s.isAuthenticated);
  if (!isAuthenticated) return <Navigate to="/login" replace />;
  return <>{children}</>;
}

export default function App() {
  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route path="/register" element={<RegisterPage />} />
      <Route path="/forgot-password" element={<ForgotPasswordPage />} />
      <Route
        path="/*"
        element={
          <ProtectedRoute>
            <MainLayout />
          </ProtectedRoute>
        }
      >
        <Route index element={<Navigate to="/dashboard" replace />} />
        <Route path="dashboard" element={<DashboardPage />} />
        {/* Connectors */}
        <Route path="connectors" element={<ConnectorsPage />} />
        <Route path="connectors/:id" element={<ConnectorDetailPage />} />
        {/* Sources */}
        <Route path="sources" element={<SourcesPage />} />
        <Route path="sources/new" element={<CreateSourceWizard />} />
        <Route path="sources/:id" element={<SourceDetailPage />} />
        <Route path="sources/:id/edit" element={<EditSourcePage />} />
        {/* Destinations */}
        <Route path="destinations" element={<DestinationsPage />} />
        <Route path="destinations/new" element={<CreateDestinationWizard />} />
        <Route path="destinations/:id" element={<DestinationDetailPage />} />
        <Route path="destinations/:id/edit" element={<EditDestinationPage />} />
        {/* Connections */}
        <Route path="connections" element={<ConnectionsPage />} />
        <Route path="connections/new" element={<CreateConnectionWizard />} />
        <Route path="connections/:id" element={<ConnectionDetailPage />} />
        <Route path="connections/:id/edit" element={<EditConnectionPage />} />
        {/* Transformations */}
        <Route path="transformations" element={<TransformationsPage />} />
        <Route path="transformations/new" element={<CreateTransformPage />} />
        <Route path="transformations/:id" element={<TransformDetailPage />} />
        <Route path="transformations/:id/edit" element={<EditTransformPage />} />
        {/* UDFs */}
        <Route path="udfs" element={<UDFsPage />} />
        <Route path="udfs/new" element={<CreateUDFPage />} />
        <Route path="udfs/:id" element={<UDFDetailPage />} />
        <Route path="udfs/:id/edit" element={<EditUDFPage />} />
        {/* Data Quality */}
        <Route path="data-quality" element={<DataQualityPage />} />
        <Route path="data-quality/policies" element={<DQPoliciesPage />} />
        <Route path="data-quality/policies/new" element={<CreateDQPolicyPage />} />
        <Route path="data-quality/policies/:id" element={<DQPolicyDetailPage />} />
        <Route path="data-quality/violations" element={<DQViolationsPage />} />
        <Route path="data-quality/violations/:id" element={<DQViolationDetailPage />} />
        <Route path="data-quality/profiling" element={<DQProfilingPage />} />
        <Route path="data-quality/templates" element={<DQTemplatesPage />} />
        {/* Alerts */}
        <Route path="alerts" element={<AlertsPage />} />
        <Route path="alerts/:id" element={<AlertDetailPage />} />
        <Route path="alerts/rules" element={<AlertRulesPage />} />
        <Route path="alerts/rules/new" element={<CreateAlertRulePage />} />
        <Route path="alerts/rules/:id" element={<AlertRuleDetailPage />} />
        <Route path="alerts/channels" element={<AlertChannelsPage />} />
        <Route path="alerts/channels/new" element={<CreateAlertChannelPage />} />
        <Route path="alerts/channels/:id" element={<AlertChannelDetailPage />} />
        <Route path="alerts/suppressions" element={<AlertSuppressionsPage />} />
        {/* Monitoring */}
        <Route path="monitoring" element={<MonitoringPage />} />
        <Route path="monitoring/workers" element={<WorkersPage />} />
        <Route path="monitoring/connections/:id" element={<ConnectionHealthPage />} />
        {/* Schema Evolution */}
        <Route path="schema-evolution" element={<SchemaEvolutionPage />} />
        <Route path="schema-evolution/:connectionId" element={<ConnectionSchemaPage />} />
        {/* Settings */}
        <Route path="settings" element={<SettingsPage />} />
        <Route path="settings/profile" element={<ProfilePage />} />
        <Route path="settings/password" element={<ChangePasswordPage />} />
        <Route path="settings/users" element={<UsersPage />} />
        <Route path="settings/roles" element={<RolesPage />} />
        <Route path="settings/audit-logs" element={<AuditLogsPage />} />
        <Route path="settings/system" element={<SystemConfigPage />} />
        <Route path="settings/system-config" element={<SystemConfigPage />} />
        <Route path="settings/feature-flags" element={<FeatureFlagsPage />} />
        <Route path="settings/maintenance-windows" element={<MaintenanceWindowsPage />} />
        <Route path="settings/resource-quotas" element={<ResourceQuotasPage />} />
        {/* GraphQL Explorer */}
        <Route path="graphql" element={<GraphQLPage />} />
        {/* DLQ */}
        <Route path="dlq" element={<DLQPage />} />
        <Route path="dlq/:connectionId" element={<DLQDetailPage />} />
        <Route path="dlq/:connectionId/:eventId" element={<DLQEventDetailPage />} />
      </Route>
    </Routes>
  );
}
