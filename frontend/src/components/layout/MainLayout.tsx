import { Outlet, Navigate, useLocation } from "react-router-dom";
import { useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { Sidebar } from "./Sidebar";
import { TopBar } from "./TopBar";
import { ErrorBoundary } from "@/components/ErrorBoundary";
import { useUIStore } from "@/stores/ui-store";
import { useAuthStore } from "@/stores/auth-store";
import { cn } from "@/lib/utils";
import { api } from "@/lib/api";

const RESOURCE_CONFIG_PATH = "/resource-config";

export function MainLayout() {
  const collapsed = useUIStore((s) => s.sidebarCollapsed);
  const isAuthenticated = useAuthStore((s) => s.isAuthenticated);
  const user = useAuthStore((s) => s.user);
  const loadUser = useAuthStore((s) => s.loadUser);
  const location = useLocation();

  // On mount (or after a refresh with a persisted token), if we're
  // authenticated but haven't loaded the user profile yet, fetch it so
  // role-gated UI (e.g. SettingsPage cards) works without a re-login.
  useEffect(() => {
    if (isAuthenticated && !user) {
      loadUser();
    }
  }, [isAuthenticated, user, loadUser]);

  // Resource-aware admission control gate: on app-shell mount (i.e. every
  // authenticated session, not just literal first login), check whether the
  // tenant's compute pool has been configured. If not, force the user onto
  // the Resource Config page — mirrors ProtectedRoute in App.tsx, but gates
  // on resource_configured instead of auth. Query key ("resource-config")
  // matches ResourceConfigPage.tsx's own query/invalidation so saving there
  // immediately lifts the gate.
  const { data: resourceConfig, isLoading: resourceConfigLoading } = useQuery({
    queryKey: ["resource-config"],
    queryFn: () => api.get("/resource-config").then((r) => r.data),
    enabled: isAuthenticated,
    staleTime: 60_000,
  });

  const resourceConfigRequired =
    isAuthenticated && !resourceConfigLoading && resourceConfig?.resource_configured === false;
  const onResourceConfigPage = location.pathname === RESOURCE_CONFIG_PATH;

  return (
    <div className="flex h-screen overflow-hidden bg-background">
      <Sidebar />
      <div className={cn("flex flex-1 flex-col overflow-hidden transition-all duration-300", collapsed ? "ml-16" : "ml-64")}>
        <TopBar />
        <main className="flex-1 overflow-y-auto p-6">
          <ErrorBoundary>
            {resourceConfigRequired && !onResourceConfigPage ? (
              <Navigate to={RESOURCE_CONFIG_PATH} replace />
            ) : (
              <Outlet />
            )}
          </ErrorBoundary>
        </main>
      </div>
    </div>
  );
}
