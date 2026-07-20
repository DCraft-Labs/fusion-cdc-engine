import { Outlet } from "react-router-dom";
import { Sidebar } from "./Sidebar";
import { TopBar } from "./TopBar";
import { ErrorBoundary } from "@/components/ErrorBoundary";
import { useUIStore } from "@/stores/ui-store";
import { cn } from "@/lib/utils";

export function MainLayout() {
  const collapsed = useUIStore((s) => s.sidebarCollapsed);

  return (
    <div className="flex h-screen overflow-hidden bg-background">
      <Sidebar />
      <div className={cn("flex flex-1 flex-col overflow-hidden transition-all duration-300", collapsed ? "ml-16" : "ml-64")}>
        <TopBar />
        <main className="flex-1 overflow-y-auto p-6">
          <ErrorBoundary>
            <Outlet />
          </ErrorBoundary>
        </main>
      </div>
    </div>
  );
}
