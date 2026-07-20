import { NavLink } from "react-router-dom";
import { useUIStore } from "@/stores/ui-store";
import { cn } from "@/lib/utils";
import {
  LayoutDashboard,
  Plug,
  Database,
  HardDrive,
  Cable,
  Shuffle,
  Code2,
  ShieldCheck,
  Bell,
  Activity,
  GitBranch,
  Settings,
  Inbox,
  ChevronsLeft,
  ChevronsRight,
} from "lucide-react";

const navSections = [
  {
    title: "MAIN",
    items: [
      { to: "/dashboard", icon: LayoutDashboard, label: "Dashboard" },
      { to: "/connectors", icon: Plug, label: "Connectors" },
    ],
  },
  {
    title: "DATA PIPELINE",
    items: [
      { to: "/sources", icon: Database, label: "Sources" },
      { to: "/destinations", icon: HardDrive, label: "Destinations" },
      { to: "/connections", icon: Cable, label: "Connections" },
    ],
  },
  {
    title: "PROCESSING",
    items: [
      { to: "/transformations", icon: Shuffle, label: "Transformations" },
      { to: "/udfs", icon: Code2, label: "UDFs" },
    ],
  },
  {
    title: "QUALITY & ALERTS",
    items: [
      { to: "/data-quality", icon: ShieldCheck, label: "Data Quality" },
      { to: "/alerts", icon: Bell, label: "Alerts" },
      { to: "/schema-evolution", icon: GitBranch, label: "Schema Evolution" },
    ],
  },
  {
    title: "OPERATIONS",
    items: [
      { to: "/monitoring", icon: Activity, label: "Monitoring" },
      { to: "/graphql", icon: Code2, label: "GraphQL Explorer" },
      { to: "/dlq", icon: Inbox, label: "Dead Letter Queue" },
    ],
  },
  {
    title: "ADMIN",
    items: [
      { to: "/settings", icon: Settings, label: "Settings" },
    ],
  },
];

export function Sidebar() {
  const collapsed = useUIStore((s) => s.sidebarCollapsed);
  const toggleSidebar = useUIStore((s) => s.toggleSidebar);

  return (
    <aside
      className={cn(
        "fixed left-0 top-0 z-40 h-screen border-r border-border bg-gray-900 text-gray-300 transition-all duration-300 flex flex-col",
        collapsed ? "w-16" : "w-64"
      )}
    >
      <div className="flex h-14 items-center justify-between border-b border-gray-800 px-4">
        {!collapsed && (
          <span className="text-lg font-bold text-white">DCraft Fusion CDC</span>
        )}
        {collapsed && <span className="text-lg font-bold text-white">DF</span>}
        <button onClick={toggleSidebar} className="rounded p-1 hover:bg-gray-800">
          {collapsed ? <ChevronsRight className="h-4 w-4" /> : <ChevronsLeft className="h-4 w-4" />}
        </button>
      </div>
      <nav className="mt-2 flex-1 overflow-y-auto px-2">
        {navSections.map((section) => (
          <div key={section.title} className="mb-3">
            {!collapsed && (
              <span className="px-3 text-[10px] font-semibold uppercase tracking-wider text-gray-500">
                {section.title}
              </span>
            )}
            <div className="mt-1 flex flex-col gap-0.5">
              {section.items.map((item) => (
                <NavLink
                  key={item.to}
                  to={item.to}
                  className={({ isActive }) =>
                    cn(
                      "flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium transition-colors",
                      isActive
                        ? "bg-gray-800 text-white border-l-2 border-primary"
                        : "text-gray-400 hover:bg-gray-800 hover:text-white"
                    )
                  }
                >
                  <item.icon className="h-4 w-4 shrink-0" />
                  {!collapsed && <span>{item.label}</span>}
                </NavLink>
              ))}
            </div>
          </div>
        ))}
      </nav>
    </aside>
  );
}
