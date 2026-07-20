import { useState } from "react";
import { useNavigate, useLocation, Link } from "react-router-dom";
import { useAuthStore } from "@/stores/auth-store";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Menu, LogOut, User, Bell, Search, ChevronRight } from "lucide-react";
import { useUIStore } from "@/stores/ui-store";

function Breadcrumb() {
  const location = useLocation();
  const segments = location.pathname.split("/").filter(Boolean);
  if (segments.length === 0) return null;
  return (
    <div className="flex items-center gap-1 text-sm text-muted-foreground">
      {segments.map((seg, i) => (
        <span key={i} className="flex items-center gap-1">
          {i > 0 && <ChevronRight className="h-3 w-3" />}
          <Link
            to={`/${segments.slice(0, i + 1).join("/")}`}
            className={i === segments.length - 1 ? "text-foreground font-medium" : "hover:text-foreground"}
          >
            {seg.replace(/-/g, " ").replace(/^\w/, (c) => c.toUpperCase())}
          </Link>
        </span>
      ))}
    </div>
  );
}

export function TopBar() {
  const navigate = useNavigate();
  const { user, logout } = useAuthStore();
  const toggleSidebar = useUIStore((s) => s.toggleSidebar);
  const [showSearch, setShowSearch] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [showUserMenu, setShowUserMenu] = useState(false);
  const [showNotifications, setShowNotifications] = useState(false);

  const { data: alerts } = useQuery({
    queryKey: ["alerts", "unread-count"],
    queryFn: () => api.get("/alerts?status=active&page_size=5").then((r) => r.data),
    refetchInterval: 30000,
  });

  const unreadCount = alerts?.length ?? 0;

  return (
    <header className="flex h-14 items-center justify-between border-b border-border bg-card px-6">
      <div className="flex items-center gap-4">
        <button onClick={toggleSidebar} className="rounded-md p-2 text-muted-foreground hover:bg-muted hover:text-foreground">
          <Menu className="h-5 w-5" />
        </button>
        <Breadcrumb />
      </div>

      <div className="flex items-center gap-2">
        {/* Search */}
        {showSearch ? (
          <div className="relative">
            <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
            <input
              autoFocus
              className="h-9 w-64 rounded-md border bg-background pl-9 pr-3 text-sm outline-none focus:ring-1 focus:ring-primary"
              placeholder="Search... (⌘K)"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              onBlur={() => { setShowSearch(false); setSearchQuery(""); }}
              onKeyDown={(e) => { if (e.key === "Escape") { setShowSearch(false); setSearchQuery(""); } }}
            />
          </div>
        ) : (
          <button onClick={() => setShowSearch(true)} className="rounded-md p-2 text-muted-foreground hover:bg-muted hover:text-foreground">
            <Search className="h-5 w-5" />
          </button>
        )}

        {/* Notifications */}
        <div className="relative">
          <button onClick={() => setShowNotifications(!showNotifications)} className="relative rounded-md p-2 text-muted-foreground hover:bg-muted hover:text-foreground">
            <Bell className="h-5 w-5" />
            {unreadCount > 0 && (
              <span className="absolute -right-0.5 -top-0.5 flex h-4 w-4 items-center justify-center rounded-full bg-destructive text-[10px] font-bold text-white">
                {unreadCount > 9 ? "9+" : unreadCount}
              </span>
            )}
          </button>
          {showNotifications && (
            <div className="absolute right-0 top-10 z-50 w-80 rounded-md border bg-popover p-2 shadow-lg" onMouseLeave={() => setShowNotifications(false)}>
              <div className="px-2 py-1 text-sm font-medium">Notifications</div>
              {alerts?.slice(0, 5).map((a: any) => (
                <div key={a.id} className="flex items-start gap-2 rounded-md px-2 py-2 hover:bg-muted cursor-pointer" onClick={() => { navigate(`/alerts/${a.id}`); setShowNotifications(false); }}>
                  <span>{a.severity === "critical" ? "🔴" : a.severity === "warning" ? "🟡" : "🔵"}</span>
                  <div className="min-w-0 flex-1">
                    <p className="text-sm truncate">{a.title ?? a.message}</p>
                    <p className="text-xs text-muted-foreground">{a.connection_name}</p>
                  </div>
                </div>
              )) ?? <p className="p-2 text-sm text-muted-foreground">No alerts</p>}
              <button className="w-full rounded-md px-2 py-1.5 text-center text-xs text-primary hover:bg-muted" onClick={() => { navigate("/alerts"); setShowNotifications(false); }}>View all alerts</button>
            </div>
          )}
        </div>

        {/* User */}
        <div className="relative">
          <button onClick={() => setShowUserMenu(!showUserMenu)} className="flex items-center gap-2 rounded-md p-2 text-sm hover:bg-muted">
            <div className="flex h-7 w-7 items-center justify-center rounded-full bg-primary/10 text-primary">
              <User className="h-4 w-4" />
            </div>
            <span className="hidden sm:inline text-sm">{user?.first_name ?? user?.username ?? "User"}</span>
          </button>
          {showUserMenu && (
            <div className="absolute right-0 top-10 z-50 w-48 rounded-md border bg-popover p-1 shadow-lg" onMouseLeave={() => setShowUserMenu(false)}>
              <button className="flex w-full items-center gap-2 rounded-sm px-3 py-2 text-sm hover:bg-accent" onClick={() => { navigate("/settings/profile"); setShowUserMenu(false); }}>
                <User className="h-4 w-4" /> Profile
              </button>
              <button className="flex w-full items-center gap-2 rounded-sm px-3 py-2 text-sm hover:bg-accent text-destructive" onClick={() => { logout(); navigate("/login"); }}>
                <LogOut className="h-4 w-4" /> Logout
              </button>
            </div>
          )}
        </div>
      </div>
    </header>
  );
}
