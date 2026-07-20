import { useNavigate } from "react-router-dom";
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from "@/components/ui/card";
import { useAuthStore } from "@/stores/auth-store";
import {
  User,
  Lock,
  Users,
  Shield,
  ScrollText,
  Settings2,
  Flag,
  Wrench,
  Gauge,
  Zap,
} from "lucide-react";

interface SettingsCard {
  title: string;
  description: string;
  icon: React.ElementType;
  path: string;
  requiredRole?: "admin" | "superadmin";
}

const settingsCards: SettingsCard[] = [
  { title: "Profile", description: "Manage your account information and preferences", icon: User, path: "/settings/profile" },
  { title: "Change Password", description: "Update your password and security settings", icon: Lock, path: "/settings/change-password" },
  { title: "Users", description: "Manage users, invite members, and assign roles", icon: Users, path: "/settings/users", requiredRole: "admin" },
  { title: "Roles", description: "Define roles and configure permission levels", icon: Shield, path: "/settings/roles", requiredRole: "admin" },
  { title: "Audit Logs", description: "View activity history and system events", icon: ScrollText, path: "/settings/audit-logs", requiredRole: "admin" },
  { title: "System Config", description: "Manage platform-wide configuration parameters", icon: Settings2, path: "/settings/system-config", requiredRole: "superadmin" },
  { title: "Feature Flags", description: "Control feature rollouts and toggle capabilities", icon: Flag, path: "/settings/feature-flags", requiredRole: "superadmin" },
  { title: "Maintenance Windows", description: "Schedule maintenance periods and manage downtime", icon: Wrench, path: "/settings/maintenance-windows", requiredRole: "superadmin" },
  { title: "Resource Quotas", description: "Monitor and configure tenant resource limits", icon: Gauge, path: "/settings/resource-quotas", requiredRole: "superadmin" },
  { title: "Spark Configuration", description: "Configure Spark cluster settings, resources, and deployment options per environment", icon: Zap, path: "/settings/spark-config", requiredRole: "superadmin" },
];

export function SettingsPage() {
  const navigate = useNavigate();
  const user = useAuthStore((s) => s.user);
  const userRole = user?.role ?? "viewer";

  const hasAccess = (requiredRole?: "admin" | "superadmin") => {
    if (!requiredRole) return true;
    if (requiredRole === "admin") return userRole === "admin" || userRole === "superadmin";
    return userRole === "superadmin";
  };

  const visibleCards = settingsCards.filter((c) => hasAccess(c.requiredRole));

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold">Settings</h1>
        <p className="text-muted-foreground">Manage your account, team, and platform configuration</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {visibleCards.map((card) => (
          <Card
            key={card.path}
            className="cursor-pointer hover:border-primary/50 hover:shadow-md transition-all"
            onClick={() => navigate(card.path)}
          >
            <CardHeader className="flex flex-row items-center gap-4 pb-2">
              <div className="rounded-lg bg-muted p-2">
                <card.icon className="h-5 w-5 text-muted-foreground" />
              </div>
              <div>
                <CardTitle className="text-base">{card.title}</CardTitle>
              </div>
            </CardHeader>
            <CardContent>
              <CardDescription>{card.description}</CardDescription>
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  );
}
