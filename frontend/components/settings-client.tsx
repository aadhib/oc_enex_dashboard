"use client";

import Link from "next/link";
import { type FormEvent, useCallback, useEffect, useMemo, useState } from "react";

import AppHeader from "@/components/app-header";
import { useAuthUser } from "@/components/use-auth-user";

type SettingsSection = "smtp" | "users" | "notifications";
type SettingsHref = "/settings/smtp" | "/settings/users" | "/settings/notifications";

type Employee = {
  emp_id: number | string;
  card_no: string;
  employee_name: string;
};

type SMTPSettings = {
  host: string;
  port: number;
  username: string;
  password: string;
  from_email: string;
  from_name: string;
  use_tls: boolean;
  use_ssl: boolean;
  cc_list: string;
  updated_at: string | null;
};

type UserRole = "admin" | "inspector";

type UserItem = {
  id: number;
  email: string;
  username: string;
  role: UserRole;
  is_active: boolean;
  created_at: string;
  updated_at: string;
  last_login_at: string | null;
};

type NotificationResult = {
  card_no: string;
  employee_name: string;
  status: string;
  notice_type: string | null;
  to_email: string | null;
  error: string | null;
};

type NotificationRunResponse = {
  date: string;
  total_targets: number;
  sent_count: number;
  skipped_count: number;
  failed_count: number;
  results: NotificationResult[];
};

type SettingsClientProps = {
  section: SettingsSection;
};

function todayIsoDate(): string {
  const now = new Date();
  const yyyy = String(now.getFullYear());
  const mm = String(now.getMonth() + 1).padStart(2, "0");
  const dd = String(now.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

const SETTINGS_TABS: Array<{ key: SettingsSection; href: SettingsHref; label: string }> = [
  { key: "smtp", href: "/settings/smtp", label: "SMTP Settings" },
  { key: "users", href: "/settings/users", label: "Users" },
  { key: "notifications", href: "/settings/notifications", label: "Notifications" }
];

export default function SettingsClient({ section }: SettingsClientProps) {
  const { me, loading, error, setError, logout, handleUnauthorized } = useAuthUser();

  const [smtpForm, setSmtpForm] = useState<SMTPSettings>({
    host: "",
    port: 587,
    username: "",
    password: "",
    from_email: "",
    from_name: "Oilchem Entry/Exit Admin",
    use_tls: true,
    use_ssl: false,
    cc_list: "",
    updated_at: null
  });
  const [smtpLoading, setSmtpLoading] = useState(false);
  const [smtpSaving, setSmtpSaving] = useState(false);
  const [smtpMessage, setSmtpMessage] = useState("");

  const [users, setUsers] = useState<UserItem[]>([]);
  const [usersLoading, setUsersLoading] = useState(false);
  const [usersMessage, setUsersMessage] = useState("");
  const [newUser, setNewUser] = useState({ email: "", username: "", password: "", role: "inspector" as UserRole });

  const [notifyDate, setNotifyDate] = useState(todayIsoDate());
  const [notifyRunning, setNotifyRunning] = useState(false);
  const [notifyResult, setNotifyResult] = useState<NotificationRunResponse | null>(null);
  const [notifyMessage, setNotifyMessage] = useState("");
  const [employeeSearch, setEmployeeSearch] = useState("");
  const [employees, setEmployees] = useState<Employee[]>([]);
  const [selectedCardNo, setSelectedCardNo] = useState("");
  const [employeeLoading, setEmployeeLoading] = useState(false);

  const selectedEmployee = useMemo(
    () => employees.find((item) => item.card_no === selectedCardNo) ?? null,
    [employees, selectedCardNo]
  );

  const loadSmtp = useCallback(async () => {
    if (me?.role !== "admin") {
      return;
    }

    setSmtpLoading(true);
    setSmtpMessage("");
    setError("");

    try {
      const response = await fetch("/api/proxy/admin/smtp-settings", { cache: "no-store" });
      if (await handleUnauthorized(response.status)) {
        return;
      }
      if (!response.ok) {
        const payload = (await response.json().catch(() => null)) as { detail?: string; error?: string } | null;
        throw new Error(payload?.error ?? payload?.detail ?? "Failed to load SMTP settings");
      }

      const payload = (await response.json()) as Omit<SMTPSettings, "password">;
      setSmtpForm((current) => ({ ...current, ...payload, password: "" }));
    } catch (err) {
      setError((err as Error).message || "Failed to load SMTP settings");
    } finally {
      setSmtpLoading(false);
    }
  }, [handleUnauthorized, me?.role, setError]);

  const loadUsers = useCallback(async () => {
    if (me?.role !== "admin") {
      return;
    }

    setUsersLoading(true);
    setUsersMessage("");

    try {
      const response = await fetch("/api/proxy/users", { cache: "no-store" });
      if (await handleUnauthorized(response.status)) {
        return;
      }
      if (!response.ok) {
        const payload = (await response.json().catch(() => null)) as { detail?: string; error?: string } | null;
        throw new Error(payload?.error ?? payload?.detail ?? "Failed to load users");
      }

      const payload = (await response.json()) as { users: UserItem[] };
      setUsers(payload.users);
    } catch (err) {
      setError((err as Error).message || "Failed to load users");
    } finally {
      setUsersLoading(false);
    }
  }, [handleUnauthorized, me?.role, setError]);

  useEffect(() => {
    if (section === "smtp" && me?.role === "admin") {
      loadSmtp();
    }
  }, [loadSmtp, me?.role, section]);

  useEffect(() => {
    if (section === "users" && me?.role === "admin") {
      loadUsers();
    }
  }, [loadUsers, me?.role, section]);

  useEffect(() => {
    if (section !== "notifications" || me?.role !== "admin") {
      return;
    }

    const controller = new AbortController();
    const timer = setTimeout(async () => {
      setEmployeeLoading(true);
      setNotifyMessage("");

      try {
        const response = await fetch(`/api/proxy/employees?search=${encodeURIComponent(employeeSearch)}`, {
          cache: "no-store",
          signal: controller.signal
        });

        if (await handleUnauthorized(response.status)) {
          return;
        }

        if (!response.ok) {
          const payload = (await response.json().catch(() => null)) as { detail?: string; error?: string } | null;
          throw new Error(payload?.error ?? payload?.detail ?? "Failed to load employees");
        }

        const payload = (await response.json()) as { employees: Employee[] };
        setEmployees(payload.employees);
        setSelectedCardNo((current) => {
          if (!payload.employees.length) {
            return "";
          }
          if (!current) {
            return payload.employees[0].card_no;
          }
          return payload.employees.some((item) => item.card_no === current) ? current : payload.employees[0].card_no;
        });
      } catch (err) {
        if ((err as Error).name !== "AbortError") {
          setNotifyMessage((err as Error).message || "Failed to load employees");
        }
      } finally {
        setEmployeeLoading(false);
      }
    }, 250);

    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [employeeSearch, handleUnauthorized, me?.role, section]);

  async function saveSmtpSettings() {
    setSmtpSaving(true);
    setSmtpMessage("");

    try {
      const response = await fetch("/api/proxy/admin/smtp-settings", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(smtpForm)
      });

      if (await handleUnauthorized(response.status)) {
        return;
      }

      if (!response.ok) {
        const payload = (await response.json().catch(() => null)) as { detail?: string; error?: string } | null;
        throw new Error(payload?.error ?? payload?.detail ?? "Failed to save SMTP settings");
      }

      const payload = (await response.json()) as Omit<SMTPSettings, "password">;
      setSmtpForm((current) => ({ ...current, ...payload, password: "" }));
      setSmtpMessage("SMTP settings saved.");
    } catch (err) {
      setSmtpMessage((err as Error).message || "Failed to save SMTP settings");
    } finally {
      setSmtpSaving(false);
    }
  }

  async function createUser(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setUsersMessage("");

    try {
      const response = await fetch("/api/proxy/users", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(newUser)
      });

      if (await handleUnauthorized(response.status)) {
        return;
      }

      if (!response.ok) {
        const payload = (await response.json().catch(() => null)) as { detail?: string; error?: string } | null;
        throw new Error(payload?.error ?? payload?.detail ?? "Failed to create user");
      }

      setNewUser({ email: "", username: "", password: "", role: "inspector" });
      setUsersMessage("User created.");
      await loadUsers();
    } catch (err) {
      setUsersMessage((err as Error).message || "Failed to create user");
    }
  }

  async function patchUser(userId: number, payload: { role?: UserRole; is_active?: boolean; password?: string }) {
    const response = await fetch(`/api/proxy/users/${userId}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });

    if (await handleUnauthorized(response.status)) {
      return false;
    }

    if (!response.ok) {
      const body = (await response.json().catch(() => null)) as { detail?: string; error?: string } | null;
      throw new Error(body?.error ?? body?.detail ?? "Failed to update user");
    }

    return true;
  }

  async function changeUserRole(user: UserItem, role: UserRole) {
    setUsersMessage("");
    try {
      await patchUser(user.id, { role });
      setUsersMessage(`Updated role for ${user.username}.`);
      await loadUsers();
    } catch (err) {
      setUsersMessage((err as Error).message || "Failed to update role");
    }
  }

  async function toggleUser(user: UserItem) {
    setUsersMessage("");
    try {
      await patchUser(user.id, { is_active: !user.is_active });
      setUsersMessage(`User ${user.username} ${user.is_active ? "disabled" : "enabled"}.`);
      await loadUsers();
    } catch (err) {
      setUsersMessage((err as Error).message || "Failed to update user status");
    }
  }

  async function resetPassword(user: UserItem) {
    const newPassword = window.prompt(`Set a temporary password for ${user.username}:`);
    if (!newPassword) {
      return;
    }

    setUsersMessage("");
    try {
      await patchUser(user.id, { password: newPassword });
      setUsersMessage(`Password updated for ${user.username}.`);
      await loadUsers();
    } catch (err) {
      setUsersMessage((err as Error).message || "Failed to update password");
    }
  }

  async function createResetLink(user: UserItem) {
    setUsersMessage("");

    try {
      const response = await fetch(`/api/proxy/users/${user.id}/reset-link`, { method: "POST" });
      if (await handleUnauthorized(response.status)) {
        return;
      }
      if (!response.ok) {
        const payload = (await response.json().catch(() => null)) as { detail?: string; error?: string } | null;
        throw new Error(payload?.error ?? payload?.detail ?? "Failed to generate reset link");
      }

      const payload = (await response.json()) as { reset_url: string };
      setUsersMessage(`Reset link generated for ${user.username}: ${payload.reset_url}`);
    } catch (err) {
      setUsersMessage((err as Error).message || "Failed to generate reset link");
    }
  }

  async function deleteUserRow(user: UserItem) {
    const confirmed = window.confirm(`Delete user ${user.username}? This action cannot be undone.`);
    if (!confirmed) {
      return;
    }

    setUsersMessage("");
    try {
      const response = await fetch(`/api/proxy/users/${user.id}`, { method: "DELETE" });
      if (await handleUnauthorized(response.status)) {
        return;
      }
      if (!response.ok) {
        const payload = (await response.json().catch(() => null)) as { detail?: string; error?: string } | null;
        throw new Error(payload?.error ?? payload?.detail ?? "Failed to delete user");
      }

      setUsersMessage(`Deleted user ${user.username}.`);
      await loadUsers();
    } catch (err) {
      setUsersMessage((err as Error).message || "Failed to delete user");
    }
  }

  async function runNotifications(scope: "selected" | "all") {
    setNotifyRunning(true);
    setNotifyResult(null);
    setNotifyMessage("");

    try {
      const params = new URLSearchParams({ date: notifyDate });
      if (scope === "selected") {
        if (!selectedCardNo) {
          throw new Error("Select an employee to send selected notification.");
        }
        params.set("card_no", selectedCardNo);
      }

      const response = await fetch(`/api/proxy/notifications/run?${params.toString()}`, {
        method: "POST"
      });

      if (await handleUnauthorized(response.status)) {
        return;
      }

      if (!response.ok) {
        const payload = (await response.json().catch(() => null)) as { detail?: string; error?: string } | null;
        throw new Error(payload?.error ?? payload?.detail ?? "Failed to run notifications");
      }

      const payload = (await response.json()) as NotificationRunResponse;
      setNotifyResult(payload);
    } catch (err) {
      setNotifyMessage((err as Error).message || "Failed to run notifications");
    } finally {
      setNotifyRunning(false);
    }
  }

  if (loading || !me) {
    return (
      <main className="mx-auto flex min-h-screen max-w-4xl items-center justify-center px-4">
        <p className="text-sm text-zinc-400">Loading settings...</p>
      </main>
    );
  }

  if (me.role !== "admin") {
    return (
      <main className="mx-auto min-h-screen max-w-4xl px-4 py-10 sm:px-8">
        <AppHeader me={me} title="Access denied" subtitle="Admin-only area" onLogout={logout} />
        <section className="rounded-2xl border border-zinc-800 bg-zinc-900/70 p-6 shadow-xl backdrop-blur">
          <p className="text-zinc-200">You do not have access to this page.</p>
          <Link
            href="/"
            className="mt-4 inline-flex rounded-lg bg-cyan-500 px-4 py-2 text-sm font-semibold text-zinc-950 transition hover:bg-cyan-400"
          >
            Back to Home
          </Link>
        </section>
      </main>
    );
  }

  return (
    <main className="mx-auto min-h-screen max-w-7xl px-4 py-6 sm:px-8">
      <AppHeader me={me} title="Settings" subtitle="Entry/Exit Admin Tools" onLogout={logout} />

      {error ? (
        <div className="mb-4 rounded-xl border border-rose-400/40 bg-rose-950/30 px-4 py-3 text-sm text-rose-300">{error}</div>
      ) : null}

      <section className="rounded-2xl border border-zinc-800 bg-zinc-900/70 p-4 shadow-xl backdrop-blur">
        <div className="mb-5 flex flex-wrap gap-2">
          {SETTINGS_TABS.map((tab) => (
            <Link
              key={tab.key}
              href={tab.href}
              className={`rounded-lg px-4 py-2 text-sm font-medium transition ${
                tab.key === section
                  ? "bg-zinc-100 text-zinc-950"
                  : "border border-zinc-700 bg-zinc-900 text-zinc-300 hover:border-zinc-500"
              }`}
            >
              {tab.label}
            </Link>
          ))}
        </div>

        {section === "smtp" ? (
          <div className="space-y-4">
            {smtpLoading ? <p className="text-sm text-zinc-400">Loading SMTP settings...</p> : null}

            <div className="grid gap-4 sm:grid-cols-2">
              <label className="block">
                <span className="mb-2 block text-sm text-zinc-300">SMTP Host</span>
                <input
                  value={smtpForm.host}
                  onChange={(event) => setSmtpForm((current) => ({ ...current, host: event.target.value }))}
                  className="w-full rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-cyan-400"
                />
              </label>

              <label className="block">
                <span className="mb-2 block text-sm text-zinc-300">SMTP Port</span>
                <input
                  type="number"
                  value={smtpForm.port}
                  onChange={(event) => setSmtpForm((current) => ({ ...current, port: Number(event.target.value || 0) }))}
                  className="w-full rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-cyan-400"
                />
              </label>

              <label className="block">
                <span className="mb-2 block text-sm text-zinc-300">SMTP Username</span>
                <input
                  value={smtpForm.username}
                  onChange={(event) => setSmtpForm((current) => ({ ...current, username: event.target.value }))}
                  className="w-full rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-cyan-400"
                />
              </label>

              <label className="block">
                <span className="mb-2 block text-sm text-zinc-300">SMTP Password</span>
                <input
                  type="password"
                  value={smtpForm.password}
                  onChange={(event) => setSmtpForm((current) => ({ ...current, password: event.target.value }))}
                  placeholder="Leave blank to keep current"
                  className="w-full rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-cyan-400"
                />
              </label>

              <label className="block">
                <span className="mb-2 block text-sm text-zinc-300">From Email</span>
                <input
                  value={smtpForm.from_email}
                  onChange={(event) => setSmtpForm((current) => ({ ...current, from_email: event.target.value }))}
                  className="w-full rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-cyan-400"
                />
              </label>

              <label className="block">
                <span className="mb-2 block text-sm text-zinc-300">From Name</span>
                <input
                  value={smtpForm.from_name}
                  onChange={(event) => setSmtpForm((current) => ({ ...current, from_name: event.target.value }))}
                  className="w-full rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-cyan-400"
                />
              </label>

              <label className="block sm:col-span-2">
                <span className="mb-2 block text-sm text-zinc-300">Default CC List</span>
                <input
                  value={smtpForm.cc_list}
                  onChange={(event) => setSmtpForm((current) => ({ ...current, cc_list: event.target.value }))}
                  className="w-full rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-cyan-400"
                />
              </label>
            </div>

            <div className="flex flex-wrap gap-3">
              <label className="inline-flex items-center gap-2 rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-200">
                <input
                  type="checkbox"
                  checked={smtpForm.use_tls}
                  onChange={(event) => setSmtpForm((current) => ({ ...current, use_tls: event.target.checked }))}
                />
                Use TLS
              </label>
              <label className="inline-flex items-center gap-2 rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-200">
                <input
                  type="checkbox"
                  checked={smtpForm.use_ssl}
                  onChange={(event) => setSmtpForm((current) => ({ ...current, use_ssl: event.target.checked }))}
                />
                Use SSL
              </label>
            </div>

            <div className="flex items-center gap-3">
              <button
                type="button"
                onClick={saveSmtpSettings}
                disabled={smtpSaving}
                className="rounded-lg bg-cyan-500 px-4 py-2 text-sm font-semibold text-zinc-950 transition hover:bg-cyan-400 disabled:cursor-not-allowed disabled:opacity-60"
              >
                {smtpSaving ? "Saving..." : "Save SMTP Settings"}
              </button>
              {smtpMessage ? <p className="text-sm text-zinc-300">{smtpMessage}</p> : null}
            </div>
          </div>
        ) : null}

        {section === "users" ? (
          <div className="space-y-4">
            <form onSubmit={createUser} className="grid gap-3 rounded-xl border border-zinc-800 bg-zinc-950/50 p-4 sm:grid-cols-5">
              <input
                value={newUser.email}
                onChange={(event) => setNewUser((current) => ({ ...current, email: event.target.value }))}
                placeholder="User email"
                className="rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-cyan-400"
                required
              />
              <input
                value={newUser.username}
                onChange={(event) => setNewUser((current) => ({ ...current, username: event.target.value }))}
                placeholder="Username"
                className="rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-cyan-400"
                required
              />
              <input
                type="password"
                value={newUser.password}
                onChange={(event) => setNewUser((current) => ({ ...current, password: event.target.value }))}
                placeholder="Password"
                className="rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-cyan-400"
                required
              />
              <select
                value={newUser.role}
                onChange={(event) => setNewUser((current) => ({ ...current, role: event.target.value as UserRole }))}
                className="rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-cyan-400"
              >
                <option value="inspector">Inspector</option>
                <option value="admin">Admin</option>
              </select>
              <button
                type="submit"
                className="rounded-lg bg-cyan-500 px-4 py-2 text-sm font-semibold text-zinc-950 transition hover:bg-cyan-400"
              >
                Create User
              </button>
            </form>

            {usersLoading ? <p className="text-sm text-zinc-400">Loading users...</p> : null}
            {usersMessage ? <p className="text-sm text-zinc-300">{usersMessage}</p> : null}

            <div className="overflow-x-auto rounded-xl border border-zinc-800 table-scroll">
              <table className="min-w-full text-sm">
                <thead className="bg-zinc-950/80 text-zinc-300">
                  <tr>
                    <th className="px-3 py-2 text-left font-medium">User</th>
                    <th className="px-3 py-2 text-left font-medium">Email</th>
                    <th className="px-3 py-2 text-left font-medium">Role</th>
                    <th className="px-3 py-2 text-left font-medium">Status</th>
                    <th className="px-3 py-2 text-left font-medium">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {users.map((user) => (
                    <tr key={user.id} className="border-t border-zinc-800">
                      <td className="px-3 py-2 text-zinc-200">{user.username}</td>
                      <td className="px-3 py-2 text-zinc-200">{user.email}</td>
                      <td className="px-3 py-2 text-zinc-200">
                        <select
                          value={user.role}
                          onChange={(event) => changeUserRole(user, event.target.value as UserRole)}
                          className="rounded border border-zinc-700 bg-zinc-950 px-2 py-1 text-xs text-zinc-100"
                        >
                          <option value="admin">Admin</option>
                          <option value="inspector">Inspector</option>
                        </select>
                      </td>
                      <td className="px-3 py-2 text-zinc-200">{user.is_active ? "Active" : "Disabled"}</td>
                      <td className="px-3 py-2 text-zinc-200">
                        <div className="flex flex-wrap gap-2">
                          <button
                            type="button"
                            onClick={() => toggleUser(user)}
                            className="rounded border border-zinc-600 px-2 py-1 text-xs hover:border-zinc-400"
                          >
                            {user.is_active ? "Disable" : "Enable"}
                          </button>
                          <button
                            type="button"
                            onClick={() => createResetLink(user)}
                            className="rounded border border-zinc-600 px-2 py-1 text-xs hover:border-zinc-400"
                          >
                            Reset Link
                          </button>
                          <button
                            type="button"
                            onClick={() => resetPassword(user)}
                            className="rounded border border-zinc-600 px-2 py-1 text-xs hover:border-zinc-400"
                          >
                            Temp Password
                          </button>
                          <button
                            type="button"
                            onClick={() => deleteUserRow(user)}
                            className="rounded border border-rose-500/60 px-2 py-1 text-xs text-rose-300 hover:border-rose-400"
                          >
                            Delete
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                  {!users.length && !usersLoading ? (
                    <tr>
                      <td colSpan={5} className="px-3 py-6 text-center text-zinc-500">
                        No users found.
                      </td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
          </div>
        ) : null}

        {section === "notifications" ? (
          <div className="space-y-4">
            <div className="grid gap-4 sm:grid-cols-[220px,1fr] sm:items-end">
              <label className="block">
                <span className="mb-2 block text-sm text-zinc-300">Notification Date</span>
                <input
                  type="date"
                  value={notifyDate}
                  onChange={(event) => setNotifyDate(event.target.value)}
                  className="w-full rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-cyan-400"
                />
              </label>

              <div className="grid gap-2 sm:grid-cols-[1fr,220px]">
                <input
                  value={employeeSearch}
                  onChange={(event) => setEmployeeSearch(event.target.value)}
                  placeholder="Search employee by name/card"
                  className="w-full rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-cyan-400"
                />
                <select
                  value={selectedCardNo}
                  onChange={(event) => setSelectedCardNo(event.target.value)}
                  className="w-full rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-cyan-400"
                >
                  {employees.map((employee) => (
                    <option key={employee.card_no} value={employee.card_no}>
                      {employee.employee_name} ({employee.card_no})
                    </option>
                  ))}
                </select>
              </div>
            </div>

            <div className="flex flex-wrap gap-2">
              <button
                type="button"
                onClick={() => runNotifications("selected")}
                disabled={notifyRunning || !selectedCardNo}
                className="rounded-lg border border-cyan-400/60 bg-cyan-500/10 px-4 py-2 text-sm font-semibold text-cyan-100 transition hover:bg-cyan-500/20 disabled:cursor-not-allowed disabled:opacity-50"
              >
                Send for Selected Employee
              </button>
              <button
                type="button"
                onClick={() => runNotifications("all")}
                disabled={notifyRunning}
                className="rounded-lg border border-zinc-600 bg-zinc-900 px-4 py-2 text-sm font-semibold text-zinc-100 transition hover:border-zinc-400 disabled:cursor-not-allowed disabled:opacity-50"
              >
                Send Notifications for Selected Date
              </button>
              {employeeLoading ? <p className="self-center text-xs text-zinc-500">Loading employees...</p> : null}
            </div>

            {notifyRunning ? <p className="text-sm text-zinc-400">Running notifications...</p> : null}
            {notifyMessage ? <p className="text-sm text-zinc-300">{notifyMessage}</p> : null}
            {selectedEmployee ? <p className="text-xs text-zinc-500">Selected: {selectedEmployee.employee_name}</p> : null}

            {notifyResult ? (
              <div className="space-y-3">
                <div className="grid gap-3 sm:grid-cols-4">
                  <article className="rounded-xl border border-zinc-800 bg-zinc-950/60 p-4">
                    <p className="text-xs uppercase tracking-[0.18em] text-zinc-500">Targets</p>
                    <p className="mt-2 text-lg text-zinc-100">{notifyResult.total_targets}</p>
                  </article>
                  <article className="rounded-xl border border-zinc-800 bg-zinc-950/60 p-4">
                    <p className="text-xs uppercase tracking-[0.18em] text-zinc-500">Sent</p>
                    <p className="mt-2 text-lg text-emerald-300">{notifyResult.sent_count}</p>
                  </article>
                  <article className="rounded-xl border border-zinc-800 bg-zinc-950/60 p-4">
                    <p className="text-xs uppercase tracking-[0.18em] text-zinc-500">Skipped</p>
                    <p className="mt-2 text-lg text-amber-300">{notifyResult.skipped_count}</p>
                  </article>
                  <article className="rounded-xl border border-zinc-800 bg-zinc-950/60 p-4">
                    <p className="text-xs uppercase tracking-[0.18em] text-zinc-500">Failed</p>
                    <p className="mt-2 text-lg text-rose-300">{notifyResult.failed_count}</p>
                  </article>
                </div>

                <div className="overflow-x-auto rounded-xl border border-zinc-800 table-scroll">
                  <table className="min-w-full text-sm">
                    <thead className="bg-zinc-950/80 text-zinc-300">
                      <tr>
                        <th className="px-3 py-2 text-left font-medium">Employee</th>
                        <th className="px-3 py-2 text-left font-medium">CardNo</th>
                        <th className="px-3 py-2 text-left font-medium">Type</th>
                        <th className="px-3 py-2 text-left font-medium">Status</th>
                        <th className="px-3 py-2 text-left font-medium">Recipient</th>
                        <th className="px-3 py-2 text-left font-medium">Error</th>
                      </tr>
                    </thead>
                    <tbody>
                      {notifyResult.results.map((item, index) => (
                        <tr key={`${item.card_no}-${index}`} className="border-t border-zinc-800">
                          <td className="px-3 py-2 text-zinc-200">{item.employee_name}</td>
                          <td className="px-3 py-2 text-zinc-200">{item.card_no}</td>
                          <td className="px-3 py-2 text-zinc-200">{item.notice_type ?? "-"}</td>
                          <td className="px-3 py-2 text-zinc-200">{item.status}</td>
                          <td className="px-3 py-2 text-zinc-200">{item.to_email ?? "-"}</td>
                          <td className="px-3 py-2 text-rose-300">{item.error ?? "-"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            ) : null}
          </div>
        ) : null}
      </section>
    </main>
  );
}
