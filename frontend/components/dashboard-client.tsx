"use client";

import Image from "next/image";
import { useCallback, useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import TimeStamp from "@/components/time-stamp";

type TabKey = "daily" | "monthly" | "yearly";
type SectionKey = "reports" | "employee-settings" | "notifications" | "smtp" | "hr-users";

type AuthMe = {
  id: number;
  role: "admin" | "hr";
  username: string;
  email: string;
};

type Employee = {
  emp_id: number | string;
  employee_id?: number | string;
  card_no: string;
  employee_name: string;
  department?: string | null;
};

type DailyReport = {
  employee_name: string;
  card_no: string;
  department?: string | null;
  date: string;
  first_in: string | null;
  last_out: string | null;
  duration_minutes: number | null;
  duration_hhmm: string | null;
};

type MonthlyRecord = {
  date: string;
  first_in: string | null;
  last_out: string | null;
  duration_minutes: number | null;
  duration_hhmm: string | null;
};

type MonthlyReport = {
  employee_name: string;
  card_no: string;
  department?: string | null;
  month: string;
  records: MonthlyRecord[];
  total_days: number;
  total_minutes: number;
  total_duration_hhmm: string | null;
  total_duration_readable: string | null;
};

type YearlyRecord = {
  month: string;
  worked_days: number;
  total_minutes: number;
  average_minutes_per_day: number | null;
  average_duration_hhmm: string | null;
  total_duration_hhmm: string | null;
  total_duration_readable: string | null;
};

type YearlyReport = {
  employee_name: string;
  card_no: string;
  department?: string | null;
  year: string;
  months: YearlyRecord[];
  total_worked_days: number;
  total_minutes: number;
  total_duration_hhmm: string | null;
  total_duration_readable: string | null;
};

type EmployeeSetting = {
  emp_id: number | string;
  card_no: string;
  employee_name: string;
  employee_email: string;
  work_start_time: string;
  work_end_time: string;
  late_grace_minutes: number;
  early_grace_minutes: number;
  notify_employee: boolean;
  notify_cc_override: string;
  updated_at: string | null;
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

type HRUser = {
  id: number;
  email: string;
  username: string;
  role: string;
  is_active: boolean;
  created_at: string;
  updated_at: string;
  last_login_at: string | null;
};

function todayIsoDate(): string {
  const now = new Date();
  const yyyy = String(now.getFullYear());
  const mm = String(now.getMonth() + 1).padStart(2, "0");
  const dd = String(now.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function toMinutesLabel(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "N/A";
  }
  const hours = Math.floor(value / 60);
  const minutes = value % 60;
  return `${hours.toString().padStart(2, "0")}:${minutes.toString().padStart(2, "0")}`;
}

function durationLabel(hhmm: string | null | undefined, minutes: number | null | undefined): string {
  if (hhmm && hhmm.trim()) {
    return hhmm;
  }
  return toMinutesLabel(minutes);
}

function readableDurationFromMinutes(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "N/A";
  }

  const hours = Math.floor(value / 60);
  const minutes = value % 60;
  if (hours <= 0) {
    return `${minutes.toString().padStart(2, "0")} Mins`;
  }
  return `${hours} Hrs ${minutes.toString().padStart(2, "0")} Mins`;
}

function readableDurationFromHhmm(value: string | null | undefined): string | null {
  if (!value) {
    return null;
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }

  const match = /^(\d+):(\d{2})$/.exec(trimmed);
  if (!match) {
    return null;
  }

  const hours = Number.parseInt(match[1], 10);
  const minutes = Number.parseInt(match[2], 10);
  if (Number.isNaN(hours) || Number.isNaN(minutes)) {
    return null;
  }
  if (hours <= 0) {
    return `${minutes.toString().padStart(2, "0")} Mins`;
  }
  return `${hours} Hrs ${minutes.toString().padStart(2, "0")} Mins`;
}

function totalDurationLabel(
  readable: string | null | undefined,
  hhmm: string | null | undefined,
  minutes: number | null | undefined
): string {
  if (readable && readable.trim()) {
    return readable;
  }

  const parsed = readableDurationFromHhmm(hhmm);
  if (parsed) {
    return parsed;
  }

  return readableDurationFromMinutes(minutes);
}

const SECTION_LABELS: Record<SectionKey, string> = {
  reports: "Reports",
  "employee-settings": "Employee Settings",
  notifications: "Notifications",
  smtp: "SMTP Settings",
  "hr-users": "HR Users"
};

export default function DashboardClient() {
  const router = useRouter();
  const today = useMemo(() => todayIsoDate(), []);

  const [me, setMe] = useState<AuthMe | null>(null);
  const [section, setSection] = useState<SectionKey>("reports");

  const [search, setSearch] = useState("");
  const [employees, setEmployees] = useState<Employee[]>([]);
  const [selectedCardNo, setSelectedCardNo] = useState("");

  const [tab, setTab] = useState<TabKey>("daily");
  const [dailyDate, setDailyDate] = useState(today);
  const [monthValue, setMonthValue] = useState(today.slice(0, 7));
  const [yearValue, setYearValue] = useState(today.slice(0, 4));

  const [dailyReport, setDailyReport] = useState<DailyReport | null>(null);
  const [monthlyReport, setMonthlyReport] = useState<MonthlyReport | null>(null);
  const [yearlyReport, setYearlyReport] = useState<YearlyReport | null>(null);

  const [employeeLoading, setEmployeeLoading] = useState(false);
  const [reportLoading, setReportLoading] = useState(false);
  const [globalLoading, setGlobalLoading] = useState(true);
  const [error, setError] = useState("");

  const [employeeSetting, setEmployeeSetting] = useState<EmployeeSetting | null>(null);
  const [settingLoading, setSettingLoading] = useState(false);
  const [settingSaving, setSettingSaving] = useState(false);
  const [settingMessage, setSettingMessage] = useState("");

  const [notifyDate, setNotifyDate] = useState(today);
  const [notifyRunning, setNotifyRunning] = useState(false);
  const [notifyResult, setNotifyResult] = useState<NotificationRunResponse | null>(null);

  const [smtpForm, setSmtpForm] = useState<SMTPSettings>({
    host: "",
    port: 587,
    username: "",
    password: "",
    from_email: "",
    from_name: "Oilchem HR Admin",
    use_tls: true,
    use_ssl: false,
    cc_list: "",
    updated_at: null
  });
  const [smtpLoading, setSmtpLoading] = useState(false);
  const [smtpSaving, setSmtpSaving] = useState(false);
  const [smtpMessage, setSmtpMessage] = useState("");

  const [hrUsers, setHrUsers] = useState<HRUser[]>([]);
  const [hrLoading, setHrLoading] = useState(false);
  const [hrMessage, setHrMessage] = useState("");
  const [newHrUser, setNewHrUser] = useState({ email: "", username: "", temp_password: "" });

  const visibleSections = useMemo(() => {
    if (me?.role === "admin") {
      return ["reports", "employee-settings", "notifications", "smtp", "hr-users"] as SectionKey[];
    }
    return ["reports", "employee-settings", "notifications"] as SectionKey[];
  }, [me?.role]);

  const handleUnauthorized = useCallback(
    async (status: number): Promise<boolean> => {
      if (status !== 401) {
        return false;
      }
      await fetch("/api/auth/logout", { method: "POST" });
      router.push("/login");
      router.refresh();
      return true;
    },
    [router]
  );

  async function loadMe() {
    setGlobalLoading(true);
    try {
      const response = await fetch("/api/proxy/auth/me", { cache: "no-store" });
      if (await handleUnauthorized(response.status)) {
        return;
      }
      if (!response.ok) {
        throw new Error("Failed to load session");
      }
      const payload = (await response.json()) as AuthMe;
      setMe(payload);
      if (payload.role !== "admin" && (section === "smtp" || section === "hr-users")) {
        setSection("reports");
      }
    } catch (err) {
      setError((err as Error).message || "Failed to load session");
    } finally {
      setGlobalLoading(false);
    }
  }

  useEffect(() => {
    loadMe();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    const timer = setTimeout(async () => {
      setEmployeeLoading(true);
      setError("");

      try {
        const response = await fetch(`/api/proxy/employees?search=${encodeURIComponent(search)}`, {
          cache: "no-store",
          signal: controller.signal
        });

        if (await handleUnauthorized(response.status)) {
          return;
        }

        if (!response.ok) {
          const payload = (await response.json().catch(() => null)) as { detail?: string } | null;
          throw new Error(payload?.detail ?? "Failed to load employees");
        }

        const payload = (await response.json()) as { employees: Employee[] };
        setEmployees(payload.employees);

        if (!payload.employees.length) {
          setSelectedCardNo("");
          return;
        }

        setSelectedCardNo((current) => {
          if (!current) {
            return payload.employees[0].card_no;
          }
          return payload.employees.some((item) => item.card_no === current)
            ? current
            : payload.employees[0].card_no;
        });
      } catch (err) {
        if ((err as Error).name !== "AbortError") {
          setError((err as Error).message || "Failed to load employees");
        }
      } finally {
        setEmployeeLoading(false);
      }
    }, 250);

    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [handleUnauthorized, search]);

  useEffect(() => {
    if (!selectedCardNo || section !== "reports") {
      return;
    }

    let cancelled = false;

    async function loadReport() {
      setReportLoading(true);
      setError("");

      const params = new URLSearchParams({ card_no: selectedCardNo });
      if (tab === "daily") {
        params.set("date", dailyDate);
      }
      if (tab === "monthly") {
        params.set("month", monthValue);
      }
      if (tab === "yearly") {
        params.set("year", yearValue);
      }

      const endpoint = `/api/proxy/reports/${tab}?${params.toString()}`;

      try {
        const response = await fetch(endpoint, { cache: "no-store" });

        if (await handleUnauthorized(response.status)) {
          return;
        }

        if (!response.ok) {
          const payload = (await response.json().catch(() => null)) as { detail?: string } | null;
          throw new Error(payload?.detail ?? "Failed to load report");
        }

        const payload = await response.json();
        if (cancelled) {
          return;
        }

        if (tab === "daily") {
          setDailyReport(payload as DailyReport);
        }
        if (tab === "monthly") {
          setMonthlyReport(payload as MonthlyReport);
        }
        if (tab === "yearly") {
          setYearlyReport(payload as YearlyReport);
        }
      } catch (err) {
        if (!cancelled) {
          setError((err as Error).message || "Failed to load report");
        }
      } finally {
        if (!cancelled) {
          setReportLoading(false);
        }
      }
    }

    loadReport();

    return () => {
      cancelled = true;
    };
  }, [dailyDate, handleUnauthorized, monthValue, selectedCardNo, section, tab, yearValue]);

  const loadEmployeeSetting = useCallback(async () => {
    if (!selectedCardNo) {
      setEmployeeSetting(null);
      return;
    }

    setSettingLoading(true);
    setSettingMessage("");

    try {
      const response = await fetch(`/api/proxy/employee-settings?search=${encodeURIComponent(selectedCardNo)}`, {
        cache: "no-store"
      });

      if (await handleUnauthorized(response.status)) {
        return;
      }

      if (!response.ok) {
        const payload = (await response.json().catch(() => null)) as { detail?: string } | null;
        throw new Error(payload?.detail ?? "Failed to load employee settings");
      }

      const payload = (await response.json()) as { employees: EmployeeSetting[] };
      const row = payload.employees.find((item) => item.card_no === selectedCardNo) ?? null;
      setEmployeeSetting(row);
    } catch (err) {
      setError((err as Error).message || "Failed to load employee settings");
    } finally {
      setSettingLoading(false);
    }
  }, [handleUnauthorized, selectedCardNo]);

  useEffect(() => {
    if (section === "employee-settings" && selectedCardNo) {
      loadEmployeeSetting();
    }
  }, [loadEmployeeSetting, section, selectedCardNo]);

  useEffect(() => {
    if (section !== "smtp" || me?.role !== "admin") {
      return;
    }

    let cancelled = false;

    async function loadSmtp() {
      setSmtpLoading(true);
      setSmtpMessage("");

      try {
        const response = await fetch("/api/proxy/admin/smtp-settings", { cache: "no-store" });
        if (await handleUnauthorized(response.status)) {
          return;
        }
        if (!response.ok) {
          const payload = (await response.json().catch(() => null)) as { detail?: string } | null;
          throw new Error(payload?.detail ?? "Failed to load SMTP settings");
        }

        const payload = (await response.json()) as Omit<SMTPSettings, "password">;
        if (cancelled) {
          return;
        }
        setSmtpForm((current) => ({ ...current, ...payload, password: "" }));
      } catch (err) {
        if (!cancelled) {
          setError((err as Error).message || "Failed to load SMTP settings");
        }
      } finally {
        if (!cancelled) {
          setSmtpLoading(false);
        }
      }
    }

    loadSmtp();

    return () => {
      cancelled = true;
    };
  }, [handleUnauthorized, me?.role, section]);

  const loadHrUsers = useCallback(async () => {
    if (me?.role !== "admin") {
      return;
    }

    setHrLoading(true);
    setHrMessage("");

    try {
      const response = await fetch("/api/proxy/admin/hr-users", { cache: "no-store" });
      if (await handleUnauthorized(response.status)) {
        return;
      }
      if (!response.ok) {
        const payload = (await response.json().catch(() => null)) as { detail?: string } | null;
        throw new Error(payload?.detail ?? "Failed to load HR users");
      }
      const payload = (await response.json()) as { users: HRUser[] };
      setHrUsers(payload.users);
    } catch (err) {
      setError((err as Error).message || "Failed to load HR users");
    } finally {
      setHrLoading(false);
    }
  }, [handleUnauthorized, me?.role]);

  useEffect(() => {
    if (section === "hr-users" && me?.role === "admin") {
      loadHrUsers();
    }
  }, [loadHrUsers, me?.role, section]);

  async function logout() {
    await fetch("/api/auth/logout", { method: "POST" });
    router.push("/login");
    router.refresh();
  }

  const exportUrl = useMemo(() => {
    if (!selectedCardNo) {
      return "";
    }

    const params = new URLSearchParams({ card_no: selectedCardNo });
    if (tab === "daily") {
      params.set("date", dailyDate);
    }
    if (tab === "monthly") {
      params.set("month", monthValue);
    }
    if (tab === "yearly") {
      params.set("year", yearValue);
    }

    return `/api/proxy/export/${tab}.pdf?${params.toString()}`;
  }, [dailyDate, monthValue, selectedCardNo, tab, yearValue]);

  const selectedEmployee = employees.find((employee) => employee.card_no === selectedCardNo);
  const selectedDepartment =
    String(selectedEmployee?.department ?? "").trim() ||
    (tab === "daily"
      ? dailyReport?.department
      : tab === "monthly"
      ? monthlyReport?.department
      : yearlyReport?.department);
  const selectedEmployeeId = String(selectedEmployee?.employee_id ?? selectedEmployee?.emp_id ?? "").trim();
  const selectedEmployeeMeta = [
    selectedEmployeeId ? `EmpID: ${selectedEmployeeId}` : "",
    selectedDepartment ? selectedDepartment : "",
    selectedEmployee?.card_no ? selectedEmployee.card_no : ""
  ].filter(Boolean);

  async function saveEmployeeSetting() {
    if (!selectedCardNo || !selectedEmployee) {
      return;
    }

    setSettingSaving(true);
    setSettingMessage("");

    try {
      const response = await fetch(`/api/proxy/employee-settings/${encodeURIComponent(selectedCardNo)}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          emp_id: selectedEmployee.emp_id,
          employee_name: selectedEmployee.employee_name,
          employee_email: employeeSetting?.employee_email ?? "",
          work_start_time: employeeSetting?.work_start_time ?? "09:00",
          work_end_time: employeeSetting?.work_end_time ?? "18:00",
          late_grace_minutes: employeeSetting?.late_grace_minutes ?? 0,
          early_grace_minutes: employeeSetting?.early_grace_minutes ?? 0,
          notify_employee: employeeSetting?.notify_employee ?? false,
          notify_cc_override: employeeSetting?.notify_cc_override ?? ""
        })
      });

      if (await handleUnauthorized(response.status)) {
        return;
      }

      if (!response.ok) {
        const payload = (await response.json().catch(() => null)) as { detail?: string } | null;
        throw new Error(payload?.detail ?? "Failed to save employee settings");
      }

      const payload = (await response.json()) as EmployeeSetting;
      setEmployeeSetting(payload);
      setSettingMessage("Employee settings saved.");
    } catch (err) {
      setSettingMessage((err as Error).message || "Failed to save employee settings");
    } finally {
      setSettingSaving(false);
    }
  }

  async function runNotification(scope: "selected" | "all") {
    setNotifyRunning(true);
    setNotifyResult(null);
    setError("");

    try {
      const params = new URLSearchParams({ date: notifyDate });
      if (scope === "selected") {
        if (!selectedCardNo) {
          throw new Error("Select an employee to run selected notification.");
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
        const payload = (await response.json().catch(() => null)) as { detail?: string } | null;
        throw new Error(payload?.detail ?? "Failed to run notifications");
      }

      const payload = (await response.json()) as NotificationRunResponse;
      setNotifyResult(payload);
    } catch (err) {
      setError((err as Error).message || "Failed to run notifications");
    } finally {
      setNotifyRunning(false);
    }
  }

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
        const payload = (await response.json().catch(() => null)) as { detail?: string } | null;
        throw new Error(payload?.detail ?? "Failed to save SMTP settings");
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

  async function createHrUser(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setHrMessage("");

    try {
      const response = await fetch("/api/proxy/admin/hr-users", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(newHrUser)
      });

      if (await handleUnauthorized(response.status)) {
        return;
      }

      if (!response.ok) {
        const payload = (await response.json().catch(() => null)) as { detail?: string } | null;
        throw new Error(payload?.detail ?? "Failed to create HR user");
      }

      setNewHrUser({ email: "", username: "", temp_password: "" });
      setHrMessage("HR user created.");
      await loadHrUsers();
    } catch (err) {
      setHrMessage((err as Error).message || "Failed to create HR user");
    }
  }

  async function toggleHrUser(user: HRUser) {
    setHrMessage("");

    try {
      const response = await fetch(`/api/proxy/admin/hr-users/${user.id}/active`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ is_active: !user.is_active })
      });

      if (await handleUnauthorized(response.status)) {
        return;
      }

      if (!response.ok) {
        const payload = (await response.json().catch(() => null)) as { detail?: string } | null;
        throw new Error(payload?.detail ?? "Failed to update HR user");
      }

      await loadHrUsers();
      setHrMessage(`User ${user.username} ${user.is_active ? "disabled" : "enabled"}.`);
    } catch (err) {
      setHrMessage((err as Error).message || "Failed to update HR user");
    }
  }

  async function sendResetLink(user: HRUser) {
    setHrMessage("");

    try {
      const response = await fetch(`/api/proxy/admin/hr-users/${user.id}/reset-link`, {
        method: "POST"
      });

      if (await handleUnauthorized(response.status)) {
        return;
      }

      if (!response.ok) {
        const payload = (await response.json().catch(() => null)) as { detail?: string } | null;
        throw new Error(payload?.detail ?? "Failed to generate reset link");
      }

      const payload = (await response.json()) as { reset_url: string };
      setHrMessage(`Reset link generated for ${user.username}: ${payload.reset_url}`);
    } catch (err) {
      setHrMessage((err as Error).message || "Failed to generate reset link");
    }
  }

  async function setTempPassword(user: HRUser) {
    const tempPassword = window.prompt(`Set a temporary password for ${user.username}:`);
    if (!tempPassword) {
      return;
    }

    setHrMessage("");

    try {
      const response = await fetch(`/api/proxy/admin/hr-users/${user.id}/set-password`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ temp_password: tempPassword })
      });

      if (await handleUnauthorized(response.status)) {
        return;
      }

      if (!response.ok) {
        const payload = (await response.json().catch(() => null)) as { detail?: string } | null;
        throw new Error(payload?.detail ?? "Failed to set temporary password");
      }

      setHrMessage(`Temporary password updated for ${user.username}.`);
    } catch (err) {
      setHrMessage((err as Error).message || "Failed to set temporary password");
    }
  }

  if (globalLoading) {
    return (
      <main className="mx-auto flex min-h-screen max-w-4xl items-center justify-center px-4">
        <p className="text-sm text-zinc-400">Loading session...</p>
      </main>
    );
  }

  return (
    <main className="mx-auto min-h-screen max-w-7xl px-4 py-6 sm:px-8">
      <header className="mb-6 flex flex-wrap items-center justify-between gap-4">
        <div className="flex items-center gap-3">
          <Image
            src="/logo.png"
            alt="Oilchem"
            width={160}
            height={36}
            className="h-8 w-auto object-contain brightness-110 drop-shadow-sm"
            priority
          />
          <div>
            <p className="text-xs uppercase tracking-[0.3em] text-zinc-500">admin.hse-oilchem.com</p>
            <h1 className="mt-1 text-2xl font-semibold tracking-tight text-zinc-100">Oilchem Entry/Exit Dashboard</h1>
          </div>
        </div>

        <div className="flex items-center gap-3">
          {me ? (
            <span className="rounded-full border border-cyan-400/50 bg-cyan-500/10 px-3 py-1 text-xs font-semibold uppercase tracking-widest text-cyan-200">
              {me.role}
            </span>
          ) : null}
          <button
            type="button"
            onClick={logout}
            className="rounded-lg border border-zinc-700 bg-zinc-900 px-4 py-2 text-sm text-zinc-100 transition hover:border-zinc-500"
          >
            Logout
          </button>
        </div>
      </header>

      {error ? (
        <div className="mb-4 rounded-xl border border-rose-400/40 bg-rose-950/30 px-4 py-3 text-sm text-rose-300">{error}</div>
      ) : null}

      <section className="grid gap-6 lg:grid-cols-[300px,1fr]">
        <aside className="rounded-2xl border border-zinc-800 bg-zinc-900/70 p-4 shadow-xl backdrop-blur">
          <div className="mb-3 flex items-center justify-between">
            <div className="flex items-center gap-2">
              <Image
                src="/logo.png"
                alt="Oilchem"
                width={88}
                height={24}
                className="h-6 w-auto object-contain opacity-90 brightness-110 drop-shadow-sm"
              />
              <h2 className="text-lg font-medium text-zinc-100">Employees</h2>
            </div>
            {employeeLoading ? <span className="text-xs text-zinc-400">Loading...</span> : null}
          </div>

          <input
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            placeholder="Search by Name, Card No or EmpID"
            className="mb-3 w-full rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none transition focus:border-cyan-400"
          />

          <div className="max-h-[65vh] space-y-2 overflow-y-auto pr-1 table-scroll">
            {employees.map((employee) => {
              const active = employee.card_no === selectedCardNo;
              const employeeId = String(employee.employee_id ?? employee.emp_id ?? "").trim();
              const department = String(employee.department ?? "").trim();
              const detailParts = [employee.card_no];
              if (employeeId) {
                detailParts.push(`EmpID: ${employeeId}`);
              }
              if (department) {
                detailParts.push(department);
              }
              return (
                <button
                  key={`${employee.emp_id}-${employee.card_no}`}
                  type="button"
                  onClick={() => setSelectedCardNo(employee.card_no)}
                  className={`w-full rounded-xl border px-3 py-2 text-left text-sm transition ${
                    active
                      ? "border-cyan-400 bg-cyan-500/20 text-cyan-100"
                      : "border-zinc-800 bg-zinc-900 text-zinc-300 hover:border-zinc-600"
                  }`}
                >
                  <p className="font-medium leading-tight">{employee.employee_name}</p>
                  <p className={`mt-1 text-xs ${active ? "text-cyan-200/80" : "text-zinc-500"}`}>{detailParts.join(" • ")}</p>
                </button>
              );
            })}
            {!employeeLoading && !employees.length ? (
              <p className="rounded-lg border border-dashed border-zinc-800 p-3 text-sm text-zinc-500">No employees found.</p>
            ) : null}
          </div>
        </aside>

        <section className="rounded-2xl border border-zinc-800 bg-zinc-900/70 p-4 shadow-xl backdrop-blur">
          <div className="mb-4 flex flex-wrap items-center justify-between gap-4">
            <div>
              <p className="text-xs uppercase tracking-[0.2em] text-zinc-500">Selected Employee</p>
              <h2 className="mt-1 text-xl font-medium text-zinc-100">
                {selectedEmployee?.employee_name ?? dailyReport?.employee_name ?? "None"}
              </h2>
              {selectedEmployeeMeta.length ? <p className="mt-1 text-sm text-zinc-400">{selectedEmployeeMeta.join(" • ")}</p> : null}
              {me ? <p className="mt-1 text-xs text-zinc-500">Logged in as {me.username}</p> : null}
            </div>

            {section === "reports" ? (
              <button
                type="button"
                disabled={!exportUrl || reportLoading}
                onClick={() => window.open(exportUrl, "_blank", "noopener,noreferrer")}
                className="flex items-center gap-2 rounded-lg border border-zinc-700 bg-zinc-900 px-4 py-2 text-sm text-zinc-100 transition hover:border-cyan-400 disabled:cursor-not-allowed disabled:opacity-50"
              >
                <Image
                  src="/logo.png"
                  alt=""
                  width={16}
                  height={16}
                  className="h-4 w-auto object-contain opacity-90 brightness-110 drop-shadow-sm"
                />
                Export {tab.toUpperCase()} PDF
              </button>
            ) : null}
          </div>

          <div className="mb-4 flex flex-wrap gap-2">
            {visibleSections.map((key) => (
              <button
                key={key}
                type="button"
                onClick={() => setSection(key)}
                className={`rounded-lg px-4 py-2 text-sm font-medium transition ${
                  section === key
                    ? "bg-zinc-100 text-zinc-950"
                    : "border border-zinc-700 bg-zinc-900 text-zinc-300 hover:border-zinc-500"
                }`}
              >
                {SECTION_LABELS[key]}
              </button>
            ))}
          </div>

          {section === "reports" ? (
            <>
              <div className="mb-4 flex flex-wrap gap-2">
                {(["daily", "monthly", "yearly"] as TabKey[]).map((tabKey) => (
                  <button
                    key={tabKey}
                    type="button"
                    onClick={() => setTab(tabKey)}
                    className={`rounded-lg px-4 py-2 text-sm font-medium transition ${
                      tab === tabKey
                        ? "bg-cyan-500 text-zinc-950"
                        : "border border-zinc-700 bg-zinc-900 text-zinc-300 hover:border-zinc-500"
                    }`}
                  >
                    {tabKey[0].toUpperCase() + tabKey.slice(1)}
                  </button>
                ))}
              </div>

              {tab === "daily" ? (
                <div className="space-y-4">
                  <label className="block max-w-xs">
                    <span className="mb-2 block text-sm text-zinc-300">Date</span>
                    <input
                      type="date"
                      value={dailyDate}
                      onChange={(event) => setDailyDate(event.target.value)}
                      className="w-full rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-cyan-400"
                    />
                  </label>

                  <div className="grid gap-3 sm:grid-cols-3">
                    <article className="rounded-xl border border-zinc-800 bg-zinc-950/60 p-4">
                      <p className="text-xs uppercase tracking-[0.18em] text-zinc-500">First IN</p>
                      <TimeStamp value={dailyReport?.first_in ?? null} className="mt-2 block text-lg text-zinc-100" />
                    </article>
                    <article className="rounded-xl border border-zinc-800 bg-zinc-950/60 p-4">
                      <p className="text-xs uppercase tracking-[0.18em] text-zinc-500">Last OUT</p>
                      <TimeStamp value={dailyReport?.last_out ?? null} className="mt-2 block text-lg text-zinc-100" />
                    </article>
                    <article className="rounded-xl border border-zinc-800 bg-zinc-950/60 p-4">
                      <p className="text-xs uppercase tracking-[0.18em] text-zinc-500">Duration</p>
                      <p className="mt-2 text-lg text-zinc-100">
                        {durationLabel(dailyReport?.duration_hhmm, dailyReport?.duration_minutes)}
                      </p>
                    </article>
                  </div>
                </div>
              ) : null}

              {tab === "monthly" ? (
                <div className="space-y-4">
                  <label className="block max-w-xs">
                    <span className="mb-2 block text-sm text-zinc-300">Month</span>
                    <input
                      type="month"
                      value={monthValue}
                      onChange={(event) => setMonthValue(event.target.value)}
                      className="w-full rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-cyan-400"
                    />
                  </label>

                  <div className="overflow-x-auto rounded-xl border border-zinc-800 table-scroll">
                    <table className="min-w-full text-sm">
                      <thead className="bg-zinc-950/80 text-zinc-300">
                        <tr>
                          <th className="px-3 py-2 text-left font-medium">Date</th>
                          <th className="px-3 py-2 text-left font-medium">First IN</th>
                          <th className="px-3 py-2 text-left font-medium">Last OUT</th>
                          <th className="px-3 py-2 text-left font-medium">Duration</th>
                        </tr>
                      </thead>
                      <tbody>
                        {monthlyReport?.records.map((record) => (
                          <tr key={record.date} className="border-t border-zinc-800">
                            <td className="px-3 py-2 text-zinc-200">{record.date}</td>
                            <td className="px-3 py-2 text-zinc-200">
                              <TimeStamp value={record.first_in} showDate={false} className="text-zinc-200" />
                            </td>
                            <td className="px-3 py-2 text-zinc-200">
                              <TimeStamp value={record.last_out} showDate={false} className="text-zinc-200" />
                            </td>
                            <td className="px-3 py-2 text-zinc-200">
                              {durationLabel(record.duration_hhmm, record.duration_minutes)}
                            </td>
                          </tr>
                        ))}
                        {!monthlyReport?.records.length ? (
                          <tr>
                            <td colSpan={4} className="px-3 py-6 text-center text-zinc-500">
                              No records for this month.
                            </td>
                          </tr>
                        ) : null}
                      </tbody>
                    </table>
                  </div>

                  <div className="grid gap-3 sm:grid-cols-2">
                    <article className="rounded-xl border border-zinc-800 bg-zinc-950/60 p-4">
                      <p className="text-xs uppercase tracking-[0.18em] text-zinc-500">Total Logged Days</p>
                      <p className="mt-2 text-lg text-zinc-100">{monthlyReport?.total_days ?? 0}</p>
                    </article>
                    <article className="rounded-xl border border-zinc-800 bg-zinc-950/60 p-4">
                      <p className="text-xs uppercase tracking-[0.18em] text-zinc-500">Total Duration</p>
                      <p className="mt-2 text-lg text-zinc-100">
                        {totalDurationLabel(
                          monthlyReport?.total_duration_readable,
                          monthlyReport?.total_duration_hhmm,
                          monthlyReport?.total_minutes
                        )}
                      </p>
                    </article>
                  </div>
                </div>
              ) : null}

              {tab === "yearly" ? (
                <div className="space-y-4">
                  <label className="block max-w-xs">
                    <span className="mb-2 block text-sm text-zinc-300">Year</span>
                    <input
                      type="number"
                      min={1900}
                      max={2100}
                      value={yearValue}
                      onChange={(event) => setYearValue(event.target.value)}
                      className="w-full rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-cyan-400"
                    />
                  </label>

                  <div className="overflow-x-auto rounded-xl border border-zinc-800 table-scroll">
                    <table className="min-w-full text-sm">
                      <thead className="bg-zinc-950/80 text-zinc-300">
                        <tr>
                          <th className="px-3 py-2 text-left font-medium">Month</th>
                          <th className="px-3 py-2 text-left font-medium">Worked Days</th>
                          <th className="px-3 py-2 text-left font-medium">Total Duration</th>
                          <th className="px-3 py-2 text-left font-medium">Average / Day</th>
                        </tr>
                      </thead>
                      <tbody>
                        {yearlyReport?.months.map((item) => (
                          <tr key={item.month} className="border-t border-zinc-800">
                            <td className="px-3 py-2 text-zinc-200">{item.month}</td>
                            <td className="px-3 py-2 text-zinc-200">{item.worked_days}</td>
                            <td className="px-3 py-2 text-zinc-200">
                              {totalDurationLabel(item.total_duration_readable, item.total_duration_hhmm, item.total_minutes)}
                            </td>
                            <td className="px-3 py-2 text-zinc-200">
                              {durationLabel(item.average_duration_hhmm, item.average_minutes_per_day)}
                            </td>
                          </tr>
                        ))}
                        {!yearlyReport?.months.length ? (
                          <tr>
                            <td colSpan={4} className="px-3 py-6 text-center text-zinc-500">
                              No records for this year.
                            </td>
                          </tr>
                        ) : null}
                      </tbody>
                    </table>
                  </div>

                  <div className="grid gap-3 sm:grid-cols-2">
                    <article className="rounded-xl border border-zinc-800 bg-zinc-950/60 p-4">
                      <p className="text-xs uppercase tracking-[0.18em] text-zinc-500">Total Worked Days</p>
                      <p className="mt-2 text-lg text-zinc-100">{yearlyReport?.total_worked_days ?? 0}</p>
                    </article>
                    <article className="rounded-xl border border-zinc-800 bg-zinc-950/60 p-4">
                      <p className="text-xs uppercase tracking-[0.18em] text-zinc-500">Total Duration</p>
                      <p className="mt-2 text-lg text-zinc-100">
                        {totalDurationLabel(
                          yearlyReport?.total_duration_readable,
                          yearlyReport?.total_duration_hhmm,
                          yearlyReport?.total_minutes
                        )}
                      </p>
                    </article>
                  </div>
                </div>
              ) : null}

              {reportLoading ? <p className="mt-4 text-sm text-zinc-400">Loading report...</p> : null}
            </>
          ) : null}

          {section === "employee-settings" ? (
            <div className="space-y-4">
              <div className="rounded-xl border border-zinc-800 bg-zinc-950/40 p-4">
                <p className="text-sm text-zinc-300">
                  Configure per-employee email, shift timings, grace windows, and notification behavior.
                </p>
              </div>

              {settingLoading ? <p className="text-sm text-zinc-400">Loading employee settings...</p> : null}

              {!selectedEmployee ? (
                <p className="rounded-lg border border-dashed border-zinc-800 p-3 text-sm text-zinc-500">
                  Select an employee from the left list.
                </p>
              ) : (
                <div className="grid gap-4 sm:grid-cols-2">
                  <label className="block">
                    <span className="mb-2 block text-sm text-zinc-300">Employee Email</span>
                    <input
                      value={employeeSetting?.employee_email ?? ""}
                      onChange={(event) =>
                        setEmployeeSetting((current) => ({
                          ...(current ?? {
                            emp_id: selectedEmployee.emp_id,
                            card_no: selectedEmployee.card_no,
                            employee_name: selectedEmployee.employee_name,
                            employee_email: "",
                            work_start_time: "09:00",
                            work_end_time: "18:00",
                            late_grace_minutes: 0,
                            early_grace_minutes: 0,
                            notify_employee: false,
                            notify_cc_override: "",
                            updated_at: null
                          }),
                          employee_email: event.target.value
                        }))
                      }
                      className="w-full rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-cyan-400"
                      placeholder="employee@company.com"
                    />
                  </label>

                  <label className="block">
                    <span className="mb-2 block text-sm text-zinc-300">CC Override</span>
                    <input
                      value={employeeSetting?.notify_cc_override ?? ""}
                      onChange={(event) =>
                        setEmployeeSetting((current) => ({
                          ...(current ?? {
                            emp_id: selectedEmployee.emp_id,
                            card_no: selectedEmployee.card_no,
                            employee_name: selectedEmployee.employee_name,
                            employee_email: "",
                            work_start_time: "09:00",
                            work_end_time: "18:00",
                            late_grace_minutes: 0,
                            early_grace_minutes: 0,
                            notify_employee: false,
                            notify_cc_override: "",
                            updated_at: null
                          }),
                          notify_cc_override: event.target.value
                        }))
                      }
                      className="w-full rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-cyan-400"
                      placeholder="cc1@example.com, cc2@example.com"
                    />
                  </label>

                  <label className="block">
                    <span className="mb-2 block text-sm text-zinc-300">Work Start Time</span>
                    <input
                      type="time"
                      value={employeeSetting?.work_start_time ?? "09:00"}
                      onChange={(event) =>
                        setEmployeeSetting((current) => ({
                          ...(current ?? {
                            emp_id: selectedEmployee.emp_id,
                            card_no: selectedEmployee.card_no,
                            employee_name: selectedEmployee.employee_name,
                            employee_email: "",
                            work_start_time: "09:00",
                            work_end_time: "18:00",
                            late_grace_minutes: 0,
                            early_grace_minutes: 0,
                            notify_employee: false,
                            notify_cc_override: "",
                            updated_at: null
                          }),
                          work_start_time: event.target.value
                        }))
                      }
                      className="w-full rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-cyan-400"
                    />
                  </label>

                  <label className="block">
                    <span className="mb-2 block text-sm text-zinc-300">Work End Time</span>
                    <input
                      type="time"
                      value={employeeSetting?.work_end_time ?? "18:00"}
                      onChange={(event) =>
                        setEmployeeSetting((current) => ({
                          ...(current ?? {
                            emp_id: selectedEmployee.emp_id,
                            card_no: selectedEmployee.card_no,
                            employee_name: selectedEmployee.employee_name,
                            employee_email: "",
                            work_start_time: "09:00",
                            work_end_time: "18:00",
                            late_grace_minutes: 0,
                            early_grace_minutes: 0,
                            notify_employee: false,
                            notify_cc_override: "",
                            updated_at: null
                          }),
                          work_end_time: event.target.value
                        }))
                      }
                      className="w-full rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-cyan-400"
                    />
                  </label>

                  <label className="block">
                    <span className="mb-2 block text-sm text-zinc-300">Late Grace Minutes</span>
                    <input
                      type="number"
                      min={0}
                      value={employeeSetting?.late_grace_minutes ?? 0}
                      onChange={(event) =>
                        setEmployeeSetting((current) => ({
                          ...(current ?? {
                            emp_id: selectedEmployee.emp_id,
                            card_no: selectedEmployee.card_no,
                            employee_name: selectedEmployee.employee_name,
                            employee_email: "",
                            work_start_time: "09:00",
                            work_end_time: "18:00",
                            late_grace_minutes: 0,
                            early_grace_minutes: 0,
                            notify_employee: false,
                            notify_cc_override: "",
                            updated_at: null
                          }),
                          late_grace_minutes: Number(event.target.value || 0)
                        }))
                      }
                      className="w-full rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-cyan-400"
                    />
                  </label>

                  <label className="block">
                    <span className="mb-2 block text-sm text-zinc-300">Early Grace Minutes</span>
                    <input
                      type="number"
                      min={0}
                      value={employeeSetting?.early_grace_minutes ?? 0}
                      onChange={(event) =>
                        setEmployeeSetting((current) => ({
                          ...(current ?? {
                            emp_id: selectedEmployee.emp_id,
                            card_no: selectedEmployee.card_no,
                            employee_name: selectedEmployee.employee_name,
                            employee_email: "",
                            work_start_time: "09:00",
                            work_end_time: "18:00",
                            late_grace_minutes: 0,
                            early_grace_minutes: 0,
                            notify_employee: false,
                            notify_cc_override: "",
                            updated_at: null
                          }),
                          early_grace_minutes: Number(event.target.value || 0)
                        }))
                      }
                      className="w-full rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-cyan-400"
                    />
                  </label>
                </div>
              )}

              {selectedEmployee ? (
                <label className="inline-flex items-center gap-2 rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-200">
                  <input
                    type="checkbox"
                    checked={employeeSetting?.notify_employee ?? false}
                    onChange={(event) =>
                      setEmployeeSetting((current) => ({
                        ...(current ?? {
                          emp_id: selectedEmployee.emp_id,
                          card_no: selectedEmployee.card_no,
                          employee_name: selectedEmployee.employee_name,
                          employee_email: "",
                          work_start_time: "09:00",
                          work_end_time: "18:00",
                          late_grace_minutes: 0,
                          early_grace_minutes: 0,
                          notify_employee: false,
                          notify_cc_override: "",
                          updated_at: null
                        }),
                        notify_employee: event.target.checked
                      }))
                    }
                  />
                  Enable notifications for this employee
                </label>
              ) : null}

              <div className="flex items-center gap-3">
                <button
                  type="button"
                  onClick={saveEmployeeSetting}
                  disabled={!selectedEmployee || settingSaving}
                  className="rounded-lg bg-cyan-500 px-4 py-2 text-sm font-semibold text-zinc-950 transition hover:bg-cyan-400 disabled:cursor-not-allowed disabled:opacity-60"
                >
                  {settingSaving ? "Saving..." : "Save Employee Settings"}
                </button>
                {settingMessage ? <p className="text-sm text-zinc-300">{settingMessage}</p> : null}
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

                <div className="flex flex-wrap gap-2">
                  <button
                    type="button"
                    onClick={() => runNotification("selected")}
                    disabled={notifyRunning || !selectedCardNo}
                    className="rounded-lg border border-cyan-400/60 bg-cyan-500/10 px-4 py-2 text-sm font-semibold text-cyan-100 transition hover:bg-cyan-500/20 disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    Send for Selected Employee
                  </button>
                  <button
                    type="button"
                    onClick={() => runNotification("all")}
                    disabled={notifyRunning}
                    className="rounded-lg border border-zinc-600 bg-zinc-900 px-4 py-2 text-sm font-semibold text-zinc-100 transition hover:border-zinc-400 disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    Send Notifications for Selected Date
                  </button>
                </div>
              </div>

              {notifyRunning ? <p className="text-sm text-zinc-400">Running notifications...</p> : null}

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

          {section === "smtp" && me?.role === "admin" ? (
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
                    onChange={(event) =>
                      setSmtpForm((current) => ({ ...current, port: Number(event.target.value || 0) }))
                    }
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
                    className="w-full rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-cyan-400"
                    placeholder="Leave blank to keep current"
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
                  <span className="mb-2 block text-sm text-zinc-300">Default CC List (comma-separated)</span>
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

          {section === "hr-users" && me?.role === "admin" ? (
            <div className="space-y-4">
              <form onSubmit={createHrUser} className="grid gap-3 rounded-xl border border-zinc-800 bg-zinc-950/50 p-4 sm:grid-cols-4">
                <input
                  value={newHrUser.email}
                  onChange={(event) => setNewHrUser((current) => ({ ...current, email: event.target.value }))}
                  placeholder="HR email"
                  className="rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-cyan-400"
                  required
                />
                <input
                  value={newHrUser.username}
                  onChange={(event) => setNewHrUser((current) => ({ ...current, username: event.target.value }))}
                  placeholder="Username"
                  className="rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-cyan-400"
                  required
                />
                <input
                  type="password"
                  value={newHrUser.temp_password}
                  onChange={(event) => setNewHrUser((current) => ({ ...current, temp_password: event.target.value }))}
                  placeholder="Temp password"
                  className="rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-cyan-400"
                  required
                />
                <button
                  type="submit"
                  className="rounded-lg bg-cyan-500 px-4 py-2 text-sm font-semibold text-zinc-950 transition hover:bg-cyan-400"
                >
                  Create HR User
                </button>
              </form>

              {hrLoading ? <p className="text-sm text-zinc-400">Loading HR users...</p> : null}
              {hrMessage ? <p className="text-sm text-zinc-300">{hrMessage}</p> : null}

              <div className="overflow-x-auto rounded-xl border border-zinc-800 table-scroll">
                <table className="min-w-full text-sm">
                  <thead className="bg-zinc-950/80 text-zinc-300">
                    <tr>
                      <th className="px-3 py-2 text-left font-medium">User</th>
                      <th className="px-3 py-2 text-left font-medium">Email</th>
                      <th className="px-3 py-2 text-left font-medium">Status</th>
                      <th className="px-3 py-2 text-left font-medium">Last Login</th>
                      <th className="px-3 py-2 text-left font-medium">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {hrUsers.map((user) => (
                      <tr key={user.id} className="border-t border-zinc-800">
                        <td className="px-3 py-2 text-zinc-200">{user.username}</td>
                        <td className="px-3 py-2 text-zinc-200">{user.email}</td>
                        <td className="px-3 py-2 text-zinc-200">{user.is_active ? "Active" : "Disabled"}</td>
                        <td className="px-3 py-2 text-zinc-200">
                          <TimeStamp value={user.last_login_at} className="text-zinc-200" />
                        </td>
                        <td className="px-3 py-2 text-zinc-200">
                          <div className="flex flex-wrap gap-2">
                            <button
                              type="button"
                              onClick={() => toggleHrUser(user)}
                              className="rounded border border-zinc-600 px-2 py-1 text-xs hover:border-zinc-400"
                            >
                              {user.is_active ? "Disable" : "Enable"}
                            </button>
                            <button
                              type="button"
                              onClick={() => sendResetLink(user)}
                              className="rounded border border-zinc-600 px-2 py-1 text-xs hover:border-zinc-400"
                            >
                              Reset Link
                            </button>
                            <button
                              type="button"
                              onClick={() => setTempPassword(user)}
                              className="rounded border border-zinc-600 px-2 py-1 text-xs hover:border-zinc-400"
                            >
                              Temp Password
                            </button>
                          </div>
                        </td>
                      </tr>
                    ))}
                    {!hrUsers.length && !hrLoading ? (
                      <tr>
                        <td colSpan={5} className="px-3 py-6 text-center text-zinc-500">
                          No HR users found.
                        </td>
                      </tr>
                    ) : null}
                  </tbody>
                </table>
              </div>
            </div>
          ) : null}
        </section>
      </section>
    </main>
  );
}
