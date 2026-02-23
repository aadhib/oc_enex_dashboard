"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

import AppHeader from "@/components/app-header";
import EmployeesPanel from "@/components/employees-panel";
import TimeStamp from "@/components/time-stamp";
import { useAuthUser } from "@/components/use-auth-user";

type TabKey = "daily" | "monthly" | "yearly";
type SectionKey = "reports" | "employee-settings";

type Employee = {
  emp_id: number | string;
  card_no: string;
  employee_name: string;
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
  duration?: string | null;
  rows?: Array<{
    date: string;
    in: string;
    out: string;
    duration: string;
    in_raw?: string | null;
    out_raw?: string | null;
    duration_minutes?: number | null;
  }>;
  transactions?: Array<{
    type: "IN" | "OUT" | string;
    time?: string | null;
    timestamp?: string | null;
    inferred?: boolean;
  }>;
  intervals?: Array<{
    date?: string | null;
    in?: string | null;
    out?: string | null;
    in_time?: string | null;
    out_time?: string | null;
    in_duration_minutes?: number | null;
    in_duration_hhmm?: string | null;
  }>;
  total_in_minutes?: number | null;
  total_out_minutes?: number | null;
  total_in?: string | null;
  total_out?: string | null;
  totalInMinutes?: number | null;
  totalOutMinutes?: number | null;
  totalInHHMM?: string | null;
  totalOutHHMM?: string | null;
  notes?: string[];
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
  totalInMinutes?: number | null;
  totalOutMinutes?: number | null;
  totalInHHMM?: string | null;
  totalOutHHMM?: string | null;
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
  totalInMinutes?: number | null;
  totalOutMinutes?: number | null;
  totalInHHMM?: string | null;
  totalOutHHMM?: string | null;
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

function todayIsoDate(): string {
  const now = new Date();
  const yyyy = String(now.getFullYear());
  const mm = String(now.getMonth() + 1).padStart(2, "0");
  const dd = String(now.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function durationLabel(hhmm: string | null | undefined, minutes: number | null | undefined): string {
  if (hhmm && hhmm.trim()) {
    return hhmm;
  }
  if (minutes === null || minutes === undefined || Number.isNaN(minutes)) {
    return "N/A";
  }
  const hours = Math.floor(minutes / 60);
  const mins = minutes % 60;
  return `${hours.toString().padStart(2, "0")}:${mins.toString().padStart(2, "0")}`;
}

function inOutTotalLabel(
  hhmm: string | null | undefined,
  minutes: number | null | undefined
): string {
  if (hhmm && hhmm.trim()) {
    return hhmm;
  }

  if (minutes === null || minutes === undefined || Number.isNaN(minutes)) {
    return "N/A";
  }

  const hours = Math.floor(minutes / 60);
  const mins = minutes % 60;
  return `${hours.toString().padStart(2, "0")}:${mins.toString().padStart(2, "0")}`;
}

function totalDurationLabel(
  readable: string | null | undefined,
  hhmm: string | null | undefined,
  minutes: number | null | undefined
): string {
  if (readable && readable.trim()) {
    return readable;
  }

  if (hhmm && /^\d+:\d{2}$/.test(hhmm.trim())) {
    const [hoursRaw, minsRaw] = hhmm.trim().split(":");
    const hours = Number.parseInt(hoursRaw, 10);
    const mins = Number.parseInt(minsRaw, 10);
    if (!Number.isNaN(hours) && !Number.isNaN(mins)) {
      if (hours <= 0) {
        return `${String(mins).padStart(2, "0")} Mins`;
      }
      return `${hours} Hrs ${String(mins).padStart(2, "0")} Mins`;
    }
  }

  if (minutes === null || minutes === undefined || Number.isNaN(minutes)) {
    return "N/A";
  }
  const hours = Math.floor(minutes / 60);
  const mins = minutes % 60;
  if (hours <= 0) {
    return `${String(mins).padStart(2, "0")} Mins`;
  }
  return `${hours} Hrs ${String(mins).padStart(2, "0")} Mins`;
}

function buildDefaultSetting(employee: Employee): EmployeeSetting {
  return {
    emp_id: employee.emp_id,
    card_no: employee.card_no,
    employee_name: employee.employee_name,
    employee_email: "",
    work_start_time: "09:00",
    work_end_time: "18:00",
    late_grace_minutes: 0,
    early_grace_minutes: 0,
    notify_employee: false,
    notify_cc_override: "",
    updated_at: null
  };
}

export default function ReportsClient() {
  const { me, loading, error, setError, logout, handleUnauthorized } = useAuthUser();
  const today = useMemo(() => todayIsoDate(), []);

  const [section, setSection] = useState<SectionKey>("reports");
  const [tab, setTab] = useState<TabKey>("daily");

  const [search, setSearch] = useState("");
  const [employees, setEmployees] = useState<Employee[]>([]);
  const [selectedCardNo, setSelectedCardNo] = useState("");

  const [dailyDate, setDailyDate] = useState(today);
  const [monthValue, setMonthValue] = useState(today.slice(0, 7));
  const [yearValue, setYearValue] = useState(today.slice(0, 4));

  const [dailyReport, setDailyReport] = useState<DailyReport | null>(null);
  const [monthlyReport, setMonthlyReport] = useState<MonthlyReport | null>(null);
  const [yearlyReport, setYearlyReport] = useState<YearlyReport | null>(null);

  const [employeeLoading, setEmployeeLoading] = useState(false);
  const [reportLoading, setReportLoading] = useState(false);

  const [employeeSetting, setEmployeeSetting] = useState<EmployeeSetting | null>(null);
  const [settingLoading, setSettingLoading] = useState(false);
  const [settingSaving, setSettingSaving] = useState(false);
  const [settingMessage, setSettingMessage] = useState("");

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
          const payload = (await response.json().catch(() => null)) as { detail?: string; error?: string } | null;
          throw new Error(payload?.error ?? payload?.detail ?? "Failed to load employees");
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
          return payload.employees.some((item) => item.card_no === current) ? current : payload.employees[0].card_no;
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
  }, [handleUnauthorized, search, setError]);

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
          const payload = (await response.json().catch(() => null)) as { detail?: string; error?: string } | null;
          throw new Error(payload?.error ?? payload?.detail ?? "Failed to load report");
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
  }, [dailyDate, handleUnauthorized, monthValue, section, selectedCardNo, setError, tab, yearValue]);

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
        const payload = (await response.json().catch(() => null)) as { detail?: string; error?: string } | null;
        throw new Error(payload?.error ?? payload?.detail ?? "Failed to load employee settings");
      }

      const payload = (await response.json()) as { employees: EmployeeSetting[] };
      const row = payload.employees.find((item) => item.card_no === selectedCardNo) ?? null;
      setEmployeeSetting(row);
    } catch (err) {
      setError((err as Error).message || "Failed to load employee settings");
    } finally {
      setSettingLoading(false);
    }
  }, [handleUnauthorized, selectedCardNo, setError]);

  useEffect(() => {
    if (section === "employee-settings" && selectedCardNo) {
      loadEmployeeSetting();
    }
  }, [loadEmployeeSetting, section, selectedCardNo]);

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

  async function saveEmployeeSetting() {
    const selectedEmployee = employees.find((item) => item.card_no === selectedCardNo);
    if (!selectedCardNo || !selectedEmployee) {
      return;
    }

    setSettingSaving(true);
    setSettingMessage("");

    const source = employeeSetting ?? buildDefaultSetting(selectedEmployee);

    try {
      const response = await fetch(`/api/proxy/employee-settings/${encodeURIComponent(selectedCardNo)}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          emp_id: selectedEmployee.emp_id,
          employee_name: selectedEmployee.employee_name,
          employee_email: source.employee_email,
          work_start_time: source.work_start_time,
          work_end_time: source.work_end_time,
          late_grace_minutes: source.late_grace_minutes,
          early_grace_minutes: source.early_grace_minutes,
          notify_employee: source.notify_employee,
          notify_cc_override: source.notify_cc_override
        })
      });

      if (await handleUnauthorized(response.status)) {
        return;
      }

      if (!response.ok) {
        const payload = (await response.json().catch(() => null)) as { detail?: string; error?: string } | null;
        throw new Error(payload?.error ?? payload?.detail ?? "Failed to save employee settings");
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

  if (loading || !me) {
    return (
      <main className="mx-auto flex min-h-screen max-w-4xl items-center justify-center px-4">
        <p className="text-sm text-zinc-400">Loading reports...</p>
      </main>
    );
  }

  const selectedEmployee = employees.find((employee) => employee.card_no === selectedCardNo);
  const selectedDepartment =
    tab === "daily" ? dailyReport?.department : tab === "monthly" ? monthlyReport?.department : yearlyReport?.department;
  const dailyRows = dailyReport?.rows ?? [];

  return (
    <main className="mx-auto min-h-screen max-w-7xl px-4 py-6 sm:px-8">
      <AppHeader me={me} title="Reports" onLogout={logout} />

      {error ? (
        <div className="mb-4 rounded-xl border border-rose-400/40 bg-rose-950/30 px-4 py-3 text-sm text-rose-300">{error}</div>
      ) : null}

      <section className="overflow-hidden rounded-3xl border border-zinc-800/80 bg-zinc-950/35 p-4 sm:p-6">
        <div className="w-full h-[calc(100vh-120px)] min-h-0 overflow-hidden">
          <div className="h-full min-h-0 flex gap-6 items-stretch">
            <div className="w-[360px] shrink-0 h-full min-h-0">
              <EmployeesPanel
                employees={employees}
                selectedCardNo={selectedCardNo}
                search={search}
                employeeLoading={employeeLoading}
                onSearchChange={setSearch}
                onSelectCardNo={setSelectedCardNo}
              />
            </div>

            <div className="flex-1 min-w-0 h-full min-h-0 overflow-hidden">
              <section className="h-full min-h-0 overflow-y-auto rounded-2xl border border-zinc-800 bg-zinc-900/70 p-4 shadow-xl backdrop-blur">
          <div className="mb-4 flex flex-wrap items-center justify-between gap-4">
            <div>
              <p className="text-xs uppercase tracking-[0.2em] text-zinc-500">Selected Employee</p>
              <h2 className="mt-1 text-xl font-medium text-zinc-100">
                {selectedEmployee?.employee_name ?? dailyReport?.employee_name ?? "None"}
              </h2>
              {selectedEmployee?.card_no ? <p className="mt-1 text-sm text-zinc-400">{selectedEmployee.card_no}</p> : null}
              {selectedDepartment && section === "reports" ? <p className="mt-1 text-sm text-zinc-500">{selectedDepartment}</p> : null}
            </div>
            {section === "reports" ? (
              <button
                type="button"
                disabled={!exportUrl || reportLoading}
                onClick={() => window.open(exportUrl, "_blank", "noopener,noreferrer")}
                className="rounded-lg border border-zinc-700 bg-zinc-900 px-4 py-2 text-sm text-zinc-100 transition hover:border-cyan-400 disabled:cursor-not-allowed disabled:opacity-50"
              >
                Export {tab.toUpperCase()} PDF
              </button>
            ) : null}
          </div>

          <div className="mb-4 flex flex-wrap gap-2">
            <button
              type="button"
              onClick={() => setSection("reports")}
              className={`rounded-lg px-4 py-2 text-sm font-medium transition ${
                section === "reports"
                  ? "bg-zinc-100 text-zinc-950"
                  : "border border-zinc-700 bg-zinc-900 text-zinc-300 hover:border-zinc-500"
              }`}
            >
              Reports
            </button>
            <button
              type="button"
              onClick={() => setSection("employee-settings")}
              className={`rounded-lg px-4 py-2 text-sm font-medium transition ${
                section === "employee-settings"
                  ? "bg-zinc-100 text-zinc-950"
                  : "border border-zinc-700 bg-zinc-900 text-zinc-300 hover:border-zinc-500"
              }`}
            >
              Employee Settings
            </button>
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

                  <div className="overflow-x-auto rounded-xl border border-zinc-800 table-scroll">
                    <table className="min-w-full text-sm">
                      <thead className="bg-zinc-950/80 text-zinc-300">
                        <tr>
                          <th className="px-3 py-2 text-left font-medium">Date</th>
                          <th className="px-3 py-2 text-left font-medium">IN</th>
                          <th className="px-3 py-2 text-left font-medium">OUT</th>
                          <th className="px-3 py-2 text-left font-medium">Duration (HH:MM)</th>
                        </tr>
                      </thead>
                      <tbody>
                        {dailyRows.map((row, index) => (
                          <tr key={`${row.in_raw ?? row.in ?? index}-${row.out_raw ?? row.out ?? index}`} className="border-t border-zinc-800">
                            <td className="px-3 py-2 text-zinc-200">{row.date}</td>
                            <td className="px-3 py-2 text-zinc-200">
                              {row.in_raw ? (
                                <TimeStamp value={row.in_raw} showDate={false} className="text-zinc-200" />
                              ) : (
                                row.in
                              )}
                            </td>
                            <td className="px-3 py-2 text-zinc-200">
                              {row.out_raw ? (
                                <TimeStamp value={row.out_raw} showDate={false} className="text-zinc-200" />
                              ) : (
                                row.out
                              )}
                            </td>
                            <td className="px-3 py-2 text-zinc-200">
                              {row.duration ?? durationLabel(null, row.duration_minutes ?? null)}
                            </td>
                          </tr>
                        ))}
                        {!dailyRows.length ? (
                          <tr>
                            <td colSpan={4} className="px-3 py-6 text-center text-zinc-500">
                              No paired IN/OUT rows found for this date window.
                            </td>
                          </tr>
                        ) : null}
                      </tbody>
                      <tfoot>
                        <tr className="border-t border-cyan-300/20 bg-cyan-500/10">
                          <td colSpan={3} className="px-3 py-2 font-medium text-cyan-100">
                            Total In Hour
                          </td>
                          <td className="px-3 py-2 font-medium text-cyan-100">
                            {dailyReport?.total_in ??
                              inOutTotalLabel(
                                dailyReport?.totalInHHMM,
                                dailyReport?.totalInMinutes ?? dailyReport?.total_in_minutes
                              )}
                          </td>
                        </tr>
                        <tr className="border-t border-cyan-300/20 bg-cyan-500/10">
                          <td colSpan={3} className="px-3 py-2 font-medium text-cyan-100">
                            Total Out Hour
                          </td>
                          <td className="px-3 py-2 font-medium text-cyan-100">
                            {dailyReport?.total_out ??
                              inOutTotalLabel(
                                dailyReport?.totalOutHHMM,
                                dailyReport?.totalOutMinutes ?? dailyReport?.total_out_minutes
                              )}
                          </td>
                        </tr>
                      </tfoot>
                    </table>
                  </div>

                  {dailyReport?.notes?.length ? (
                    <div className="rounded-xl border border-amber-500/30 bg-amber-500/10 px-4 py-3">
                      <p className="text-xs uppercase tracking-[0.18em] text-amber-300/90">Notes</p>
                      <ul className="mt-2 space-y-1 text-sm text-amber-200/90">
                        {dailyReport.notes.map((note, index) => (
                          <li key={`${note}-${index}`}>{note}</li>
                        ))}
                      </ul>
                    </div>
                  ) : null}
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
                            <td className="px-3 py-2 text-zinc-200">{durationLabel(record.duration_hhmm, record.duration_minutes)}</td>
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
                      <tfoot>
                        <tr className="border-t border-cyan-300/20 bg-cyan-500/10">
                          <td colSpan={3} className="px-3 py-2 font-medium text-cyan-100">
                            Total In Hour
                          </td>
                          <td className="px-3 py-2 font-medium text-cyan-100">
                            {inOutTotalLabel(monthlyReport?.totalInHHMM, monthlyReport?.totalInMinutes)}
                          </td>
                        </tr>
                        <tr className="border-t border-cyan-300/20 bg-cyan-500/10">
                          <td colSpan={3} className="px-3 py-2 font-medium text-cyan-100">
                            Total Out Hour
                          </td>
                          <td className="px-3 py-2 font-medium text-cyan-100">
                            {inOutTotalLabel(monthlyReport?.totalOutHHMM, monthlyReport?.totalOutMinutes)}
                          </td>
                        </tr>
                      </tfoot>
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
                            <td className="px-3 py-2 text-zinc-200">{durationLabel(item.average_duration_hhmm, item.average_minutes_per_day)}</td>
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
                      <tfoot>
                        <tr className="border-t border-cyan-300/20 bg-cyan-500/10">
                          <td colSpan={3} className="px-3 py-2 font-medium text-cyan-100">
                            Total In Hour
                          </td>
                          <td className="px-3 py-2 font-medium text-cyan-100">
                            {inOutTotalLabel(yearlyReport?.totalInHHMM, yearlyReport?.totalInMinutes)}
                          </td>
                        </tr>
                        <tr className="border-t border-cyan-300/20 bg-cyan-500/10">
                          <td colSpan={3} className="px-3 py-2 font-medium text-cyan-100">
                            Total Out Hour
                          </td>
                          <td className="px-3 py-2 font-medium text-cyan-100">
                            {inOutTotalLabel(yearlyReport?.totalOutHHMM, yearlyReport?.totalOutMinutes)}
                          </td>
                        </tr>
                      </tfoot>
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
                      value={(employeeSetting ?? buildDefaultSetting(selectedEmployee)).employee_email}
                      onChange={(event) =>
                        setEmployeeSetting((current) => ({
                          ...(current ?? buildDefaultSetting(selectedEmployee)),
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
                      value={(employeeSetting ?? buildDefaultSetting(selectedEmployee)).notify_cc_override}
                      onChange={(event) =>
                        setEmployeeSetting((current) => ({
                          ...(current ?? buildDefaultSetting(selectedEmployee)),
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
                      value={(employeeSetting ?? buildDefaultSetting(selectedEmployee)).work_start_time}
                      onChange={(event) =>
                        setEmployeeSetting((current) => ({
                          ...(current ?? buildDefaultSetting(selectedEmployee)),
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
                      value={(employeeSetting ?? buildDefaultSetting(selectedEmployee)).work_end_time}
                      onChange={(event) =>
                        setEmployeeSetting((current) => ({
                          ...(current ?? buildDefaultSetting(selectedEmployee)),
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
                      value={(employeeSetting ?? buildDefaultSetting(selectedEmployee)).late_grace_minutes}
                      onChange={(event) =>
                        setEmployeeSetting((current) => ({
                          ...(current ?? buildDefaultSetting(selectedEmployee)),
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
                      value={(employeeSetting ?? buildDefaultSetting(selectedEmployee)).early_grace_minutes}
                      onChange={(event) =>
                        setEmployeeSetting((current) => ({
                          ...(current ?? buildDefaultSetting(selectedEmployee)),
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
                    checked={(employeeSetting ?? buildDefaultSetting(selectedEmployee)).notify_employee}
                    onChange={(event) =>
                      setEmployeeSetting((current) => ({
                        ...(current ?? buildDefaultSetting(selectedEmployee)),
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
              </section>
            </div>
          </div>
        </div>
      </section>
    </main>
  );
}
