"use client";

import Link from "next/link";
import { useEffect, useState } from "react";

import AppHeader from "@/components/app-header";
import TimeStamp from "@/components/time-stamp";
import { useAuthUser } from "@/components/use-auth-user";

type DashboardSummary = {
  totalEmployees: number;
  inCount: number;
  outCount: number;
  unknownCount?: number | null;
  generatedAt: string;
};

const EMPTY_SUMMARY: DashboardSummary = {
  totalEmployees: 0,
  inCount: 0,
  outCount: 0,
  unknownCount: 0,
  generatedAt: ""
};

export default function HomeDashboardClient() {
  const { me, loading, error, setError, logout, handleUnauthorized } = useAuthUser();
  const [summary, setSummary] = useState<DashboardSummary>(EMPTY_SUMMARY);
  const [summaryLoading, setSummaryLoading] = useState(true);

  useEffect(() => {
    if (!me) {
      return;
    }

    let cancelled = false;

    async function loadSummary() {
      setSummaryLoading(true);
      setError("");

      try {
        const response = await fetch("/api/proxy/dashboard/summary", { cache: "no-store" });
        if (await handleUnauthorized(response.status)) {
          return;
        }
        if (!response.ok) {
          const payload = (await response.json().catch(() => null)) as { detail?: string; error?: string } | null;
          throw new Error(payload?.error ?? payload?.detail ?? "Failed to load dashboard summary");
        }

        const payload = (await response.json()) as DashboardSummary;
        if (!cancelled) {
          setSummary(payload);
        }
      } catch (err) {
        if (!cancelled) {
          setError((err as Error).message || "Failed to load dashboard summary");
        }
      } finally {
        if (!cancelled) {
          setSummaryLoading(false);
        }
      }
    }

    loadSummary();

    return () => {
      cancelled = true;
    };
  }, [handleUnauthorized, me, setError]);

  if (loading || !me) {
    return (
      <main className="mx-auto flex min-h-screen max-w-4xl items-center justify-center px-4">
        <p className="text-sm text-zinc-400">Loading dashboard...</p>
      </main>
    );
  }

  const cards = [
    { label: "IN", value: summary.inCount, accent: "text-emerald-300" },
    { label: "OUT", value: summary.outCount, accent: "text-amber-300" },
    { label: "TOTAL EMPLOYEES", value: summary.totalEmployees, accent: "text-cyan-200" }
  ];

  if (summary.unknownCount !== undefined && summary.unknownCount !== null) {
    cards.push({ label: "UNKNOWN", value: summary.unknownCount, accent: "text-zinc-200" });
  }

  return (
    <main className="mx-auto min-h-screen max-w-7xl px-4 py-6 sm:px-8">
      <AppHeader
        me={me}
        title="Home Dashboard"
        subtitle="At-a-glance entry/exit state"
        onLogout={logout}
      />

      {error ? (
        <div className="mb-4 rounded-xl border border-rose-400/40 bg-rose-950/30 px-4 py-3 text-sm text-rose-300">{error}</div>
      ) : null}

      <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        {cards.map((card) => (
          <article key={card.label} className="rounded-2xl border border-zinc-800 bg-zinc-900/70 p-5 shadow-xl backdrop-blur">
            <p className="text-xs uppercase tracking-[0.18em] text-zinc-500">{card.label}</p>
            <p className={`mt-3 text-4xl font-semibold ${card.accent}`}>{summaryLoading ? "--" : card.value}</p>
          </article>
        ))}
      </section>

      <section className="mt-6 rounded-2xl border border-zinc-800 bg-zinc-900/70 p-5 shadow-xl backdrop-blur">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <p className="text-sm text-zinc-300">Quick Actions</p>
            {summary.generatedAt ? (
              <p className="mt-1 text-xs text-zinc-500">
                Generated: <TimeStamp value={summary.generatedAt} className="text-zinc-400" />
              </p>
            ) : null}
          </div>

          <div className="flex flex-wrap gap-2">
            <Link
              href="/reports"
              className="rounded-lg bg-cyan-500 px-4 py-2 text-sm font-semibold text-zinc-950 transition hover:bg-cyan-400"
            >
              Open Reports
            </Link>
          </div>
        </div>
      </section>
    </main>
  );
}
