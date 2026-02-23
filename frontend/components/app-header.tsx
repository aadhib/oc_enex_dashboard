"use client";

import Image from "next/image";
import Link from "next/link";
import { usePathname } from "next/navigation";

import type { AuthUser } from "@/components/use-auth-user";

type AppHeaderProps = {
  me: AuthUser;
  title: string;
  subtitle?: string;
  actions?: React.ReactNode;
  onLogout: () => Promise<void>;
};

export default function AppHeader({ me, title, subtitle, actions, onLogout }: AppHeaderProps) {
  const pathname = usePathname();
  const roleLabel = me.role === "admin" ? "Admin" : "Inspector";
  const headerAriaLabel = subtitle ? `${title} - ${subtitle}` : title;
  const navClass = (active: boolean) =>
    `rounded-lg border px-3 py-2 text-sm transition ${
      active
        ? "border-zinc-400 bg-zinc-100 text-zinc-950"
        : "border-zinc-700 bg-zinc-900 text-zinc-200 hover:border-zinc-500"
    }`;

  return (
    <header className="mb-6 flex flex-wrap items-center justify-between gap-4" aria-label={headerAriaLabel}>
      <div className="flex items-center gap-3 md:gap-4">
        <Image
          src="/oilchem_logo.png"
          alt="Oilchem Logo"
          width={44}
          height={44}
          className="h-10 w-10 shrink-0 object-contain md:h-11 md:w-11"
          priority
        />
        <div className="flex flex-col leading-tight">
          <span className="text-sm font-medium text-slate-400 md:text-base">Oilchem</span>
          <span className="text-xl font-semibold text-white md:text-2xl">Entry/Exit Dashboard</span>
        </div>
      </div>

      <div className="flex flex-wrap items-center justify-end gap-2">
        {actions}

        <Link href="/" className={navClass(pathname === "/" || pathname === "/dashboard")}>
          Home
        </Link>
        <Link href="/reports" className={navClass(Boolean(pathname?.startsWith("/reports")))}>
          Reports
        </Link>
        {me.role === "admin" ? (
          <Link href="/settings/smtp" className={navClass(Boolean(pathname?.startsWith("/settings")))}>
            Settings
          </Link>
        ) : null}

        <span className="inline-flex items-center rounded-lg border border-cyan-400/50 bg-cyan-500/10 px-3 py-2 text-xs text-cyan-200">
          <span className="font-semibold text-cyan-100">{roleLabel}</span>
          <span className="ml-2 text-cyan-200/85">{me.username}</span>
        </span>
        <button
          type="button"
          onClick={onLogout}
          className="rounded-lg border border-rose-500/50 bg-rose-500/10 px-3 py-2 text-sm text-rose-200 transition hover:bg-rose-500/20"
        >
          Logout
        </button>
      </div>
    </header>
  );
}
