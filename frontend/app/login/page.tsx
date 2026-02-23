"use client";

import Image from "next/image";
import { FormEvent, useState } from "react";
import { useRouter } from "next/navigation";

export default function LoginPage() {
  const router = useRouter();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [logoSrc, setLogoSrc] = useState("/oilchem_logo.png");

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError("");
    setSubmitting(true);

    try {
      const response = await fetch("/api/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username, password })
      });

      if (!response.ok) {
        const payload = (await response.json().catch(() => null)) as { detail?: string } | null;
        setError(payload?.detail ?? "Login failed");
        return;
      }

      router.push("/");
      router.refresh();
    } catch {
      setError("Unable to reach server");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <main className="mx-auto flex min-h-screen w-full max-w-md items-center px-4">
      <div className="w-full rounded-3xl border border-zinc-800 bg-zinc-900/80 p-8 shadow-2xl backdrop-blur">
        <div className="mb-5 flex justify-center">
          <Image
            src={logoSrc}
            alt="Oilchem"
            width={64}
            height={64}
            priority
            className="h-16 w-16 object-contain"
            onError={() => {
              if (logoSrc !== "/oilchem_logo.png") {
                setLogoSrc("/oilchem_logo.png");
              }
            }}
          />
        </div>

        <p className="text-center text-xs uppercase tracking-[0.25em] text-zinc-400">OILCHEM</p>
        <h1 className="mt-3 text-center text-3xl font-semibold text-zinc-100">Entry/Exit Dashboard</h1>
        <p className="mt-2 text-center text-sm text-zinc-400">Please sign in using your authorized credentials</p>

        <form className="mt-6 space-y-4" onSubmit={handleSubmit}>
          <label className="block">
            <span className="mb-2 block text-sm text-zinc-300">Username or Email</span>
            <input
              className="w-full rounded-xl border border-zinc-700 bg-zinc-950 px-4 py-3 text-zinc-100 outline-none transition focus:border-cyan-400"
              value={username}
              onChange={(event) => setUsername(event.target.value)}
              required
              autoComplete="username"
            />
          </label>

          <label className="block">
            <span className="mb-2 block text-sm text-zinc-300">Password</span>
            <input
              type="password"
              className="w-full rounded-xl border border-zinc-700 bg-zinc-950 px-4 py-3 text-zinc-100 outline-none transition focus:border-cyan-400"
              value={password}
              onChange={(event) => setPassword(event.target.value)}
              required
              autoComplete="current-password"
            />
          </label>

          {error ? <p className="text-sm text-rose-400">{error}</p> : null}

          <button
            type="submit"
            disabled={submitting}
            className="w-full rounded-xl bg-cyan-500 px-4 py-3 text-sm font-semibold text-zinc-950 transition hover:bg-cyan-400 disabled:cursor-not-allowed disabled:opacity-60"
          >
            {submitting ? "Signing in..." : "Sign in"}
          </button>
        </form>
      </div>
    </main>
  );
}
