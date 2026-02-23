"use client";

import { useCallback, useEffect, useState } from "react";
import { useRouter } from "next/navigation";

export type AuthUser = {
  id: number;
  role: "admin" | "inspector";
  username: string;
  email: string;
};

export function useAuthUser() {
  const router = useRouter();
  const [me, setMe] = useState<AuthUser | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const logout = useCallback(async () => {
    await fetch("/api/auth/logout", { method: "POST" });
    router.push("/login");
    router.refresh();
  }, [router]);

  const handleUnauthorized = useCallback(
    async (status: number): Promise<boolean> => {
      if (status !== 401) {
        return false;
      }
      await logout();
      return true;
    },
    [logout]
  );

  const reloadMe = useCallback(async () => {
    setLoading(true);
    setError("");

    try {
      const response = await fetch("/api/proxy/auth/me", { cache: "no-store" });
      if (await handleUnauthorized(response.status)) {
        return;
      }
      if (!response.ok) {
        const payload = (await response.json().catch(() => null)) as { detail?: string } | null;
        throw new Error(payload?.detail ?? "Failed to load session");
      }
      const payload = (await response.json()) as AuthUser;
      setMe(payload);
    } catch (err) {
      setError((err as Error).message || "Failed to load session");
    } finally {
      setLoading(false);
    }
  }, [handleUnauthorized]);

  useEffect(() => {
    reloadMe();
  }, [reloadMe]);

  return {
    me,
    loading,
    error,
    setError,
    reloadMe,
    logout,
    handleUnauthorized
  };
}
