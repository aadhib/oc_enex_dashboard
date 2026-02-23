export const TOKEN_COOKIE_NAME = "oc_hr_admin_token";

export function getBackendApiUrl(): string {
  const raw =
    process.env.BACKEND_API_URL?.trim() ||
    process.env.API_BASE_URL?.trim() ||
    process.env.NEXT_PUBLIC_API_BASE_URL?.trim();
  if (!raw) {
    throw new Error("Set BACKEND_API_URL (or API_BASE_URL) to your FastAPI URL.");
  }
  return raw.endsWith("/") ? raw.slice(0, -1) : raw;
}
