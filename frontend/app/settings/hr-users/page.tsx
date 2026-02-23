import { redirect } from "next/navigation";

export default function LegacyHrUsersPage() {
  redirect("/settings/users");
}
