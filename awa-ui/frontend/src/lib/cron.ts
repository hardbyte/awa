import type { CronJobRow } from "@/lib/api";
import { CRON_OVERDUE_GRACE_MS } from "@/lib/constants";

/** Whether a live schedule has missed its next fire. */
export function isOverdue(cron: Pick<CronJobRow, "next_fire_at" | "paused_at" | "retired_at">, nowMs = Date.now()): boolean {
  if (cron.paused_at || cron.retired_at || !cron.next_fire_at) return false;
  return new Date(cron.next_fire_at).getTime() < nowMs - CRON_OVERDUE_GRACE_MS;
}
