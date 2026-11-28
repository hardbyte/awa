import type { QueueOverview } from "@/lib/api";
import { formatRelative } from "@/lib/time";

/**
 * "Paused 25m ago by ops" for a paused queue: when and by whom, as far as the
 * server reports them. A server before 0.7 reports neither, which leaves
 * "Paused".
 */
export function pausedLabel(
  queue: Pick<QueueOverview, "paused_at" | "paused_by">,
  now: number
): string {
  const at = queue.paused_at && Number.isFinite(new Date(queue.paused_at).getTime())
    ? ` ${formatRelative(queue.paused_at, now)}`
    : "";
  const by = queue.paused_by?.trim() ? ` by ${queue.paused_by.trim()}` : "";
  return `Paused${at}${by}`;
}
