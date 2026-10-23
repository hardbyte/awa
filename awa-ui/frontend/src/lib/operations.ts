import type { QueueStats, RuntimeInstance } from "./api";

export function availableWait(seconds: number | null): string {
  if (seconds === null || !Number.isFinite(seconds)) return "—";
  const value = Math.max(0, seconds);
  if (value < 60) return `${value.toFixed(1)} s`;
  if (value < 3600)
    return `${Math.floor(value / 60)}m ${Math.floor(value % 60)}s`;
  return `${Math.floor(value / 3600)}h ${Math.floor((value % 3600) / 60)}m`;
}

export function queueNeedsReview(queue: QueueStats): boolean {
  return (
    queue.paused ||
    queue.failed > 0 ||
    queue.retryable > 0 ||
    queue.descriptor_mismatch ||
    queue.descriptor_stale
  );
}

export function compareQueuePressure(
  left: QueueStats,
  right: QueueStats,
): number {
  return (
    (right.lag_seconds ?? -1) - (left.lag_seconds ?? -1) ||
    right.available - left.available ||
    left.queue.localeCompare(right.queue)
  );
}

export function runtimeConcern(instance: RuntimeInstance): string {
  if (instance.stale) return "Snapshot stale · health unknown";
  if (instance.shutting_down) return "Shutting down";
  const problems = [
    !instance.postgres_connected && "Postgres disconnected",
    !instance.poll_loop_alive && "Poll loop stopped",
    !instance.heartbeat_alive && "Heartbeat loop stopped",
    !instance.maintenance_alive && "Maintenance loop stopped",
  ].filter(Boolean);
  return problems.join(" · ") || (instance.healthy ? "Healthy" : "Degraded");
}
