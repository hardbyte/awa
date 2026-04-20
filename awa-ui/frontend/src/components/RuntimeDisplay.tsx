import type { QueueRuntimeConfigSnapshot, RuntimeInstance } from "@/lib/api";
import { Badge } from "@/components/ui/badge";

export function formatDateTime(value: string): string {
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value));
}

export function formatSnapshotInterval(ms: number): string {
  if (ms < 1_000) return `${ms}ms`;
  if (ms % 1_000 === 0) return `${ms / 1_000}s`;
  return `${(ms / 1_000).toFixed(1)}s`;
}

export function queueCapacityLabel(config?: QueueRuntimeConfigSnapshot | null): string {
  if (!config) return "—";
  if (config.mode === "weighted") {
    return `min ${config.min_workers ?? 0} / w ${config.weight ?? 1}`;
  }
  return `max ${config.max_workers ?? 0}`;
}

export function rateLimitLabel(config?: QueueRuntimeConfigSnapshot | null): string {
  if (!config?.rate_limit) return "—";
  return `${config.rate_limit.max_rate}/s (${config.rate_limit.burst})`;
}

export function instanceLabel(instance: RuntimeInstance): string {
  return instance.hostname ?? `pid ${instance.pid}`;
}

export function shortInstanceId(instanceId: string): string {
  return instanceId.split("-")[0] ?? instanceId;
}

export function queueListLabel(instance: RuntimeInstance): string {
  if (instance.queues.length === 0) return "No queues";
  if (instance.queues.length <= 3) {
    return instance.queues.map((queue) => queue.queue).join(", ");
  }
  return `${instance.queues
    .slice(0, 3)
    .map((queue) => queue.queue)
    .join(", ")} +${instance.queues.length - 3}`;
}

export function queueConfigDetails(config?: QueueRuntimeConfigSnapshot | null): string {
  if (!config) return "No runtime config snapshot";
  return `poll ${formatSnapshotInterval(config.poll_interval_ms)} · deadline ${config.deadline_duration_secs}s · aging ${config.priority_aging_interval_secs}s`;
}

export function RuntimeHealthBadge({ instance }: { instance: RuntimeInstance }) {
  if (instance.stale) return <Badge intent="warning">Stale</Badge>;
  if (instance.healthy) return <Badge intent="success">Healthy</Badge>;
  return <Badge intent="danger">Degraded</Badge>;
}

export function LoopBadge({ label, healthy }: { label: string; healthy: boolean }) {
  return <Badge intent={healthy ? "success" : "danger"}>{label}</Badge>;
}

// Inline dot+label row for the three worker loops.
export function LoopStatus({
  instance,
}: {
  instance: Pick<RuntimeInstance, "poll_loop_alive" | "heartbeat_alive" | "maintenance_alive">;
}) {
  const items: Array<{ label: string; healthy: boolean }> = [
    { label: "poll", healthy: instance.poll_loop_alive },
    { label: "heartbeat", healthy: instance.heartbeat_alive },
    { label: "maintenance", healthy: instance.maintenance_alive },
  ];
  return (
    <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs">
      {items.map((item) => (
        <span key={item.label} className="inline-flex items-center gap-1.5">
          <span
            aria-hidden="true"
            className={`inline-block size-1.5 rounded-full ${
              item.healthy ? "bg-success" : "bg-danger"
            }`}
          />
          <span className={item.healthy ? "text-muted-fg" : "text-danger-fg"}>
            {item.label}
          </span>
        </span>
      ))}
    </div>
  );
}

export function PostgresBadge({ connected }: { connected: boolean }) {
  return <Badge intent={connected ? "secondary" : "danger"}>{connected ? "db ok" : "db down"}</Badge>;
}

export function ShutdownBadge({ shuttingDown }: { shuttingDown: boolean }) {
  if (!shuttingDown) return null;
  return <Badge intent="warning">Shutting down</Badge>;
}
