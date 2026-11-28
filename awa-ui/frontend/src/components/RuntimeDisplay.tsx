import type { QueueRuntimeConfigSnapshot, RuntimeInstance } from "@/lib/api";
import { CheckIcon, ClockIcon, XCircleIcon } from "@heroicons/react/16/solid";
import { Badge } from "@/components/ui/badge";

export function formatSnapshotInterval(ms: number): string {
  if (ms < 1_000) return `${ms}ms`;
  if (ms % 1_000 === 0) return `${ms / 1_000}s`;
  return `${(ms / 1_000).toFixed(1)}s`;
}

export function queueCapacityLabel(config?: QueueRuntimeConfigSnapshot | null): string {
  if (!config) return "—";
  if (config.mode === "weighted") {
    return `min ${config.min_workers ?? 0} / weight ${config.weight ?? 1}`;
  }
  return `max ${config.max_workers ?? 0}`;
}

export function rateLimitLabel(config?: QueueRuntimeConfigSnapshot | null): string {
  if (!config?.rate_limit) return "—";
  return `${config.rate_limit.max_rate}/s · burst ${config.rate_limit.burst}`;
}

export function dlqPolicyLabel(config?: QueueRuntimeConfigSnapshot | null): string {
  if (config?.dlq_enabled === true) return "DLQ on";
  if (config?.dlq_enabled === false) return "DLQ off";
  return "DLQ unknown";
}

/** A badge for a queue whose dead-letter queue is on; off and unknown get none. */
export function DlqOnBadge({ config }: { config?: QueueRuntimeConfigSnapshot | null }) {
  return config?.dlq_enabled === true ? <Badge intent="secondary">DLQ on</Badge> : null;
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
  return `poll ${formatSnapshotInterval(config.poll_interval_ms)} · deadline ${config.deadline_duration_secs}s · aging ${config.priority_aging_interval_secs}s · ${dlqPolicyLabel(config)}`;
}

/** A badge only for an instance that is stale or unhealthy. */
export function RuntimeHealthBadge({ instance }: { instance: RuntimeInstance }) {
  if (instance.stale) {
    return (
      <Badge intent="secondary">
        <ClockIcon className="size-3" aria-hidden="true" />
        Stale
      </Badge>
    );
  }
  if (instance.healthy) return null;
  return (
    <Badge intent="danger">
      <XCircleIcon className="size-3" aria-hidden="true" />
      Unhealthy
    </Badge>
  );
}

/** One loop's state in words: "alive" plainly, "down" as a red badge. */
export function LoopBadge({ healthy }: { healthy: boolean }) {
  if (healthy) {
    return (
      <span className="inline-flex items-center gap-1 text-sm text-muted-fg">
        <CheckIcon className="size-3.5" aria-hidden="true" />
        Alive
      </span>
    );
  }
  return (
    <Badge intent="danger">
      <XCircleIcon className="size-3" aria-hidden="true" />
      Down
    </Badge>
  );
}

// The three worker loops in one row, each with an icon and its state in text.
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
      {items.map((item) =>
        item.healthy ? (
          <span key={item.label} className="inline-flex items-center gap-1 text-muted-fg">
            <CheckIcon className="size-3" aria-hidden="true" />
            {item.label}
            <span className="sr-only"> alive</span>
          </span>
        ) : (
          <span key={item.label} className="inline-flex items-center gap-1 font-medium text-danger-subtle-fg">
            <XCircleIcon className="size-3" aria-hidden="true" />
            {item.label} down
          </span>
        )
      )}
    </div>
  );
}

/** A badge only when the instance has lost its database connection. */
export function PostgresBadge({ connected }: { connected: boolean }) {
  return connected ? null : <Badge intent="danger">Database down</Badge>;
}

export function ShutdownBadge({ shuttingDown }: { shuttingDown: boolean }) {
  if (!shuttingDown) return null;
  return <Badge intent="warning">Shutting down</Badge>;
}
