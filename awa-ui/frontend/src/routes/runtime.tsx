import { useQuery } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import { fetchQueueRuntime, fetchRuntime } from "@/lib/api";
import type { QueueRuntimeSummary, RuntimeInstance, RuntimeOverview } from "@/lib/api";
import { Heading } from "@/components/ui/heading";
import { Badge } from "@/components/ui/badge";
import { Card, CardAction, CardContent, CardHeader } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableColumn,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { timeAgo } from "@/lib/time";

function formatDateTime(value: string): string {
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value));
}

function formatSnapshotInterval(ms: number): string {
  if (ms < 1_000) return `${ms}ms`;
  if (ms % 1_000 === 0) return `${ms / 1_000}s`;
  return `${(ms / 1_000).toFixed(1)}s`;
}

function queueCapacityLabel(runtime: QueueRuntimeSummary): string {
  if (!runtime.config) return "—";
  if (runtime.config.mode === "weighted") {
    return `min ${runtime.config.min_workers ?? 0} / w ${runtime.config.weight ?? 1}`;
  }
  return `max ${runtime.config.max_workers ?? 0}`;
}

function rateLimitLabel(runtime: QueueRuntimeSummary): string {
  if (!runtime.config?.rate_limit) return "—";
  return `${runtime.config.rate_limit.max_rate}/s (${runtime.config.rate_limit.burst})`;
}

function instanceLabel(instance: RuntimeInstance): string {
  return instance.hostname ?? `pid ${instance.pid}`;
}

function shortInstanceId(instanceId: string): string {
  return instanceId.split("-")[0] ?? instanceId;
}

function healthBadge(instance: RuntimeInstance) {
  if (instance.stale) return <Badge intent="warning">Stale</Badge>;
  if (instance.healthy) return <Badge intent="success">Healthy</Badge>;
  return <Badge intent="danger">Degraded</Badge>;
}

function loopBadge(label: string, healthy: boolean) {
  return <Badge intent={healthy ? "success" : "danger"}>{label}</Badge>;
}

function postgresBadge(connected: boolean) {
  return <Badge intent={connected ? "secondary" : "danger"}>{connected ? "db ok" : "db down"}</Badge>;
}

function shutdownBadge(shuttingDown: boolean) {
  if (!shuttingDown) return null;
  return <Badge intent="warning">Shutting down</Badge>;
}

function queueListLabel(instance: RuntimeInstance): string {
  if (instance.queues.length === 0) return "No queues";
  if (instance.queues.length <= 3) {
    return instance.queues.map((queue) => queue.queue).join(", ");
  }
  return `${instance.queues
    .slice(0, 3)
    .map((queue) => queue.queue)
    .join(", ")} +${instance.queues.length - 3}`;
}

function queueConfigDetails(runtime: QueueRuntimeSummary): string {
  if (!runtime.config) return "No runtime config snapshot";
  return `poll ${formatSnapshotInterval(runtime.config.poll_interval_ms)} · deadline ${runtime.config.deadline_duration_secs}s · aging ${runtime.config.priority_aging_interval_secs}s`;
}

export function RuntimePage() {
  const runtimeQuery = useQuery<RuntimeOverview>({
    queryKey: ["runtime"],
    queryFn: fetchRuntime,
  });

  const queueRuntimeQuery = useQuery<QueueRuntimeSummary[]>({
    queryKey: ["queue-runtime"],
    queryFn: fetchQueueRuntime,
  });

  const runtime = runtimeQuery.data;
  const queues = queueRuntimeQuery.data ?? [];
  const staleInstances = runtime?.instances.filter((instance) => instance.stale) ?? [];
  const degradedInstances =
    runtime?.instances.filter((instance) => !instance.stale && !instance.healthy) ?? [];
  const mismatchQueues = queues.filter((queue) => queue.config_mismatch);
  const hasAttention =
    (runtime?.leader_instances ?? 0) !== 1 ||
    staleInstances.length > 0 ||
    degradedInstances.length > 0 ||
    mismatchQueues.length > 0;

  return (
    <div className="space-y-6">
      <Heading level={2}>Runtime</Heading>

      <Card>
        <CardHeader
          title="Cluster Summary"
          description="Leader election, worker liveness, and queue configuration snapshots"
        />
        <CardContent>
          <div className="grid grid-cols-2 gap-3 md:grid-cols-5">
            <div className="rounded-lg border p-3">
              <div className="text-xs uppercase tracking-wide text-muted-fg">Instances</div>
              <div className="mt-1 text-2xl font-semibold tabular-nums">
                {runtime?.total_instances ?? "—"}
              </div>
            </div>
            <div className="rounded-lg border p-3">
              <div className="text-xs uppercase tracking-wide text-muted-fg">Live</div>
              <div className="mt-1 text-2xl font-semibold tabular-nums">
                {runtime?.live_instances ?? "—"}
              </div>
            </div>
            <div className="rounded-lg border p-3">
              <div className="text-xs uppercase tracking-wide text-muted-fg">Healthy</div>
              <div className="mt-1 text-2xl font-semibold tabular-nums">
                {runtime?.healthy_instances ?? "—"}
              </div>
            </div>
            <div className="rounded-lg border p-3">
              <div className="text-xs uppercase tracking-wide text-muted-fg">Leader</div>
              <div className="mt-1 text-2xl font-semibold tabular-nums">
                {runtime?.leader_instances ?? "—"}
              </div>
            </div>
            <div className="rounded-lg border p-3">
              <div className="text-xs uppercase tracking-wide text-muted-fg">Stale</div>
              <div className="mt-1 text-2xl font-semibold tabular-nums">
                {runtime?.stale_instances ?? "—"}
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      {hasAttention && (
        <Card>
          <CardHeader
            title="Attention Needed"
            description="These states usually need an operator decision rather than passive observation"
          />
          <CardContent className="space-y-3">
            {(runtime?.leader_instances ?? 0) !== 1 && (
              <div className="rounded-lg border border-warning/40 bg-warning-subtle px-4 py-3 text-sm">
                <div className="font-medium text-warning-subtle-fg">
                  Leader status is unexpected
                </div>
                <div className="mt-1 text-warning-subtle-fg/80">
                  Expected exactly one maintenance leader, found {runtime?.leader_instances ?? 0}.
                </div>
              </div>
            )}
            {degradedInstances.length > 0 && (
              <div className="rounded-lg border border-danger/40 bg-danger-subtle px-4 py-3 text-sm">
                <div className="font-medium text-danger-subtle-fg">
                  {degradedInstances.length} degraded instance{degradedInstances.length === 1 ? "" : "s"}
                </div>
                <div className="mt-1 text-danger-subtle-fg/80">
                  {degradedInstances
                    .slice(0, 3)
                    .map((instance) => instanceLabel(instance))
                    .join(", ")}
                </div>
              </div>
            )}
            {staleInstances.length > 0 && (
              <div className="rounded-lg border border-warning/40 bg-warning-subtle px-4 py-3 text-sm">
                <div className="font-medium text-warning-subtle-fg">
                  {staleInstances.length} stale instance{staleInstances.length === 1 ? "" : "s"}
                </div>
                <div className="mt-1 text-warning-subtle-fg/80">
                  Snapshot age exceeded the stale window. Instance may have crashed or lost database connectivity.
                </div>
              </div>
            )}
            {mismatchQueues.length > 0 && (
              <div className="rounded-lg border border-warning/40 bg-warning-subtle px-4 py-3 text-sm">
                <div className="font-medium text-warning-subtle-fg">
                  Queue config mismatch detected
                </div>
                <div className="mt-1 flex flex-wrap gap-2 text-warning-subtle-fg/80">
                  {mismatchQueues.map((queue) => (
                    <Link
                      key={queue.queue}
                      to="/queues"
                      className="text-primary no-underline hover:underline"
                    >
                      {queue.queue}
                    </Link>
                  ))}
                </div>
              </div>
            )}
          </CardContent>
        </Card>
      )}

      <Card>
        <CardHeader
          title="Instances"
          description="Per-worker loop health and leadership status"
        />
        <CardContent>
          {runtime && runtime.instances.length > 0 ? (
            <>
            <div className="space-y-3 sm:hidden">
              {runtime.instances.map((instance) => (
                <div key={instance.instance_id} className="rounded-lg border p-4">
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <div className="font-medium">{instanceLabel(instance)}</div>
                      <div className="text-xs text-muted-fg">
                        {instance.version} · pid {instance.pid}
                      </div>
                      <div className="text-xs text-muted-fg">
                        instance {shortInstanceId(instance.instance_id)}
                      </div>
                    </div>
                    <div className="flex flex-wrap gap-1">
                      {healthBadge(instance)}
                      {instance.leader && <Badge intent="primary">Leader</Badge>}
                      {postgresBadge(instance.postgres_connected)}
                      {shutdownBadge(instance.shutting_down)}
                    </div>
                  </div>
                  <div className="mt-3 flex flex-wrap gap-1">
                    {loopBadge("poll", instance.poll_loop_alive)}
                    {loopBadge("heartbeat", instance.heartbeat_alive)}
                    {loopBadge("maintenance", instance.maintenance_alive)}
                  </div>
                  <div className="mt-3 grid grid-cols-2 gap-x-4 gap-y-1 text-sm">
                    <span className="text-muted-fg">Snapshot</span>
                    <span>{timeAgo(instance.last_seen_at)}</span>
                    <span className="text-muted-fg">Interval</span>
                    <span>{formatSnapshotInterval(instance.snapshot_interval_ms)}</span>
                    <span className="text-muted-fg">Started</span>
                    <span>{timeAgo(instance.started_at)}</span>
                    <span className="text-muted-fg">DB</span>
                    <span>{instance.postgres_connected ? "Connected" : "Disconnected"}</span>
                    <span className="text-muted-fg">Global max</span>
                    <span>{instance.global_max_workers ?? "—"}</span>
                    <span className="text-muted-fg">Queues</span>
                    <span>{queueListLabel(instance)}</span>
                  </div>
                </div>
              ))}
            </div>
            <Table aria-label="Runtime instances" className="hidden sm:table">
              <TableHeader>
                <TableColumn isRowHeader>Instance</TableColumn>
                <TableColumn>Health</TableColumn>
                <TableColumn>Loops</TableColumn>
                <TableColumn>Role</TableColumn>
                <TableColumn>Snapshot</TableColumn>
                <TableColumn>Started</TableColumn>
                <TableColumn>Queues</TableColumn>
              </TableHeader>
              <TableBody>
                {runtime.instances.map((instance) => (
                  <TableRow key={instance.instance_id} id={instance.instance_id}>
                    <TableCell className="font-medium">
                      <div>{instanceLabel(instance)}</div>
                      <div className="text-xs text-muted-fg">
                        {instance.version} · pid {instance.pid}
                      </div>
                      <div className="text-xs text-muted-fg">
                        instance {shortInstanceId(instance.instance_id)}
                      </div>
                    </TableCell>
                    <TableCell>
                      <div className="flex flex-wrap gap-1">
                        {healthBadge(instance)}
                        {postgresBadge(instance.postgres_connected)}
                        {shutdownBadge(instance.shutting_down)}
                      </div>
                    </TableCell>
                    <TableCell>
                      <div className="flex flex-wrap gap-1">
                        {loopBadge("poll", instance.poll_loop_alive)}
                        {loopBadge("heartbeat", instance.heartbeat_alive)}
                        {loopBadge("maintenance", instance.maintenance_alive)}
                      </div>
                    </TableCell>
                    <TableCell>
                      {instance.leader ? (
                        <Badge intent="primary">Leader</Badge>
                      ) : (
                        <span className="text-sm text-muted-fg">Worker</span>
                      )}
                    </TableCell>
                    <TableCell>
                      <div>{timeAgo(instance.last_seen_at)}</div>
                      <div className="text-xs text-muted-fg">
                        every {formatSnapshotInterval(instance.snapshot_interval_ms)}
                      </div>
                    </TableCell>
                    <TableCell>
                      <div>{timeAgo(instance.started_at)}</div>
                      <div className="text-xs text-muted-fg">
                        {formatDateTime(instance.started_at)}
                      </div>
                    </TableCell>
                    <TableCell>
                      <div>{instance.queues.length}</div>
                      <div className="text-xs text-muted-fg">{queueListLabel(instance)}</div>
                      <div className="text-xs text-muted-fg">
                        global {instance.global_max_workers ?? "—"}
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
            </>
          ) : runtimeQuery.isLoading ? (
            <p className="py-4 text-sm text-muted-fg">Loading runtime...</p>
          ) : runtimeQuery.isError ? (
            <p className="py-4 text-sm text-danger">Failed to load runtime snapshots.</p>
          ) : (
            <p className="py-4 text-sm text-muted-fg">
              No runtime snapshots yet. Start a worker to populate this view.
            </p>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader
          title="Queue Runtime"
          description="Configured capacity model, rate limits, and per-queue node coverage"
        >
          <CardAction>
            <Link to="/queues" className="text-sm text-primary no-underline hover:underline">
              Queue controls
            </Link>
          </CardAction>
        </CardHeader>
        <CardContent>
          {queues.length > 0 ? (
            <>
            <div className="space-y-3 sm:hidden">
              {queues.map((queue) => (
                <div key={queue.queue} className="rounded-lg border p-4">
                  <div className="flex items-center justify-between gap-3">
                    <Link
                      to="/jobs"
                      search={{ q: `queue:${queue.queue}` }}
                      className="font-medium text-primary no-underline hover:underline"
                    >
                      {queue.queue}
                    </Link>
                    {queue.config ? (
                      <Badge intent={queue.config.mode === "weighted" ? "secondary" : "outline"}>
                        {queue.config.mode === "weighted" ? "Weighted" : "Reserved"}
                      </Badge>
                    ) : (
                      <Badge intent="outline">Unknown</Badge>
                    )}
                  </div>
                  <div className="mt-3 grid grid-cols-2 gap-x-4 gap-y-1 text-sm">
                    <span className="text-muted-fg">Capacity</span>
                    <span>{queueCapacityLabel(queue)}</span>
                    <span className="text-muted-fg">Global max</span>
                    <span>{queue.config?.global_max_workers ?? "—"}</span>
                    <span className="text-muted-fg">Rate limit</span>
                    <span>{rateLimitLabel(queue)}</span>
                    <span className="text-muted-fg">In flight</span>
                    <span>{queue.total_in_flight}</span>
                    <span className="text-muted-fg">Nodes</span>
                    <span>{queue.healthy_instances}/{queue.live_instances || queue.instance_count} healthy</span>
                    <span className="text-muted-fg">Config</span>
                    <span>
                      {queue.config_mismatch
                        ? "Mismatch"
                        : queue.config
                          ? "Synced"
                          : "Unknown"}
                    </span>
                  </div>
                  <div className="mt-3 text-sm text-muted-fg">
                    {queueConfigDetails(queue)}
                  </div>
                  <div className="mt-1 text-sm text-muted-fg">
                    {queue.stale_instances > 0
                      ? `${queue.stale_instances} stale node snapshot(s)`
                      : queue.overflow_held_total != null
                        ? `Overflow held ${queue.overflow_held_total}`
                        : "No additional runtime notes"}
                  </div>
                </div>
              ))}
            </div>
            <Table aria-label="Queue runtime summary" className="hidden sm:table">
              <TableHeader>
                <TableColumn isRowHeader>Queue</TableColumn>
                <TableColumn>Mode</TableColumn>
                <TableColumn>Capacity</TableColumn>
                <TableColumn>Rate limit</TableColumn>
                <TableColumn>In flight</TableColumn>
                <TableColumn>Nodes</TableColumn>
                <TableColumn>Notes</TableColumn>
              </TableHeader>
              <TableBody>
                {queues.map((queue) => (
                  <TableRow key={queue.queue} id={queue.queue}>
                    <TableCell className="font-medium">
                      <Link
                        to="/jobs"
                        search={{ q: `queue:${queue.queue}` }}
                        className="text-primary no-underline hover:underline"
                      >
                        {queue.queue}
                      </Link>
                    </TableCell>
                    <TableCell>
                      {queue.config ? (
                        <Badge intent={queue.config.mode === "weighted" ? "secondary" : "outline"}>
                          {queue.config.mode === "weighted" ? "Weighted" : "Reserved"}
                        </Badge>
                      ) : (
                        "—"
                      )}
                    </TableCell>
                    <TableCell>
                      <div>{queueCapacityLabel(queue)}</div>
                      <div className="text-xs text-muted-fg">
                        global {queue.config?.global_max_workers ?? "—"}
                      </div>
                    </TableCell>
                    <TableCell>
                      <div>{rateLimitLabel(queue)}</div>
                      <div className="text-xs text-muted-fg">{queueConfigDetails(queue)}</div>
                    </TableCell>
                    <TableCell>{queue.total_in_flight}</TableCell>
                    <TableCell>
                      <div>{queue.healthy_instances}/{queue.live_instances || queue.instance_count} healthy</div>
                      <div className="text-xs text-muted-fg">
                        {queue.stale_instances > 0
                          ? `${queue.stale_instances} stale`
                          : `${queue.instance_count} nodes`}
                      </div>
                    </TableCell>
                    <TableCell>
                      {queue.config_mismatch ? (
                        <Badge intent="warning">Config mismatch</Badge>
                      ) : queue.overflow_held_total != null ? (
                        <span className="text-sm text-muted-fg">
                          overflow held {queue.overflow_held_total}
                        </span>
                      ) : (
                        <span className="text-sm text-muted-fg">—</span>
                      )}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
            </>
          ) : queueRuntimeQuery.isLoading ? (
            <p className="py-4 text-sm text-muted-fg">Loading queue runtime...</p>
          ) : queueRuntimeQuery.isError ? (
            <p className="py-4 text-sm text-danger">Failed to load queue runtime snapshots.</p>
          ) : (
            <p className="py-4 text-sm text-muted-fg">
              No queue runtime snapshots yet.
            </p>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
