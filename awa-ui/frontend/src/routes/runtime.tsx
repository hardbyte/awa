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

function healthBadge(instance: RuntimeInstance) {
  if (instance.stale) return <Badge intent="warning">Stale</Badge>;
  if (instance.healthy) return <Badge intent="success">Healthy</Badge>;
  return <Badge intent="danger">Degraded</Badge>;
}

function loopBadge(label: string, healthy: boolean) {
  return <Badge intent={healthy ? "success" : "danger"}>{label}</Badge>;
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

      <Card>
        <CardHeader
          title="Instances"
          description="Per-worker loop health and leadership status"
        />
        <CardContent>
          {runtime && runtime.instances.length > 0 ? (
            <Table aria-label="Runtime instances">
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
                    </TableCell>
                    <TableCell>{healthBadge(instance)}</TableCell>
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
                    <TableCell>{instance.queues.length}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          ) : runtimeQuery.isLoading ? (
            <p className="py-4 text-sm text-muted-fg">Loading runtime...</p>
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
            <Table aria-label="Queue runtime summary">
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
                    <TableCell>{queueCapacityLabel(queue)}</TableCell>
                    <TableCell>{rateLimitLabel(queue)}</TableCell>
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
          ) : queueRuntimeQuery.isLoading ? (
            <p className="py-4 text-sm text-muted-fg">Loading queue runtime...</p>
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
