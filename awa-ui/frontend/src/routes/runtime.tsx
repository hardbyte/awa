import { useQuery } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import { fetchQueueRuntime, fetchRuntime } from "@/lib/api";
import type { QueueRuntimeSummary, RuntimeOverview } from "@/lib/api";
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
import { usePollInterval } from "@/hooks/use-poll-interval";
import {
  formatDateTime,
  formatSnapshotInterval,
  instanceLabel,
  LoopBadge,
  PostgresBadge,
  queueCapacityLabel,
  queueConfigDetails,
  queueListLabel,
  rateLimitLabel,
  RuntimeHealthBadge,
  shortInstanceId,
  ShutdownBadge,
} from "@/components/RuntimeDisplay";

export function RuntimePage() {
  const pollInterval = usePollInterval();

  const runtimeQuery = useQuery<RuntimeOverview>({
    queryKey: ["runtime"],
    queryFn: fetchRuntime,
    refetchInterval: pollInterval,
  });

  const queueRuntimeQuery = useQuery<QueueRuntimeSummary[]>({
    queryKey: ["queue-runtime"],
    queryFn: fetchQueueRuntime,
    refetchInterval: pollInterval,
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
                      <RuntimeHealthBadge instance={instance} />
                      {instance.leader && <Badge intent="primary">Leader</Badge>}
                      <PostgresBadge connected={instance.postgres_connected} />
                      <ShutdownBadge shuttingDown={instance.shutting_down} />
                    </div>
                  </div>
                  <div className="mt-3 flex flex-wrap gap-1">
                    <LoopBadge label="poll" healthy={instance.poll_loop_alive} />
                    <LoopBadge label="heartbeat" healthy={instance.heartbeat_alive} />
                    <LoopBadge label="maintenance" healthy={instance.maintenance_alive} />
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
                  <div className="mt-3">
                    <Link
                      to="/runtime/$instanceId"
                      params={{ instanceId: instance.instance_id }}
                      className="text-sm text-primary no-underline hover:underline"
                    >
                      View instance details
                    </Link>
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
                <TableColumn>Details</TableColumn>
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
                        <RuntimeHealthBadge instance={instance} />
                        <PostgresBadge connected={instance.postgres_connected} />
                        <ShutdownBadge shuttingDown={instance.shutting_down} />
                      </div>
                    </TableCell>
                    <TableCell>
                      <div className="flex flex-wrap gap-1">
                        <LoopBadge label="poll" healthy={instance.poll_loop_alive} />
                        <LoopBadge label="heartbeat" healthy={instance.heartbeat_alive} />
                        <LoopBadge label="maintenance" healthy={instance.maintenance_alive} />
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
                    <TableCell>
                      <Link
                        to="/runtime/$instanceId"
                        params={{ instanceId: instance.instance_id }}
                        className="text-primary no-underline hover:underline"
                      >
                        View details
                      </Link>
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
                    <span>{queueCapacityLabel(queue.config)}</span>
                    <span className="text-muted-fg">Global max</span>
                    <span>{queue.config?.global_max_workers ?? "—"}</span>
                    <span className="text-muted-fg">Rate limit</span>
                    <span>{rateLimitLabel(queue.config)}</span>
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
                    {queueConfigDetails(queue.config)}
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
                      <div>{queueCapacityLabel(queue.config)}</div>
                      <div className="text-xs text-muted-fg">
                        global {queue.config?.global_max_workers ?? "—"}
                      </div>
                    </TableCell>
                    <TableCell>
                      <div>{rateLimitLabel(queue.config)}</div>
                      <div className="text-xs text-muted-fg">{queueConfigDetails(queue.config)}</div>
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
