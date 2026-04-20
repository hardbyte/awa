import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link, useNavigate } from "@tanstack/react-router";
import {
  fetchQueueRuntime,
  fetchRuntime,
  fetchStorage,
} from "@/lib/api";
import type {
  QueueRuntimeSummary,
  RuntimeOverview,
  StorageStatusReport,
} from "@/lib/api";
import {
  shouldShowStorageTransitionCard,
  StorageTransitionCard,
} from "@/components/StorageTransition";
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
  LoopStatus,
  PostgresBadge,
  queueCapacityLabel,
  queueConfigDetails,
  queueListLabel,
  rateLimitLabel,
  RuntimeHealthBadge,
  shortInstanceId,
  ShutdownBadge,
} from "@/components/RuntimeDisplay";

type LifecycleFilter = "live" | "all";

export function RuntimePage() {
  const poll = usePollInterval();
  const navigate = useNavigate();
  const [lifecycle, setLifecycle] = useState<LifecycleFilter>("live");
  const [showStopped, setShowStopped] = useState(false);

  const runtimeQuery = useQuery<RuntimeOverview>({
    queryKey: ["runtime"],
    queryFn: fetchRuntime,
    refetchInterval: poll.interval, staleTime: poll.staleTime,
  });

  const queueRuntimeQuery = useQuery<QueueRuntimeSummary[]>({
    queryKey: ["queue-runtime"],
    queryFn: fetchQueueRuntime,
    refetchInterval: poll.interval, staleTime: poll.staleTime,
  });

  // Optional: backends before 0.5.5-alpha return 404 for /storage, in which
  // case fetchStorage resolves to null and the card simply doesn't render.
  const storageQuery = useQuery<StorageStatusReport | null>({
    queryKey: ["storage"],
    queryFn: fetchStorage,
    refetchInterval: poll.interval,
    staleTime: poll.staleTime,
    retry: false,
  });

  const runtime = runtimeQuery.data;
  const queues = queueRuntimeQuery.data ?? [];
  const allInstances = runtime?.instances ?? [];
  const staleInstances = allInstances.filter((instance) => instance.stale);
  const liveInstances = allInstances.filter((instance) => !instance.stale);
  const degradedInstances =
    allInstances.filter((instance) => !instance.stale && !instance.healthy);
  const mismatchQueues = queues.filter((queue) => queue.config_mismatch);
  // Stale instances alone don't warrant attention — they're expected during
  // rolling deploys and pod reschedules, and the runtime-instances GC ages
  // them out on the normal cleanup cycle. Surface them in the Cluster
  // Summary stats and the Live/All filter below instead. The actionable
  // conditions are: zero live workers (no one is processing jobs), leader
  // drift (maintenance not running on exactly one node), degraded instances
  // (alive but with an unhealthy loop), and queue config mismatch.
  const hasLiveInstances = runtime ? runtime.live_instances > 0 : true;
  // Only compute attention once runtime data is loaded — otherwise the
  // default `leader_instances ?? 0 !== 1` flashes the Attention card in
  // for one frame on every refresh.
  const hasAttention =
    runtime !== undefined &&
    (!hasLiveInstances ||
      runtime.leader_instances !== 1 ||
      degradedInstances.length > 0 ||
      mismatchQueues.length > 0);

  // Live filter: hide stale instances unless explicitly expanded or mode=all.
  const visibleInstances =
    lifecycle === "all" || showStopped
      ? allInstances
      : liveInstances;
  const hiddenStoppedCount =
    lifecycle === "all" || showStopped ? 0 : staleInstances.length;

  const navigateToInstance = (instanceId: string) => {
    void navigate({
      to: "/runtime/$instanceId",
      params: { instanceId },
    });
  };

  return (
    <div className="space-y-6">
      <Heading level={2}>Runtime</Heading>

      {hasAttention && (
        <Card>
          <CardHeader
            title="Attention Needed"
            description="These states usually need an operator decision rather than passive observation"
          />
          <CardContent className="space-y-3">
            {!hasLiveInstances && (
              <div className="rounded-lg border border-danger/40 bg-danger-subtle px-4 py-3 text-sm">
                <div className="font-medium text-danger-subtle-fg">
                  No live worker instances
                </div>
                <div className="mt-1 text-danger-subtle-fg/80">
                  No instance has reported a snapshot within the live window, so
                  no one is claiming or executing jobs right now. Check that
                  your worker fleet is running and connected to this database.
                </div>
              </div>
            )}
            {hasLiveInstances && (runtime?.leader_instances ?? 0) !== 1 && (
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

      {shouldShowStorageTransitionCard(storageQuery.data) && storageQuery.data && (
        <StorageTransitionCard report={storageQuery.data} />
      )}

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
        >
          <CardAction>
            <div
              role="tablist"
              aria-label="Instance lifecycle filter"
              className="inline-flex rounded-md border p-0.5 text-xs"
            >
              <button
                type="button"
                role="tab"
                aria-selected={lifecycle === "live"}
                onClick={() => {
                  setLifecycle("live");
                  setShowStopped(false);
                }}
                className={`rounded px-2.5 py-1 transition ${
                  lifecycle === "live"
                    ? "bg-primary-subtle text-primary-subtle-fg"
                    : "text-muted-fg hover:text-fg"
                }`}
              >
                Live {liveInstances.length > 0 && (
                  <span className="ml-1 tabular-nums">{liveInstances.length}</span>
                )}
              </button>
              <button
                type="button"
                role="tab"
                aria-selected={lifecycle === "all"}
                onClick={() => {
                  setLifecycle("all");
                  setShowStopped(false);
                }}
                className={`rounded px-2.5 py-1 transition ${
                  lifecycle === "all"
                    ? "bg-primary-subtle text-primary-subtle-fg"
                    : "text-muted-fg hover:text-fg"
                }`}
              >
                All {allInstances.length > 0 && (
                  <span className="ml-1 tabular-nums">{allInstances.length}</span>
                )}
              </button>
            </div>
          </CardAction>
        </CardHeader>
        <CardContent>
          {visibleInstances.length > 0 && (
            <div className="space-y-3 sm:hidden">
              {visibleInstances.map((instance) => (
                <button
                  key={instance.instance_id}
                  type="button"
                  onClick={() => navigateToInstance(instance.instance_id)}
                  className="block w-full rounded-lg border p-4 text-left transition hover:bg-secondary/40"
                >
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
                  <div className="mt-3">
                    <LoopStatus instance={instance} />
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
                </button>
              ))}
            </div>
          )}
          <Table bleed aria-label="Runtime instances" className="hidden sm:table">
            <TableHeader>
              <TableColumn isRowHeader>Instance</TableColumn>
              <TableColumn>Health</TableColumn>
              <TableColumn>Loops</TableColumn>
              <TableColumn>Role</TableColumn>
              <TableColumn>Snapshot</TableColumn>
              <TableColumn>Started</TableColumn>
              <TableColumn>Queues</TableColumn>
            </TableHeader>
            <TableBody
              renderEmptyState={() => {
                if (runtimeQuery.isLoading) {
                  return (
                    <div className="p-6 text-center text-sm text-muted-fg">
                      Loading instances…
                    </div>
                  );
                }
                if (runtimeQuery.isError) {
                  return (
                    <div className="p-6 text-center text-sm text-danger-fg">
                      Failed to load runtime snapshots.
                    </div>
                  );
                }
                return (
                  <div className="p-6 text-center text-sm text-muted-fg">
                    {lifecycle === "live"
                      ? "No live worker instances. Switch to All to include recently stopped instances."
                      : "No worker instances recorded."}
                  </div>
                );
              }}
            >
              {visibleInstances.map((instance) => (
                    <TableRow
                      key={instance.instance_id}
                      id={instance.instance_id}
                      onAction={() => navigateToInstance(instance.instance_id)}
                      className="cursor-pointer"
                    >
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
                      <LoopStatus instance={instance} />
                    </TableCell>
                    <TableCell>
                      <div className="flex flex-col gap-0.5">
                        {instance.leader ? (
                          <Badge intent="primary" className="w-fit">Leader</Badge>
                        ) : (
                          <span className="text-sm text-muted-fg">Worker</span>
                        )}
                        {instance.storage_capability &&
                          instance.storage_capability !== "canonical" && (
                            <span className="text-xs capitalize text-muted-fg">
                              {instance.storage_capability.replace(/_/g, " ")}
                            </span>
                          )}
                      </div>
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
                      <div className="max-w-[12rem]">
                        <div className="flex items-baseline gap-1.5">
                          <span className="tabular-nums">{instance.queues.length}</span>
                          {instance.global_max_workers !== null && (
                            <span className="text-xs text-muted-fg">
                              / {instance.global_max_workers} global
                            </span>
                          )}
                        </div>
                        <div
                          className="truncate text-xs text-muted-fg"
                          title={queueListLabel(instance)}
                        >
                          {queueListLabel(instance)}
                        </div>
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
            </TableBody>
          </Table>
          {hiddenStoppedCount > 0 && (
            <div className="mt-3 flex items-center justify-between rounded-md border border-dashed bg-secondary/30 px-4 py-2 text-sm text-muted-fg">
              <span>
                {hiddenStoppedCount} recently stopped instance
                {hiddenStoppedCount === 1 ? "" : "s"} hidden
              </span>
              <button
                type="button"
                onClick={() => setShowStopped(true)}
                className="text-primary no-underline hover:underline"
              >
                Show all
              </button>
            </div>
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
          {queues.length > 0 && (
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
          )}
          <Table bleed aria-label="Queue runtime summary" className="hidden sm:table">
            <TableHeader>
              <TableColumn isRowHeader>Queue</TableColumn>
              <TableColumn>Mode</TableColumn>
              <TableColumn>Capacity</TableColumn>
              <TableColumn>Rate limit</TableColumn>
              <TableColumn className="text-right">In flight</TableColumn>
              <TableColumn>Nodes</TableColumn>
              <TableColumn>Notes</TableColumn>
            </TableHeader>
            <TableBody
              renderEmptyState={() => (
                <div className="p-6 text-center text-sm text-muted-fg">
                  {queueRuntimeQuery.isLoading
                    ? "Loading queue runtime…"
                    : queueRuntimeQuery.isError
                      ? "Failed to load queue runtime snapshots."
                      : "No queue runtime snapshots yet."}
                </div>
              )}
            >
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
                    <TableCell className="text-right tabular-nums">
                      {queue.total_in_flight}
                    </TableCell>
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
        </CardContent>
      </Card>
    </div>
  );
}
