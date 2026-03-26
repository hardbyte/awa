import { useQuery } from "@tanstack/react-query";
import { Link, useParams } from "@tanstack/react-router";
import { fetchRuntime } from "@/lib/api";
import type { RuntimeInstance, RuntimeOverview } from "@/lib/api";
import { usePollInterval } from "@/hooks/use-poll-interval";
import { Copyable } from "@/components/CopyButton";
import { Heading } from "@/components/ui/heading";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import {
  DescriptionDetails,
  DescriptionList,
  DescriptionTerm,
} from "@/components/ui/description-list";
import {
  Table,
  TableBody,
  TableCell,
  TableColumn,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { timeAgo } from "@/lib/time";
import {
  formatDateTime,
  formatSnapshotInterval,
  instanceLabel,
  LoopBadge,
  PostgresBadge,
  queueCapacityLabel,
  queueConfigDetails,
  rateLimitLabel,
  RuntimeHealthBadge,
  shortInstanceId,
  ShutdownBadge,
} from "@/components/RuntimeDisplay";

function attentionItems(instance: RuntimeInstance): string[] {
  const items: string[] = [];
  if (instance.stale) {
    items.push("Snapshot is stale. This instance may have crashed or lost database connectivity.");
  }
  if (!instance.healthy) {
    items.push("Instance health is degraded. At least one runtime loop is reporting unhealthy.");
  }
  if (!instance.postgres_connected) {
    items.push("Database connectivity is down for this worker instance.");
  }
  if (instance.shutting_down) {
    items.push("Instance is shutting down and may stop claiming new work.");
  }
  return items;
}

export function RuntimeInstancePage() {
  const { instanceId } = useParams({ strict: false });
  const pollInterval = usePollInterval();
  const runtimeQuery = useQuery<RuntimeOverview>({
    queryKey: ["runtime"],
    queryFn: fetchRuntime,
    refetchInterval: pollInterval,
  });

  if (runtimeQuery.isLoading) {
    return <p className="text-sm text-muted-fg">Loading runtime instance…</p>;
  }
  if (runtimeQuery.error) {
    return <p className="text-sm text-danger">Error: {String(runtimeQuery.error)}</p>;
  }

  const instance = runtimeQuery.data?.instances.find(
    (candidate) => candidate.instance_id === instanceId
  );

  if (!instance) {
    return (
      <div className="space-y-4">
        <Link
          to="/runtime"
          className="text-sm text-muted-fg no-underline hover:text-fg"
        >
          &larr; Back to runtime
        </Link>
        <p className="text-sm text-muted-fg">
          Runtime instance not found. The snapshot may have expired or this URL is stale.
        </p>
      </div>
    );
  }

  const notes = attentionItems(instance);

  return (
    <div className="space-y-6">
      <Link
        to="/runtime"
        className="text-sm text-muted-fg no-underline hover:text-fg"
      >
        &larr; Back to runtime
      </Link>

      <div className="flex flex-wrap items-center gap-3">
        <Heading level={2}>{instanceLabel(instance)}</Heading>
        <RuntimeHealthBadge instance={instance} />
        {instance.leader && <Badge intent="primary">Leader</Badge>}
        <PostgresBadge connected={instance.postgres_connected} />
        <ShutdownBadge shuttingDown={instance.shutting_down} />
      </div>

      <Card>
        <CardHeader
          title="Instance Summary"
          description="Identity, lifecycle, and queue coverage for this worker snapshot"
        />
        <CardContent>
          <DescriptionList>
            <DescriptionTerm>Instance ID</DescriptionTerm>
            <DescriptionDetails>
              <Copyable value={instance.instance_id}>
                <code className="rounded bg-muted px-1.5 py-0.5 font-mono text-sm">
                  {instance.instance_id}
                </code>
              </Copyable>
            </DescriptionDetails>

            <DescriptionTerm>Short ID</DescriptionTerm>
            <DescriptionDetails>
              <code className="rounded bg-muted px-1.5 py-0.5 font-mono text-sm">
                {shortInstanceId(instance.instance_id)}
              </code>
            </DescriptionDetails>

            <DescriptionTerm>Version / pid</DescriptionTerm>
            <DescriptionDetails>
              {instance.version} · pid {instance.pid}
            </DescriptionDetails>

            <DescriptionTerm>Snapshot</DescriptionTerm>
            <DescriptionDetails>
              {timeAgo(instance.last_seen_at)} (every{" "}
              {formatSnapshotInterval(instance.snapshot_interval_ms)})
            </DescriptionDetails>

            <DescriptionTerm>Started</DescriptionTerm>
            <DescriptionDetails>
              {formatDateTime(instance.started_at)}
            </DescriptionDetails>

            <DescriptionTerm>Hostname</DescriptionTerm>
            <DescriptionDetails>{instance.hostname ?? "—"}</DescriptionDetails>

            <DescriptionTerm>Database</DescriptionTerm>
            <DescriptionDetails>
              {instance.postgres_connected ? "Connected" : "Disconnected"}
            </DescriptionDetails>

            <DescriptionTerm>Global max workers</DescriptionTerm>
            <DescriptionDetails>{instance.global_max_workers ?? "—"}</DescriptionDetails>

            <DescriptionTerm>Queue coverage</DescriptionTerm>
            <DescriptionDetails>{instance.queues.length}</DescriptionDetails>
          </DescriptionList>
        </CardContent>
      </Card>

      {notes.length > 0 && (
        <Card>
          <CardHeader
            title="Attention Needed"
            description="This instance is not in a clean steady-state snapshot"
          />
          <CardContent className="space-y-3">
            {notes.map((note) => (
              <div
                key={note}
                className="rounded-lg border border-warning/40 bg-warning-subtle px-4 py-3 text-sm text-warning-subtle-fg"
              >
                {note}
              </div>
            ))}
          </CardContent>
        </Card>
      )}

      <Card>
        <CardHeader
          title="Loop Health"
          description="Poll, heartbeat, and maintenance status from the latest runtime snapshot"
        />
        <CardContent>
          <div className="grid gap-3 md:grid-cols-3">
            <div className="rounded-lg border p-4">
              <div className="text-xs uppercase tracking-wide text-muted-fg">Poll</div>
              <div className="mt-2">
                <LoopBadge label="poll" healthy={instance.poll_loop_alive} />
              </div>
            </div>
            <div className="rounded-lg border p-4">
              <div className="text-xs uppercase tracking-wide text-muted-fg">Heartbeat</div>
              <div className="mt-2">
                <LoopBadge label="heartbeat" healthy={instance.heartbeat_alive} />
              </div>
            </div>
            <div className="rounded-lg border p-4">
              <div className="text-xs uppercase tracking-wide text-muted-fg">Maintenance</div>
              <div className="mt-2">
                <LoopBadge
                  label="maintenance"
                  healthy={instance.maintenance_alive}
                />
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader
          title="Queues On This Instance"
          description="Per-queue runtime configuration and current in-flight load for this worker"
        />
        <CardContent>
          {instance.queues.length > 0 ? (
            <>
              <div className="space-y-3 sm:hidden">
                {instance.queues.map((queue) => (
                  <div key={queue.queue} className="rounded-lg border p-4">
                    <div className="flex items-center justify-between gap-3">
                      <Link
                        to="/jobs"
                        search={{ q: `queue:${queue.queue}` }}
                        className="font-medium text-primary no-underline hover:underline"
                      >
                        {queue.queue}
                      </Link>
                      <Badge
                        intent={
                          queue.config.mode === "weighted" ? "secondary" : "outline"
                        }
                      >
                        {queue.config.mode === "weighted" ? "Weighted" : "Reserved"}
                      </Badge>
                    </div>
                    <div className="mt-3 grid grid-cols-2 gap-x-4 gap-y-1 text-sm">
                      <span className="text-muted-fg">Capacity</span>
                      <span>{queueCapacityLabel(queue.config)}</span>
                      <span className="text-muted-fg">Rate limit</span>
                      <span>{rateLimitLabel(queue.config)}</span>
                      <span className="text-muted-fg">In flight</span>
                      <span>{queue.in_flight}</span>
                      <span className="text-muted-fg">Overflow held</span>
                      <span>{queue.overflow_held ?? "—"}</span>
                      <span className="text-muted-fg">Global max</span>
                      <span>{queue.config.global_max_workers ?? "—"}</span>
                    </div>
                    <div className="mt-3 text-sm text-muted-fg">
                      {queueConfigDetails(queue.config)}
                    </div>
                  </div>
                ))}
              </div>

              <Table aria-label="Runtime queue assignments" className="hidden sm:table">
                <TableHeader>
                  <TableColumn isRowHeader>Queue</TableColumn>
                  <TableColumn>Mode</TableColumn>
                  <TableColumn>Capacity</TableColumn>
                  <TableColumn>Rate limit</TableColumn>
                  <TableColumn>In flight</TableColumn>
                  <TableColumn>Overflow</TableColumn>
                </TableHeader>
                <TableBody>
                  {instance.queues.map((queue) => (
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
                        <Badge
                          intent={
                            queue.config.mode === "weighted" ? "secondary" : "outline"
                          }
                        >
                          {queue.config.mode === "weighted" ? "Weighted" : "Reserved"}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        <div>{queueCapacityLabel(queue.config)}</div>
                        <div className="text-xs text-muted-fg">
                          global {queue.config.global_max_workers ?? "—"}
                        </div>
                      </TableCell>
                      <TableCell>
                        <div>{rateLimitLabel(queue.config)}</div>
                        <div className="text-xs text-muted-fg">
                          {queueConfigDetails(queue.config)}
                        </div>
                      </TableCell>
                      <TableCell>{queue.in_flight}</TableCell>
                      <TableCell>{queue.overflow_held ?? "—"}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </>
          ) : (
            <p className="py-4 text-sm text-muted-fg">
              No queue snapshots recorded for this worker instance.
            </p>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
