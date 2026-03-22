import { useQuery } from "@tanstack/react-query";
import { Link, useNavigate } from "@tanstack/react-router";
import { fetchStats, fetchQueues, fetchJobs, fetchRuntime } from "@/lib/api";
import type { StateCounts, QueueStats, JobRow, RuntimeOverview } from "@/lib/api";
import { StateBadge } from "@/components/StateBadge";
import { Heading } from "@/components/ui/heading";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import {
  Table,
  TableHeader,
  TableBody,
  TableRow,
  TableCell,
  TableColumn,
} from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { LagValue } from "@/components/LagValue";
import { timeAgo } from "@/lib/time";
import { DASHBOARD_QUEUE_LIMIT } from "@/lib/constants";

/** Background tint per state for counter cards */
const STATE_CARD_BG: Record<string, string> = {
  available: "bg-info-subtle",
  running: "bg-success-subtle",
  failed: "bg-danger-subtle",
  scheduled: "bg-secondary/50",
  waiting_external: "bg-[oklch(0.87_0.07_280)]/30",
};

/** Headline metrics — includes deferred and callback-blocked work */
const COUNTER_KEYS = ["available", "running", "failed", "scheduled", "waiting_external"] as const;

export function DashboardPage() {
  const navigate = useNavigate();

  const statsQuery = useQuery<StateCounts>({
    queryKey: ["stats"],
    queryFn: fetchStats,
  });

  const queuesQuery = useQuery<QueueStats[]>({
    queryKey: ["queues"],
    queryFn: fetchQueues,
  });

  const failedQuery = useQuery<JobRow[]>({
    queryKey: ["jobs", { state: "failed", limit: 10 }],
    queryFn: () => fetchJobs({ state: "failed", limit: 10 }),
  });

  const runtimeQuery = useQuery<RuntimeOverview>({
    queryKey: ["runtime"],
    queryFn: fetchRuntime,
  });

  const completedPerHour = queuesQuery.data
    ? queuesQuery.data.reduce((sum, q) => sum + q.completed_last_hour, 0)
    : null;

  const totalJobs = statsQuery.data
    ? Object.values(statsQuery.data).reduce((a, b) => a + b, 0)
    : null;

  // Sort queues by activity (available + running + failed desc), take top N
  const topQueues = queuesQuery.data
    ? [...queuesQuery.data]
        .sort(
          (a, b) =>
            b.available +
            b.running +
            b.failed -
            (a.available + a.running + a.failed)
        )
        .slice(0, DASHBOARD_QUEUE_LIMIT)
    : [];

  const hasMoreQueues =
    queuesQuery.data && queuesQuery.data.length > DASHBOARD_QUEUE_LIMIT;

  return (
    <div className="space-y-6">
      <Heading level={2}>Dashboard</Heading>

      {/* Headline counter cards */}
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
        {COUNTER_KEYS.map((key) => {
          const count = statsQuery.data?.[key] ?? 0;
          const bg = STATE_CARD_BG[key] ?? "";
          return (
            <Link
              key={key}
              to="/jobs"
              search={{ state: key }}
              className="no-underline"
            >
              <Card
                className={`text-center transition-colors hover:opacity-80 ${bg}`}
              >
                <CardContent className="py-4">
                  <div className="text-3xl font-bold tabular-nums">
                    {count.toLocaleString()}
                  </div>
                  <div className="mt-1.5">
                    <StateBadge state={key} />
                  </div>
                </CardContent>
              </Card>
            </Link>
          );
        })}
        <Link to="/jobs" search={{ state: "completed" }} className="no-underline">
          <Card className="bg-success-subtle/50 text-center transition-colors hover:opacity-80">
            <CardContent className="py-4">
              <div className="text-3xl font-bold tabular-nums">
                {completedPerHour != null ? completedPerHour.toLocaleString() : "—"}
              </div>
              <div className="mt-1.5 text-xs text-muted-fg">completed/hr</div>
            </CardContent>
          </Card>
        </Link>
      </div>

      {/* Total jobs count */}
      {totalJobs != null && (
        <p className="text-sm text-muted-fg">
          {totalJobs.toLocaleString()} total jobs across{" "}
          {queuesQuery.data?.length ?? 0} queues
        </p>
      )}

      <Card>
        <CardHeader
          title="Runtime"
          description="Worker instances, leader health, and current runtime topology"
        />
        <CardContent>
          <div className="mb-4 grid grid-cols-2 gap-3 md:grid-cols-4">
            <div className="rounded-lg border p-3">
              <div className="text-xs uppercase tracking-wide text-muted-fg">Live</div>
              <div className="mt-1 text-2xl font-semibold tabular-nums">
                {runtimeQuery.data?.live_instances ?? "—"}
              </div>
            </div>
            <div className="rounded-lg border p-3">
              <div className="text-xs uppercase tracking-wide text-muted-fg">Healthy</div>
              <div className="mt-1 text-2xl font-semibold tabular-nums">
                {runtimeQuery.data?.healthy_instances ?? "—"}
              </div>
            </div>
            <div className="rounded-lg border p-3">
              <div className="text-xs uppercase tracking-wide text-muted-fg">Leader</div>
              <div className="mt-1 text-2xl font-semibold tabular-nums">
                {runtimeQuery.data?.leader_instances ?? "—"}
              </div>
            </div>
            <div className="rounded-lg border p-3">
              <div className="text-xs uppercase tracking-wide text-muted-fg">Stale</div>
              <div className="mt-1 text-2xl font-semibold tabular-nums">
                {runtimeQuery.data?.stale_instances ?? "—"}
              </div>
            </div>
          </div>

          {runtimeQuery.data && runtimeQuery.data.instances.length > 0 ? (
            <Table aria-label="Runtime instances">
              <TableHeader>
                <TableColumn isRowHeader>Instance</TableColumn>
                <TableColumn>Health</TableColumn>
                <TableColumn>Loops</TableColumn>
                <TableColumn>Role</TableColumn>
                <TableColumn>Queues</TableColumn>
                <TableColumn>Seen</TableColumn>
              </TableHeader>
              <TableBody>
                {runtimeQuery.data.instances.map((instance) => {
                  const label = instance.hostname ?? `pid ${instance.pid}`;
                  const healthLabel = instance.stale
                    ? "Stale"
                    : instance.healthy
                      ? "Healthy"
                      : "Degraded";
                  const healthIntent = instance.stale
                    ? "warning"
                    : instance.healthy
                      ? "success"
                      : "danger";
                  return (
                    <TableRow key={instance.instance_id} id={instance.instance_id}>
                      <TableCell className="font-medium">
                        <div>{label}</div>
                        <div className="text-xs text-muted-fg">
                          {instance.version} · pid {instance.pid}
                        </div>
                      </TableCell>
                      <TableCell>
                        <Badge intent={healthIntent}>{healthLabel}</Badge>
                      </TableCell>
                      <TableCell>
                        <div className="flex flex-wrap gap-1">
                          <Badge intent={instance.poll_loop_alive ? "success" : "danger"}>
                            poll
                          </Badge>
                          <Badge intent={instance.heartbeat_alive ? "success" : "danger"}>
                            heartbeat
                          </Badge>
                          <Badge intent={instance.maintenance_alive ? "success" : "danger"}>
                            maintenance
                          </Badge>
                        </div>
                      </TableCell>
                      <TableCell>
                        {instance.leader ? (
                          <Badge intent="primary">Leader</Badge>
                        ) : (
                          <span className="text-sm text-muted-fg">Worker</span>
                        )}
                      </TableCell>
                      <TableCell>{instance.queues.length}</TableCell>
                      <TableCell>{timeAgo(instance.last_seen_at)}</TableCell>
                    </TableRow>
                  );
                })}
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

      {/* Queue summary — top N by activity */}
      <Card>
        <CardHeader
          title="Queues"
          description={
            hasMoreQueues
              ? `Showing top ${DASHBOARD_QUEUE_LIMIT} by activity`
              : undefined
          }
        />
        <CardContent>
          {topQueues.length > 0 ? (
            <>
              <Table aria-label="Queue summary">
                <TableHeader>
                  <TableColumn isRowHeader>Queue</TableColumn>
                  <TableColumn>Available</TableColumn>
                  <TableColumn>Running</TableColumn>
                  <TableColumn>Failed</TableColumn>
                  <TableColumn>Waiting</TableColumn>
                  <TableColumn>Completed/hr</TableColumn>
                  <TableColumn>Lag (s)</TableColumn>
                  <TableColumn>Status</TableColumn>
                </TableHeader>
                <TableBody>
                  {topQueues.map((q) => (
                    <TableRow
                      key={q.queue}
                      id={q.queue}
                      className="cursor-pointer"
                      onAction={() =>
                        void navigate({
                          to: "/jobs",
                          search: { q: `queue:${q.queue}` },
                        })
                      }
                    >
                      <TableCell className="font-medium">
                        <Link
                          to="/queues/$name"
                          params={{ name: q.queue }}
                          className="text-primary no-underline hover:underline"
                        >
                          {q.queue}
                        </Link>
                      </TableCell>
                      <TableCell>{q.available.toLocaleString()}</TableCell>
                      <TableCell>{q.running}</TableCell>
                      <TableCell>
                        <span className={q.failed > 0 ? "text-danger" : ""}>
                          {q.failed}
                        </span>
                      </TableCell>
                      <TableCell>
                        {q.waiting_external > 0 ? q.waiting_external : "-"}
                      </TableCell>
                      <TableCell>{q.completed_last_hour}</TableCell>
                      <TableCell>
                        <LagValue seconds={q.lag_seconds} />
                      </TableCell>
                      <TableCell>
                        {q.paused ? (
                          <Badge intent="warning">Paused</Badge>
                        ) : (
                          <Badge intent="success">Active</Badge>
                        )}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
              {hasMoreQueues && (
                <div className="px-4 py-3">
                  <Link
                    to="/queues"
                    className="text-sm text-primary no-underline hover:underline"
                  >
                    View all {queuesQuery.data?.length} queues &rarr;
                  </Link>
                </div>
              )}
            </>
          ) : queuesQuery.isLoading ? (
            <p className="py-4 text-sm text-muted-fg">Loading queues...</p>
          ) : (
            <p className="py-4 text-sm text-muted-fg">No queues found.</p>
          )}
        </CardContent>
      </Card>

      {/* Recent failures */}
      <Card>
        <CardHeader title="Recent Failures" />
        <CardContent>
          {failedQuery.data && failedQuery.data.length > 0 ? (
            <Table aria-label="Recent failures">
              <TableHeader>
                <TableColumn isRowHeader>ID</TableColumn>
                <TableColumn>Kind</TableColumn>
                <TableColumn>Queue</TableColumn>
                <TableColumn>Attempt</TableColumn>
                <TableColumn>Failed</TableColumn>
                <TableColumn>Error</TableColumn>
              </TableHeader>
              <TableBody>
                {failedQuery.data.map((job) => {
                  const lastErr =
                    job.errors && job.errors.length > 0
                      ? job.errors[job.errors.length - 1]
                      : null;
                  const errMsg =
                    lastErr &&
                    typeof lastErr === "object" &&
                    lastErr !== null
                      ? String(
                          (lastErr as Record<string, unknown>)["error"] ?? ""
                        )
                      : "";
                  return (
                    <TableRow
                      key={job.id}
                      id={job.id}
                      className="cursor-pointer"
                      onAction={() =>
                        void navigate({
                          to: "/jobs/$id",
                          params: { id: String(job.id) },
                        })
                      }
                    >
                      <TableCell className="font-mono text-primary">
                        {job.id}
                      </TableCell>
                      <TableCell>{job.kind}</TableCell>
                      <TableCell>{job.queue}</TableCell>
                      <TableCell>
                        {job.attempt}/{job.max_attempts}
                      </TableCell>
                      <TableCell>
                        {job.finalized_at
                          ? timeAgo(job.finalized_at)
                          : "-"}
                      </TableCell>
                      <TableCell className="max-w-[300px] truncate text-danger">
                        {errMsg || "-"}
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          ) : failedQuery.isLoading ? (
            <p className="py-4 text-sm text-muted-fg">Loading...</p>
          ) : (
            <p className="py-4 text-sm text-muted-fg">
              No recent failures.
            </p>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
