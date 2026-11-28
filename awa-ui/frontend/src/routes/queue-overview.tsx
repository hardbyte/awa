import { Link } from "@tanstack/react-router";
import { useManagedQuery } from "@/hooks/use-managed-query";
import { useQueueHub } from "@/hooks/use-queue-hub";
import { fetchCronJobs, fetchJobs, fetchRuntime, type CronJobRow, type JobRow, type RuntimeOverview } from "@/lib/api";
import { fleetCapacity } from "@/lib/capacity";
import { dlqDepthQuery } from "@/lib/shared-queries";
import { groupFailures } from "@/lib/errors";
import { formatNumber } from "@/lib/number";
import { formatDuration } from "@/lib/time";
import { describeCron } from "@/components/CronExpr";
import { InstanceLink, KindLink, ScheduleLink } from "@/components/links";
import { ListState } from "@/components/ListState";
import { instanceLabel } from "@/components/RuntimeDisplay";
import { Card, CardContent, CardHeader } from "@/components/ui/card";

/** How many recent failures the overview reads to find failing kinds. */
const FAILURE_SAMPLE = 50;

function Stat({
  name,
  label,
  value,
  hint,
  to,
  testId,
}: {
  label: string;
  value: string;
  hint?: string;
  to?: { tab: "jobs" | "dlq"; state?: string };
  testId: string;
  name: string;
}) {
  const content = <span className="text-2xl font-semibold tabular-nums">{value}</span>;
  return (
    <div className="rounded-lg border p-3" data-testid={testId}>
      <div className="text-xs text-muted-fg">{label}</div>
      <div className="mt-1">
        {to ? (
          <Link
            to={to.tab === "jobs" ? "/queues/$name/jobs" : "/queues/$name/dlq"}
            params={{ name }}
            search={to.state ? { state: to.state } : {}}
            className="text-fg no-underline hover:underline"
          >
            {content}
          </Link>
        ) : (
          content
        )}
      </div>
      {hint && <div className="mt-0.5 text-xs text-muted-fg">{hint}</div>}
    </div>
  );
}

export function QueueOverviewTab() {
  const { name, queue, runtime } = useQueueHub();
  const failuresParams = { queue: name, state: "failed", limit: FAILURE_SAMPLE };
  const failures = useManagedQuery<JobRow[]>({
    queryKey: ["jobs", failuresParams],
    queryFn: () => fetchJobs(failuresParams),
  });
  const cronQuery = useManagedQuery<CronJobRow[]>({ queryKey: ["cron"], queryFn: fetchCronJobs });
  const fleetQuery = useManagedQuery<RuntimeOverview>({ queryKey: ["runtime"], queryFn: fetchRuntime });
  const depthQuery = useManagedQuery(dlqDepthQuery);
  if (!queue) return null;

  const dlqRows = depthQuery.data?.by_queue.find((entry) => entry.queue === name)?.count ?? 0;
  const lag =
    queue.lag_seconds == null ? "—" : queue.lag_seconds < 10 ? `${queue.lag_seconds.toFixed(1)}s` : formatDuration(queue.lag_seconds);

  // The commonest error of each kind in the sample.
  const topErrors = new Map<string, string>();
  for (const group of groupFailures(failures.data ?? [])) {
    if (!topErrors.has(group.kind)) topErrors.set(group.kind, group.count > 1 ? `${group.count}× ${group.summary}` : group.summary);
  }
  const failingKinds = [...(failures.data ?? []).reduce((groups, job) => {
    const group = groups.get(job.kind) ?? { count: 0, descriptor: job.kind_descriptor };
    groups.set(job.kind, { ...group, count: group.count + 1 });
    return groups;
  }, new Map<string, { count: number; descriptor: JobRow["kind_descriptor"] }>())].sort((a, b) => b[1].count - a[1].count);
  const schedules = (cronQuery.data ?? []).filter((cron) => cron.queue === name);
  const instances = (fleetQuery.data?.instances ?? []).filter(
    (instance) => !instance.stale && instance.queues.some((entry) => entry.queue === name)
  );
  const capacity = fleetCapacity(fleetQuery.data?.instances ?? [], name);

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4 xl:grid-cols-8">
        <Stat name={name} testId="queue-stat-ready" label="Ready" value={formatNumber(queue.available)} to={{ tab: "jobs", state: "available" }} />
        <Stat
          name={name}
          testId="queue-stat-lag"
          label="Oldest ready job"
          value={lag}
          hint={queue.paused ? "Queue is paused" : undefined}
        />
        <Stat
          name={name}
          testId="queue-stat-running"
          label={capacity ? "Running / capacity" : "Running"}
          value={capacity ? `${formatNumber(queue.running)} / ${formatNumber(capacity)}` : formatNumber(queue.running)}
          to={{ tab: "jobs", state: "running" }}
        />
        <Stat name={name} testId="queue-stat-retryable" label="Retrying" value={formatNumber(queue.retryable)} to={{ tab: "jobs", state: "retryable" }} />
        <Stat name={name} testId="queue-stat-failed" label="Failed" value={formatNumber(queue.failed)} to={{ tab: "jobs", state: "failed" }} />
        <Stat name={name} testId="queue-stat-waiting" label="Waiting for callback" value={formatNumber(queue.waiting_external)} to={{ tab: "jobs", state: "waiting_external" }} />
        <Stat name={name} testId="queue-stat-scheduled" label="Scheduled" value={formatNumber(queue.scheduled)} hint="for later" to={{ tab: "jobs", state: "scheduled" }} />
        <Stat name={name} testId="queue-stat-completed" label="Completed / hour" value={formatNumber(queue.completed_last_hour)} />
      </div>

      {dlqRows > 0 && (
        <p className="text-sm" data-testid="queue-dlq-rows">
          <Link to="/queues/$name/dlq" params={{ name }} className="text-primary no-underline hover:underline">
            {formatNumber(dlqRows)} dead-lettered job(s)
          </Link>{" "}
          from this queue.
        </p>
      )}

      <div className="grid gap-6 lg:grid-cols-3">
        <Card>
          <CardHeader title="Failing kinds" description={`In the ${FAILURE_SAMPLE} most recent failed jobs.`} />
          <CardContent>
            {failingKinds.length === 0 ? (
              <ListState
                isLoading={failures.isLoading}
                error={failures.isError ? failures.error : null}
                onRetry={() => void failures.refetch()}
                noun="failed jobs"
                empty="No failed jobs."
              />
            ) : (
              <ul className="space-y-1 text-sm" data-testid="queue-failing-kinds">
                {failingKinds.map(([kind, { count, descriptor }]) => (
                  <li key={kind} className="flex items-center justify-between gap-2">
                    <span className="min-w-0">
                      <KindLink kind={kind} descriptor={descriptor} />
                      {topErrors.get(kind) && (
                        <span className="block truncate text-xs text-muted-fg" title={topErrors.get(kind)}>
                          {topErrors.get(kind)}
                        </span>
                      )}
                    </span>
                    <Link
                      to="/queues/$name/jobs"
                      params={{ name }}
                      search={{ state: "failed", q: `kind:${kind}` }}
                      className="tabular-nums text-danger-subtle-fg no-underline hover:underline"
                      aria-label={`${count} failed ${kind} jobs`}
                    >
                      {formatNumber(count)}
                    </Link>
                  </li>
                ))}
              </ul>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader title="Schedules" description="Schedules that enqueue into this queue." />
          <CardContent>
            {schedules.length === 0 ? (
              <ListState
                isLoading={cronQuery.isLoading}
                error={cronQuery.isError ? cronQuery.error : null}
                onRetry={() => void cronQuery.refetch()}
                noun="schedules"
                empty="None."
              />
            ) : (
              <ul className="space-y-1 text-sm" data-testid="queue-schedules">
                {schedules.map((cron) => (
                  <li key={cron.name} className="flex flex-wrap items-baseline gap-x-2">
                    <ScheduleLink name={cron.name} />
                    <span className="text-xs text-muted-fg">{describeCron(cron.cron_expr)}</span>
                  </li>
                ))}
              </ul>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader
            title="Instances"
            description={
              runtime
                ? `${runtime.live_instances} live, ${runtime.healthy_instances} healthy, ${formatNumber(runtime.total_in_flight)} in flight.`
                : "Live workers serving this queue."
            }
          />
          <CardContent>
            {instances.length === 0 ? (
              <ListState
                isLoading={fleetQuery.isLoading}
                error={fleetQuery.isError ? fleetQuery.error : null}
                onRetry={() => void fleetQuery.refetch()}
                noun="instances"
                empty="No live worker serves this queue."
              />
            ) : (
              <ul className="space-y-1 text-sm" data-testid="queue-instances">
                {instances.map((instance) => (
                  <li key={instance.instance_id}>
                    <InstanceLink instanceId={instance.instance_id}>{instanceLabel(instance)}</InstanceLink>
                  </li>
                ))}
              </ul>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
