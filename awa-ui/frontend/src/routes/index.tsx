import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import {
  fetchStats,
  fetchQueues,
  fetchQueueHealth,
  fetchQueueHistory,
  fetchJobs,
  fetchRuntime,
  fetchDlqDepth,
} from "@/lib/api";
import type { HealthReport, QueueHistory } from "@/lib/api";
import {
  eventIndexes,
  formatCount,
  formatRate,
  groupFailures,
  joinQueues,
  healthOf,
  needsAttention,
  runtimeConcern,
  totalBacklog,
  totalRate,
} from "@/lib/operations";
import { timeAgo } from "@/lib/time";
import { usePollInterval } from "@/hooks/use-poll-interval";
import { Heading } from "@/components/ui/heading";
import {
  Card,
  CardAction,
  CardContent,
  CardHeader,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { HistorySpark } from "@/components/HistorySpark";
import { QueueFlowTable } from "@/components/QueueFlowTable";
import { SnapshotStatus } from "@/components/SnapshotStatus";

const FAILURE_SAMPLE = 50;

const VERDICT_TONE: Record<HealthReport["status"], string> = {
  critical: "border-l-danger",
  warning: "border-l-warning",
  unknown: "border-l-border",
  ok: "border-l-success",
};

/** Shown only when trends are missing, so the page explains a gap rather
 * than narrating what it shows. */
function historyGap(history: QueueHistory | undefined): string | null {
  if (!history) return null;
  if (!history.installed)
    return "No trends: queue history needs schema v051 (run awa migrate).";
  if (!history.observed_since)
    return "No trends yet: the maintenance leader records one sample a minute.";
  return null;
}

export function DashboardPage() {
  const poll = usePollInterval();
  const polling = { refetchInterval: poll.interval, staleTime: poll.staleTime };
  const [showAll, setShowAll] = useState(false);

  const stats = useQuery({
    queryKey: ["stats"],
    queryFn: fetchStats,
    ...polling,
  });
  const queues = useQuery({
    queryKey: ["queues"],
    queryFn: fetchQueues,
    ...polling,
  });
  const history = useQuery({
    queryKey: ["queue-history", 60],
    queryFn: () => fetchQueueHistory(60),
    ...polling,
  });
  const health = useQuery({
    queryKey: ["queue-health"],
    queryFn: fetchQueueHealth,
    ...polling,
  });
  const runtime = useQuery({
    queryKey: ["runtime"],
    queryFn: fetchRuntime,
    ...polling,
  });
  const failures = useQuery({
    queryKey: ["jobs", { state: "failed", limit: FAILURE_SAMPLE }],
    queryFn: () => fetchJobs({ state: "failed", limit: FAILURE_SAMPLE }),
    ...polling,
  });
  const dlq = useQuery({
    queryKey: ["dlq-depth"],
    queryFn: fetchDlqDepth,
    refetchInterval: Math.max(poll.interval, 60_000),
    staleTime: Math.max(poll.staleTime, 60_000),
  });

  const report = health.data;
  const assessed = joinQueues(queues.data ?? [], report, history.data);
  const attention = assessed.filter(needsAttention);
  // Draining queues stay in view so recovery is visible, not just declared.
  const focus = assessed.filter(
    (row) => needsAttention(row) || healthOf(row) === "draining",
  );
  const draining = focus.length - attention.length;
  const rest = assessed.filter((row) => !focus.includes(row));

  const backlogSeries = totalBacklog(history.data);
  const markers = eventIndexes(history.data);
  const backlog = queues.data?.reduce((sum, queue) => sum + queue.available, 0);
  // Measured rates when every served queue has counters, otherwise the
  // completed-last-hour average so the number never silently drops.
  const measuredCompleted = totalRate(report, (f) => f.completed_per_minute);
  const completedPerMinute =
    measuredCompleted ??
    (queues.data
      ? queues.data.reduce((sum, queue) => sum + queue.completed_last_hour, 0) /
        60
      : undefined);
  const arrivalsPerMinute = totalRate(report, (f) => f.arrivals_per_minute);
  const backlogTrend = report
    ? report.queues.reduce<number | null>(
        (sum, queue) =>
          queue.trend && sum !== null ? sum + queue.trend.per_minute : sum,
        0,
      )
    : null;
  const recentEvents = [...(history.data?.events ?? [])].reverse().slice(0, 5);

  const instances = runtime.data?.instances ?? [];
  const troubled = instances.filter(
    (instance) => instance.stale || !instance.healthy,
  );
  const failureGroups = groupFailures(failures.data ?? []);

  const errors = [
    ["Job counts", stats],
    ["Queues", queues],
    ["Queue history", history],
    ["Queue health", health],
    ["Workers", runtime],
    ["Failed jobs", failures],
    ["Dead letter queue", dlq],
  ]
    .filter(([, query]) => (query as { isError: boolean }).isError)
    .map(([name]) => name as string);

  const counters = [
    { label: "Running", state: "running", value: stats.data?.running },
    {
      label: "Retrying",
      state: "retryable",
      value: stats.data?.retryable,
      tone: "text-warning-subtle-fg",
    },
    {
      label: "Failed",
      state: "failed",
      value: stats.data?.failed,
      tone: "text-danger-subtle-fg",
    },
    { label: "Scheduled", state: "scheduled", value: stats.data?.scheduled },
    {
      label: "Waiting external",
      state: "waiting_external",
      value: stats.data?.waiting_external,
    },
  ];

  return (
    <div className="space-y-6">
      <div className="space-y-1">
        <Heading level={2}>Overview</Heading>
        <SnapshotStatus sources={[stats, queues, health, runtime, failures]} />
        {historyGap(history.data) && (
          <p className="text-xs text-muted-fg">{historyGap(history.data)}</p>
        )}
      </div>

      {errors.length > 0 && (
        <div
          role="alert"
          className="rounded-lg border border-danger/40 bg-danger-subtle px-4 py-3 text-sm text-danger-subtle-fg"
        >
          Could not refresh: {errors.join(", ")}. Previous values stay visible
          where available.
        </div>
      )}

      <Card
        className={`border-l-4 ${report ? VERDICT_TONE[report.status] : "border-l-border"}`}
      >
        <CardContent className="@container">
          <div className="grid gap-6 @3xl:grid-cols-[minmax(0,1fr)_auto] @3xl:items-center">
            <div className="min-w-0 space-y-1">
              <h2
                className="text-xl font-semibold tracking-tight text-fg sm:text-2xl"
                data-testid="verdict"
              >
                {report?.title ??
                  (health.isError
                    ? "Queue health unavailable"
                    : "Checking queues…")}
              </h2>
              {report && (
                <p className="text-sm text-muted-fg">{report.detail}</p>
              )}
            </div>
            <div className="flex flex-wrap items-end gap-x-8 gap-y-3">
              <div>
                <p className="text-xs text-muted-fg">
                  Waiting across all queues
                </p>
                <Link
                  to="/jobs"
                  search={{ state: "available" }}
                  className="text-2xl font-semibold tabular-nums hover:underline"
                >
                  {backlog?.toLocaleString() ?? "—"}
                </Link>
                <p className="text-xs text-muted-fg tabular-nums">
                  {report?.history_available && backlogTrend !== null
                    ? // Same noise floor the server applies per queue.
                      Math.abs(backlogTrend) * 5 <
                      Math.max(20, (backlog ?? 0) * 0.05)
                      ? "flat over 5m"
                      : `${formatRate(backlogTrend, true)} over 5m`
                    : "No trend yet"}
                </p>
              </div>
              <HistorySpark
                data={backlogSeries}
                width={200}
                height={44}
                label="Jobs waiting across all queues over the last hour; dashed lines mark fleet changes"
                className="text-primary"
                markers={markers}
              />
              <div>
                <p className="text-xs text-muted-fg">Finishing</p>
                <p className="text-2xl font-semibold tabular-nums">
                  {completedPerMinute === undefined
                    ? "—"
                    : formatRate(completedPerMinute)}
                </p>
                <p className="text-xs text-muted-fg">
                  {measuredCompleted !== null
                    ? arrivalsPerMinute !== null
                      ? `arriving ${formatRate(arrivalsPerMinute)} · last 5m`
                      : "measured, last 5m"
                    : "average, last hour"}
                </p>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      <nav aria-label="Job states" className="flex flex-wrap gap-2">
        {counters.map((counter) => (
          <Link
            key={counter.state}
            to="/jobs"
            search={{ state: counter.state }}
            className="inline-flex min-h-11 items-center gap-2 rounded-lg border px-3 text-sm hover:bg-muted"
          >
            <span className="text-muted-fg">{counter.label}</span>
            <span
              className={`font-semibold tabular-nums ${counter.value ? (counter.tone ?? "") : ""}`}
            >
              {counter.value === undefined ? "—" : formatCount(counter.value)}
            </span>
          </Link>
        ))}
        <Link
          to="/dlq"
          className="inline-flex min-h-11 items-center gap-2 rounded-lg border px-3 text-sm hover:bg-muted"
        >
          <span className="text-muted-fg">Dead letters</span>
          <span className="font-semibold tabular-nums">
            {dlq.data ? formatCount(dlq.data.total) : "—"}
          </span>
        </Link>
        <Link
          to="/runtime"
          search={{ lifecycle: "all" }}
          className="inline-flex min-h-11 items-center gap-2 rounded-lg border px-3 text-sm hover:bg-muted"
        >
          <span className="text-muted-fg">Workers</span>
          <span
            className={`font-semibold tabular-nums ${troubled.length ? "text-danger-subtle-fg" : ""}`}
          >
            {runtime.data
              ? `${runtime.data.healthy_instances}/${runtime.data.live_instances} healthy`
              : "—"}
          </span>
        </Link>
      </nav>

      <Card>
        <CardHeader
          title={
            attention.length
              ? `Needs attention (${attention.length})`
              : draining
                ? `Recovering (${draining})`
                : "Queues"
          }
        >
          <CardAction>
            <Link
              to="/queues"
              className="text-sm font-medium text-primary hover:underline"
            >
              All queues
            </Link>
          </CardAction>
        </CardHeader>
        <CardContent className="px-0 sm:px-0">
          {assessed.length === 0 ? (
            <p className="px-4 py-6 text-sm text-muted-fg sm:px-6">
              {queues.isPending
                ? "Loading queues…"
                : queues.isError
                  ? "Queue data unavailable."
                  : "No queues yet. Queues appear when registered or used."}
            </p>
          ) : (
            <>
              {(focus.length > 0 || showAll) && (
                <QueueFlowTable
                  rows={showAll ? assessed : focus}
                  label={
                    showAll ? "All queues by risk" : "Queues needing attention"
                  }
                />
              )}
              {rest.length > 0 && (
                <div className="border-t px-4 pt-3 sm:px-6">
                  <Button
                    intent="plain"
                    size="sm"
                    onPress={() => setShowAll((value) => !value)}
                  >
                    {showAll
                      ? "Show only queues needing attention or draining"
                      : `Show ${rest.length} queue${rest.length === 1 ? "" : "s"} keeping up`}
                  </Button>
                </div>
              )}
            </>
          )}
        </CardContent>
      </Card>

      <div className="grid grid-cols-1 items-start gap-6 xl:grid-cols-2">
        <Card>
          <CardHeader
            title="Recurring failures"
            description={`From the latest ${FAILURE_SAMPLE} failed jobs`}
          >
            <CardAction>
              <Link
                to="/jobs"
                search={{ state: "failed" }}
                className="text-sm font-medium text-primary hover:underline"
              >
                All failed
              </Link>
            </CardAction>
          </CardHeader>
          <CardContent className="px-0 sm:px-0">
            {failureGroups.length ? (
              <ul className="divide-y" aria-label="Failure groups">
                {failureGroups.slice(0, 6).map((group) => (
                  <li key={group.key}>
                    <Link
                      to="/jobs/$id"
                      params={{ id: String(group.latest.id) }}
                      className="flex items-start gap-3 px-4 py-3 hover:bg-muted/50 sm:px-6"
                    >
                      <span className="w-10 shrink-0 text-right font-semibold tabular-nums text-danger-subtle-fg">
                        ×{group.count}
                      </span>
                      <span className="min-w-0 flex-1">
                        <span className="block truncate text-sm font-medium">
                          {group.latest.kind_descriptor?.display_name?.trim() ||
                            group.kind}
                          <span className="font-normal text-muted-fg">
                            {" "}
                            · {group.queue}
                          </span>
                        </span>
                        <span className="line-clamp-2 text-sm text-danger-subtle-fg sm:line-clamp-1">
                          {group.message}
                        </span>
                      </span>
                      <span className="shrink-0 text-xs text-muted-fg">
                        {group.latest.finalized_at
                          ? timeAgo(group.latest.finalized_at)
                          : "—"}
                      </span>
                    </Link>
                  </li>
                ))}
              </ul>
            ) : (
              <p className="px-4 py-6 text-sm text-muted-fg sm:px-6">
                {failures.isPending
                  ? "Loading failed jobs…"
                  : failures.isError
                    ? "Failed-job data unavailable."
                    : "No failed jobs in retained data."}
              </p>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader
            title="Workers"
            description={
              runtime.data
                ? `${runtime.data.healthy_instances} healthy of ${runtime.data.live_instances} live · ${runtime.data.stale_instances} stale · ${runtime.data.leader_instances} leader`
                : "Runtime snapshots"
            }
          >
            <CardAction>
              <Link
                to="/runtime"
                search={{ lifecycle: "all" }}
                className="text-sm font-medium text-primary hover:underline"
              >
                All workers
              </Link>
            </CardAction>
          </CardHeader>
          <CardContent className="px-0 sm:px-0">
            {troubled.length ? (
              <ul className="divide-y" aria-label="Workers needing attention">
                {troubled.slice(0, 6).map((instance) => (
                  <li key={instance.instance_id}>
                    <Link
                      to="/runtime/$instanceId"
                      params={{ instanceId: instance.instance_id }}
                      className="flex items-center justify-between gap-3 px-4 py-3 hover:bg-muted/50 sm:px-6"
                    >
                      <span className="min-w-0">
                        <span className="block truncate text-sm font-medium">
                          {instance.hostname || `pid ${instance.pid}`}
                        </span>
                        <span className="block text-sm text-danger-subtle-fg">
                          {runtimeConcern(instance)}
                        </span>
                      </span>
                      <span className="shrink-0 text-right text-xs text-muted-fg">
                        Seen {timeAgo(instance.last_seen_at)}
                        <span className="block">{instance.version}</span>
                      </span>
                    </Link>
                  </li>
                ))}
                {troubled.length > 6 && (
                  <li className="px-4 py-3 text-sm text-muted-fg sm:px-6">
                    and {troubled.length - 6} more on the Workers page
                  </li>
                )}
              </ul>
            ) : (
              <p className="px-4 py-6 text-sm text-muted-fg sm:px-6">
                {runtime.isPending
                  ? "Loading workers…"
                  : runtime.isError
                    ? "Worker data unavailable."
                    : instances.length
                      ? `All ${instances.length} reporting instances are healthy.`
                      : "No runtime snapshots. Start a worker to report health."}
              </p>
            )}
            {recentEvents.length > 0 && (
              <div className="border-t px-4 pt-3 sm:px-6">
                <p className="text-xs font-medium text-muted-fg">
                  Fleet changes, last hour
                </p>
                <ul className="mt-1 space-y-1" aria-label="Fleet changes">
                  {recentEvents.map((event) => (
                    <li
                      key={`${event.at}-${event.kind}`}
                      className="flex justify-between gap-3 text-sm"
                    >
                      <span className="min-w-0 truncate">{event.detail}</span>
                      <span className="shrink-0 text-xs text-muted-fg">
                        {timeAgo(event.at)}
                      </span>
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
