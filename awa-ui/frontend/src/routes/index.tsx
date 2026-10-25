import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import {
  fetchStats,
  fetchQueues,
  fetchQueueHistory,
  fetchJobs,
  fetchRuntime,
  fetchDlqDepth,
} from "@/lib/api";
import type { QueueHistory } from "@/lib/api";
import {
  assessQueues,
  formatCount,
  formatDuration,
  formatRate,
  groupFailures,
  overallVerdict,
  runtimeConcern,
  totalBacklog,
  trailingTrend,
  type Verdict,
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

const VERDICT_TONE: Record<Verdict["tone"], string> = {
  danger: "border-l-danger",
  warning: "border-l-warning",
  info: "border-l-info",
  success: "border-l-success",
  muted: "border-l-border",
};

/** Where the trend data comes from, in one line. */
function historyCoverage(history: QueueHistory | null | undefined): string {
  if (history === null)
    return "This server predates queue history; only current depth is shown.";
  if (!history) return "Loading queue history…";
  if (!history.enabled)
    return "Queue history is disabled on this server (--queue-history-minutes 0).";
  if (!history.observed_since)
    return "Queue history starts with the first sample.";
  const seconds =
    (Date.now() - new Date(history.observed_since).getTime()) / 1000;
  return `Trends from samples every ${history.sample_interval_secs}s by this server, covering the last ${formatDuration(Math.min(seconds, history.window_secs))}.`;
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
    queryKey: ["queue-history"],
    queryFn: fetchQueueHistory,
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

  const historyEnabled = history.data?.enabled ?? false;
  const assessed = assessQueues(queues.data ?? [], history.data);
  const attention = assessed.filter((row) => row.assessment.needsAttention);
  const rest = assessed.filter((row) => !row.assessment.needsAttention);
  const verdict = queues.data ? overallVerdict(assessed, historyEnabled) : null;

  const backlogSeries = totalBacklog(history.data);
  const backlogTrend = trailingTrend(
    backlogSeries,
    history.data?.bucket_secs ?? 60,
  );
  const backlog = queues.data?.reduce((sum, queue) => sum + queue.available, 0);
  const completedPerMinute = queues.data
    ? queues.data.reduce((sum, queue) => sum + queue.completed_last_hour, 0) /
      60
    : undefined;

  const instances = runtime.data?.instances ?? [];
  const troubled = instances.filter(
    (instance) => instance.stale || !instance.healthy,
  );
  const failureGroups = groupFailures(failures.data ?? []);

  const errors = [
    ["Job counts", stats],
    ["Queues", queues],
    ["Queue history", history],
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
        <SnapshotStatus sources={[stats, queues, runtime, failures]} />
        <p className="text-xs text-muted-fg">{historyCoverage(history.data)}</p>
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
        className={`border-l-4 ${verdict ? VERDICT_TONE[verdict.tone] : "border-l-border"}`}
      >
        <CardContent className="@container">
          <div className="grid gap-6 @3xl:grid-cols-[minmax(0,1fr)_auto] @3xl:items-center">
            <div className="min-w-0 space-y-1">
              <p className="text-xs font-medium tracking-wide text-muted-fg uppercase">
                Is it keeping up?
              </p>
              <h2
                className="text-xl font-semibold tracking-tight text-fg sm:text-2xl"
                data-testid="verdict"
              >
                {verdict?.title ??
                  (queues.isError
                    ? "Queue data unavailable"
                    : "Checking queues…")}
              </h2>
              {verdict && (
                <p className="text-sm text-muted-fg">{verdict.detail}</p>
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
                  {backlogTrend
                    ? `${formatRate(backlogTrend.perMinute, true)} over ${Math.round(backlogTrend.minutes)}m`
                    : "No trend yet"}
                </p>
              </div>
              <HistorySpark
                data={backlogSeries}
                width={200}
                height={44}
                label="Jobs waiting across all queues over the last hour"
                className="text-primary"
              />
              <div>
                <p className="text-xs text-muted-fg">Finishing</p>
                <p className="text-2xl font-semibold tabular-nums">
                  {completedPerMinute === undefined
                    ? "—"
                    : formatRate(completedPerMinute)}
                </p>
                <p className="text-xs text-muted-fg">average, last hour</p>
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
              : "Queues"
          }
          description={
            attention.length
              ? "Stalled, falling behind, waiting long, or paused with work. Worst first."
              : `${assessed.length} queues, none stalled, falling behind, or waiting long.`
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
              {(attention.length > 0 || showAll) && (
                <QueueFlowTable
                  rows={showAll ? assessed : attention}
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
                      ? "Show only queues needing attention"
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
            description={`Latest ${FAILURE_SAMPLE} retained failed jobs, grouped by kind, queue, and error.`}
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
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
