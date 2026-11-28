import { Link, type LinkProps } from "@tanstack/react-router";
import {
  CheckCircleIcon,
  ExclamationTriangleIcon,
  InformationCircleIcon,
  XCircleIcon,
} from "@heroicons/react/16/solid";
import { useManagedQuery } from "@/hooks/use-managed-query";
import { useNow } from "@/hooks/use-now";
import {
  fetchCronJobs,
  fetchJobs,
  fetchQueueRuntime,
  fetchQueues,
  fetchRuntime,
  fetchStats,
  type CronJobRow,
  type JobRow,
  type QueueOverview,
  type QueueRuntimeSummary,
  type RuntimeOverview,
  type StateCounts,
} from "@/lib/api";
import {
  attentionItems,
  healthFigures,
  liveFailedCount,
  queuesNeedingAttention,
  type AttentionItem,
  type AttentionSeverity,
} from "@/lib/attention";
import { batchOpsQuery, dlqDepthQuery } from "@/lib/shared-queries";
import {
  OVERVIEW_FAILED_SAMPLE,
  OVERVIEW_QUEUE_LIMIT,
  OVERVIEW_RUNNING_SAMPLE,
} from "@/lib/constants";
import { formatDuration, formatRelative } from "@/lib/time";
import { formatNumber, plural } from "@/lib/number";
import { Page, PageHeader } from "@/components/Page";
import { Card, CardAction, CardContent, CardHeader } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableColumn,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { PausedBadge } from "@/components/PausedBadge";
import { QueueLink, entityLinkClass } from "@/components/links";
import { Time } from "@/components/Time";

const FAILED_SAMPLE = { state: "failed", dead_lettered: false, limit: OVERVIEW_FAILED_SAMPLE } as const;
const RUNNING_SAMPLE = { state: "running", limit: OVERVIEW_RUNNING_SAMPLE } as const;

type Tone = "neutral" | "warning" | "danger";

const TONE_FIGURE: Record<Tone, string> = {
  neutral: "text-fg",
  warning: "text-warning-subtle-fg",
  danger: "text-danger-subtle-fg",
};

interface TileProps {
  label: string;
  /** The figure; undefined while loading. */
  value: number | undefined;
  error?: Error | null;
  tone?: Tone;
  link: LinkProps;
  testId: string;
  children?: React.ReactNode;
}

/** One figure of the health strip, linking to the jobs behind it. */
function HealthTile({ label, value, error, tone = "neutral", link, testId, children }: TileProps) {
  return (
    <Link
      {...link}
      className="rounded-lg border bg-bg p-4 no-underline outline-hidden transition-colors hover:bg-muted/40 focus-visible:ring-2 focus-visible:ring-ring"
      data-testid={testId}
    >
      <div className="text-xs font-medium text-muted-fg">{label}</div>
      <div
        className={`mt-1 text-2xl font-semibold tabular-nums ${error ? "text-muted-fg" : TONE_FIGURE[tone]}`}
        data-testid="tile-value"
      >
        {error ? (
          <span className="inline-flex items-center gap-1.5" title={error.message}>
            —
            <ExclamationTriangleIcon className="size-4 text-warning-subtle-fg" aria-hidden="true" />
          </span>
        ) : value === undefined ? (
          <span className="inline-block h-8 w-14 animate-pulse rounded bg-muted" aria-label="Loading" />
        ) : (
          formatNumber(value)
        )}
      </div>
      <div className="mt-1 min-h-5 text-xs text-muted-fg">
        {error ? <span role="alert">Failed to load: {error.message}</span> : children}
      </div>
    </Link>
  );
}

const SEVERITY_ICON: Record<AttentionSeverity, { Icon: typeof XCircleIcon; className: string; label: string }> = {
  action: { Icon: XCircleIcon, className: "text-danger-subtle-fg", label: "Needs action" },
  warning: { Icon: ExclamationTriangleIcon, className: "text-warning-subtle-fg", label: "Warning" },
  notice: { Icon: InformationCircleIcon, className: "text-muted-fg", label: "Notice" },
};

function AttentionRow({ item }: { item: AttentionItem }) {
  const { Icon, className, label } = SEVERITY_ICON[item.severity];
  return (
    <li className="flex gap-3 px-(--gutter) py-3" data-testid="attention-item" data-severity={item.severity} data-id={item.id}>
      <Icon className={`mt-1 size-4 shrink-0 ${className}`} aria-label={label} role="img" />
      <div className="min-w-0">
        <Link {...(item.target as LinkProps)} className={`${entityLinkClass} font-medium`}>
          {item.title}
        </Link>
        {item.detail && (
          <div className="truncate text-sm text-muted-fg" title={item.detail}>
            {item.detail}
          </div>
        )}
      </div>
    </li>
  );
}

export function DashboardPage() {
  const now = useNow();
  const statsQuery = useManagedQuery<StateCounts>({ queryKey: ["stats"], queryFn: fetchStats });
  const queuesQuery = useManagedQuery<QueueOverview[]>({ queryKey: ["queues"], queryFn: fetchQueues });
  const queueRuntimeQuery = useManagedQuery<QueueRuntimeSummary[]>({
    queryKey: ["queue-runtime"],
    queryFn: fetchQueueRuntime,
  });
  const runtimeQuery = useManagedQuery<RuntimeOverview>({ queryKey: ["runtime"], queryFn: fetchRuntime });
  const dlqQuery = useManagedQuery(dlqDepthQuery);
  const batchQuery = useManagedQuery(batchOpsQuery);
  const cronQuery = useManagedQuery<CronJobRow[]>({ queryKey: ["cron"], queryFn: fetchCronJobs });
  const failedQuery = useManagedQuery<JobRow[]>({
    queryKey: ["jobs", FAILED_SAMPLE],
    queryFn: () => fetchJobs(FAILED_SAMPLE),
  });
  const runningQuery = useManagedQuery<JobRow[]>({
    queryKey: ["jobs", RUNNING_SAMPLE],
    queryFn: () => fetchJobs(RUNNING_SAMPLE),
  });

  const input = {
    now,
    queues: queuesQuery.data,
    queueRuntime: queueRuntimeQuery.data,
    runtime: runtimeQuery.data,
    dlqDepth: dlqQuery.data,
    batchOps: batchQuery.data,
    cron: cronQuery.data,
    failedJobs: failedQuery.data,
    runningJobs: runningQuery.data,
  };
  const figures = healthFigures(input);
  const items = attentionItems(input);
  const checks: [string, { isError: boolean; isLoading: boolean }][] = [
    ["queues", queuesQuery],
    ["queue configs", queueRuntimeQuery],
    ["fleet", runtimeQuery],
    ["dead letters", dlqQuery],
    ["bulk operations", batchQuery],
    ["schedules", cronQuery],
    ["failed jobs", failedQuery],
    ["running jobs", runningQuery],
  ];
  const failedChecks = checks.filter(([, q]) => q.isError).map(([name]) => name);
  const loadingChecks = checks.some(([, q]) => q.isLoading);
  const checkedAt = Math.max(queuesQuery.dataUpdatedAt, runtimeQuery.dataUpdatedAt, failedQuery.dataUpdatedAt);

  const stats = statsQuery.data;
  const runtime = runtimeQuery.data;
  const leader = runtime?.instances.find((i) => i.leader && !i.stale);
  const nextFire = (cronQuery.data ?? [])
    .filter((c) => !c.paused_at && !c.retired_at && c.next_fire_at)
    .map((c) => c.next_fire_at!)
    .sort()[0];
  const topQueues = queuesQuery.data
    ? queuesNeedingAttention(queuesQuery.data, dlqQuery.data, OVERVIEW_QUEUE_LIMIT)
    : [];
  const statsError = statsQuery.isError ? statsQuery.error : null;
  // Dead letters have their own tile, so the Failed tile counts the rest.
  const liveFailed =
    stats && dlqQuery.data ? liveFailedCount(stats.failed ?? 0, dlqQuery.data.total) : undefined;

  return (
    <Page>
      <PageHeader title="Overview" />

      <section aria-label="Health" className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6">
        <HealthTile
          label="Ready"
          testId="tile-ready"
          value={figures.ready ?? undefined}
          error={queuesQuery.isError ? queuesQuery.error : null}
          link={{ to: "/jobs", search: { state: "available" } }}
        >
          {figures.oldestReady
            ? `oldest ${formatDuration(figures.oldestReady.seconds)} in ${figures.oldestReady.queue}`
            : "nothing waiting"}
          {figures.pausedReady && figures.pausedReady.jobs > 0 && (
            <div>
              +{formatNumber(figures.pausedReady.jobs)} held in {plural(figures.pausedReady.queues, "paused queue")}
            </div>
          )}
        </HealthTile>
        <HealthTile
          label="Retrying"
          testId="tile-retrying"
          value={stats ? stats.retryable ?? 0 : undefined}
          error={statsError}
          link={{ to: "/jobs", search: { state: "retryable" } }}
        >
          {figures.topRetrying
            ? `most in ${figures.topRetrying.queue} (${formatNumber(figures.topRetrying.count)})`
            : "no retries pending"}
        </HealthTile>
        <HealthTile
          label="Failed"
          testId="tile-failed"
          value={liveFailed}
          error={statsError ?? (dlqQuery.isError ? dlqQuery.error : null)}
          tone={(liveFailed ?? 0) > 0 ? "danger" : "neutral"}
          link={{ to: "/jobs", search: { state: "failed", dead_lettered: false } }}
        >
          {figures.completedLastHour !== null &&
            `${formatNumber(figures.completedLastHour)} completed in the last hour`}
        </HealthTile>
        <HealthTile
          label="Dead letters"
          testId="tile-dead-letters"
          value={dlqQuery.data?.total}
          error={dlqQuery.isError ? dlqQuery.error : null}
          tone={(dlqQuery.data?.total ?? 0) > 0 ? "warning" : "neutral"}
          link={{ to: "/dlq" }}
        >
          {figures.topDeadLetters
            ? `most in ${figures.topDeadLetters.queue} (${formatNumber(figures.topDeadLetters.count)})`
            : "none"}
        </HealthTile>
        <HealthTile
          label="Stuck running"
          testId="tile-stuck"
          value={figures.stuck?.count}
          error={runningQuery.isError ? runningQuery.error : null}
          tone={(figures.stuck?.count ?? 0) > 0 ? "danger" : "neutral"}
          link={{ to: "/jobs", search: { state: "running" } }}
        >
          {figures.stuck &&
            (figures.stuck.sampled >= OVERVIEW_RUNNING_SAMPLE
              ? `in the ${formatNumber(figures.stuck.sampled)} newest running jobs`
              : `of ${plural(figures.stuck.sampled, "running job")}`)}
        </HealthTile>
        <HealthTile
          label="Bulk operations"
          testId="tile-bulk"
          value={figures.activeBatchOps ?? undefined}
          error={batchQuery.isError ? batchQuery.error : null}
          link={{ to: "/batch-ops" }}
        >
          {figures.latestFailedBatchOp ? (
            <span className="line-clamp-2 text-danger-subtle-fg" title={figures.latestFailedBatchOp.last_error ?? undefined}>
              last failed: {figures.latestFailedBatchOp.last_error ?? "no error recorded"}
            </span>
          ) : (
            "in flight"
          )}
        </HealthTile>
      </section>

      <div className="flex flex-col gap-1 text-sm text-muted-fg sm:flex-row sm:flex-wrap sm:gap-x-6">
        <p data-testid="fleet-line">
          <Link to="/runtime" className={entityLinkClass}>
            Fleet
          </Link>
          :{" "}
          {runtimeQuery.isError
            ? "failed to load"
            : runtime
              ? [
                  `${runtime.healthy_instances}/${runtime.live_instances} healthy`,
                  leader ? `leader ${leader.hostname ?? `pid ${leader.pid}`}` : "no leader",
                  runtime.stale_instances > 0 ? `${runtime.stale_instances} stale` : null,
                ]
                  .filter(Boolean)
                  .join(" · ")
              : "…"}
        </p>
        <p data-testid="deferred-line">
          Not yet due:{" "}
          {statsError
            ? "failed to load"
            : stats
              ? [
                  `${formatNumber(stats.scheduled ?? 0)} scheduled`,
                  `${formatNumber(stats.waiting_external ?? 0)} waiting for callback`,
                  nextFire ? `next schedule fires ${formatRelative(nextFire, now)}` : null,
                ]
                  .filter(Boolean)
                  .join(" · ")
              : "…"}
        </p>
      </div>

      <Card className="[--gutter:--spacing(4)] sm:[--gutter:--spacing(6)]">
        <CardHeader title="Needs attention" description={items.length > 0 ? plural(items.length, "item") : undefined} />
        <CardContent className="px-0">
          {items.length > 0 ? (
            <ul className="divide-y border-t" aria-label="Needs attention">
              {items.map((item) => (
                <AttentionRow key={item.id} item={item} />
              ))}
            </ul>
          ) : loadingChecks ? (
            <p className="px-(--gutter) text-sm text-muted-fg">Checking…</p>
          ) : (
            <p className="flex items-center gap-2 px-(--gutter) text-sm text-muted-fg" data-testid="attention-empty">
              <CheckCircleIcon className="size-4 text-success" aria-hidden="true" />
              <span>
                Nothing needs attention
                {checkedAt > 0 && (
                  <>
                    {" "}· checked <Time value={new Date(checkedAt).toISOString()} />
                  </>
                )}
                {figures.stuck && figures.stuck.sampled >= OVERVIEW_RUNNING_SAMPLE && (
                  <span className="block" data-testid="attention-stuck-sample">
                    Stuck jobs were looked for among the {formatNumber(figures.stuck.sampled)} newest running jobs only.
                  </span>
                )}
              </span>
            </p>
          )}
          {failedChecks.length > 0 && (
            <p
              className="mt-3 px-(--gutter) text-sm text-warning-subtle-fg"
              role="alert"
              data-testid="attention-failed-checks"
            >
              Not checked because their reads failed: {failedChecks.join(", ")}.
            </p>
          )}
        </CardContent>
      </Card>

      <Card className="[--gutter:--spacing(4)] sm:[--gutter:--spacing(6)]">
        <CardHeader title="Queues needing attention">
          <CardAction>
            <Link to="/queues" className={`${entityLinkClass} text-sm`}>
              All queues →
            </Link>
          </CardAction>
        </CardHeader>
        <CardContent>
          {queuesQuery.isError ? (
            <p className="text-sm text-danger-subtle-fg" role="alert">
              Failed to load queues: {queuesQuery.error.message}
            </p>
          ) : queuesQuery.isLoading ? (
            <p className="text-sm text-muted-fg">Loading queues…</p>
          ) : topQueues.length === 0 ? (
            <p className="text-sm text-muted-fg" data-testid="queues-attention-empty">
              No queue has failures, retries, dead letters or held work.
            </p>
          ) : (
            <>
              <ul className="divide-y sm:hidden" aria-label="Queues needing attention">
                {topQueues.map((q) => (
                  <li key={q.queue} className="py-3">
                    <div className="flex items-center gap-2">
                      <QueueLink queue={q.queue} descriptor={q} className="font-medium" />
                      {q.paused && <PausedBadge queue={q} />}
                    </div>
                    <div className="mt-1 text-sm text-muted-fg">
                      {[
                        q.available > 0 && `${formatNumber(q.available)} ready`,
                        q.retryable > 0 && `${formatNumber(q.retryable)} retrying`,
                        q.liveFailed > 0 && `${formatNumber(q.liveFailed)} failed`,
                        (q.deadLetters ?? 0) > 0 && plural(q.deadLetters!, "dead letter"),
                      ]
                        .filter(Boolean)
                        .join(" · ")}
                    </div>
                  </li>
                ))}
              </ul>
              <Table bleed aria-label="Queues needing attention" className="hidden sm:block">
                <TableHeader>
                  <TableColumn isRowHeader>Queue</TableColumn>
                  <TableColumn className="text-right">Ready</TableColumn>
                  <TableColumn className="text-right">Oldest ready</TableColumn>
                  <TableColumn className="text-right">Retrying</TableColumn>
                  <TableColumn className="text-right">Failed</TableColumn>
                  <TableColumn className="text-right">Dead letters</TableColumn>
                </TableHeader>
                <TableBody>
                  {topQueues.map((q) => (
                    <TableRow key={q.queue} id={q.queue}>
                      <TableCell>
                        <span className="inline-flex items-center gap-2">
                          <QueueLink queue={q.queue} descriptor={q} className="font-medium" />
                          {q.paused && <PausedBadge queue={q} />}
                        </span>
                      </TableCell>
                      <TableCell className="text-right tabular-nums">{formatNumber(q.available)}</TableCell>
                      <TableCell className="text-right tabular-nums">
                        {q.available > 0 && q.lag_seconds !== null ? formatDuration(q.lag_seconds) : "—"}
                      </TableCell>
                      <TableCell className="text-right tabular-nums">{formatNumber(q.retryable)}</TableCell>
                      <TableCell className={`text-right tabular-nums ${q.liveFailed > 0 ? "text-danger-subtle-fg" : ""}`}>
                        {formatNumber(q.liveFailed)}
                      </TableCell>
                      <TableCell className="text-right tabular-nums">
                        {q.deadLetters === null ? "—" : formatNumber(q.deadLetters)}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </>
          )}
        </CardContent>
      </Card>
    </Page>
  );
}
