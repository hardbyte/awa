import type { ReactNode } from "react";
import { Link } from "@tanstack/react-router";
import type { QueueRuntimeSummary, QueueStats } from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { HistorySpark } from "@/components/HistorySpark";
import {
  capacityLabel,
  formatCount,
  formatDuration,
  formatRate,
  type AssessedQueue,
  type QueueHealth,
} from "@/lib/operations";

export const HEALTH_BADGE: Record<
  QueueHealth,
  "danger" | "warning" | "info" | "secondary" | "outline"
> = {
  stalled: "danger",
  behind: "warning",
  slow: "warning",
  paused: "secondary",
  draining: "info",
  unknown: "outline",
  steady: "outline",
  idle: "outline",
};

/** Text tint for the backlog sparkline, so the trend reads at a glance. */
export const SPARK_TONE: Record<QueueHealth, string> = {
  stalled: "text-danger",
  behind: "text-warning",
  slow: "text-warning",
  paused: "text-muted-fg",
  draining: "text-info",
  unknown: "text-muted-fg",
  steady: "text-muted-fg",
  idle: "text-muted-fg",
};

function queueName(queue: QueueStats): string {
  return queue.display_name?.trim() || queue.queue;
}

function JobsLink({
  queue,
  state,
  value,
  className = "",
}: {
  queue: string;
  state: string;
  value: number;
  className?: string;
}) {
  return (
    <Link
      to="/jobs"
      search={{ state, q: `queue:${queue}` }}
      className={`tabular-nums hover:underline ${value === 0 ? "text-muted-fg" : className}`}
    >
      {value.toLocaleString()}
    </Link>
  );
}

function Capacity({ summary }: { summary: QueueRuntimeSummary | undefined }) {
  if (summary && summary.live_instances === 0) {
    return <span className="text-danger-subtle-fg">No live workers</span>;
  }
  const title = summary
    ? `${summary.healthy_instances}/${summary.live_instances} live instances healthy${summary.config_mismatch ? " · config differs between instances" : ""}`
    : "No runtime has reported this queue";
  return (
    <span
      title={title}
      className={summary?.config_mismatch ? "text-warning-subtle-fg" : ""}
    >
      {capacityLabel(summary)}
      {summary?.config_mismatch && " · mismatch"}
      {summary?.config?.dlq_enabled === false && (
        <>
          {" · "}
          <span>DLQ off</span>
        </>
      )}
    </span>
  );
}

/** Descriptor problems are rare and worth seeing without opening the queue. */
function DescriptorFlags({ queue }: { queue: QueueStats }) {
  return (
    <>
      {queue.descriptor_stale && (
        <Badge
          intent="warning"
          className="ml-2"
          title="No runtime has re-synced this descriptor recently"
        >
          Descriptor stale
        </Badge>
      )}
      {queue.descriptor_mismatch && (
        <Badge
          intent="danger"
          className="ml-2"
          title="Live runtimes declare different descriptors for this queue"
        >
          Descriptor drift
        </Badge>
      )}
    </>
  );
}

function TrendText({ row }: { row: AssessedQueue }) {
  const { trend, clearSeconds, health } = row.assessment;
  if (!trend) return <span className="text-muted-fg">—</span>;
  // Below the noise threshold the fitted slope is batching jitter; printing
  // "-0.6/min" would invite reading meaning into it.
  if (health === "steady" || health === "idle" || health === "paused") {
    return <span className="text-muted-fg">flat</span>;
  }
  return (
    <span className="tabular-nums">
      {formatRate(trend.perMinute, true)}
      {clearSeconds !== null && (
        <span className="text-muted-fg">
          {" "}
          · ~{formatDuration(clearSeconds)}
        </span>
      )}
    </span>
  );
}

/**
 * One line per queue, ordered by the caller. Switches to stacked cards when
 * its own container is narrow (not the viewport: the docked sidebar takes a
 * fixed share of a tablet screen).
 */
export function QueueFlowTable({
  rows,
  controls,
  runtime,
  label,
}: {
  rows: ReadonlyArray<AssessedQueue>;
  controls?: (queue: QueueStats) => ReactNode;
  /** When given, adds a capacity column from live runtime config. */
  runtime?: ReadonlyMap<string, QueueRuntimeSummary>;
  label: string;
}) {
  return (
    <div className="@container">
      <div className="hidden overflow-x-auto @4xl:block">
        <table className="w-full text-sm" aria-label={label}>
          <thead>
            <tr className="border-b text-left text-xs text-muted-fg [&>th]:px-2.5 [&>th]:py-2 [&>th]:font-medium [&>th]:whitespace-nowrap">
              <th scope="col" className="pl-4">
                Queue
              </th>
              <th scope="col">Status</th>
              <th scope="col" className="text-right">
                Waiting
              </th>
              <th scope="col">Last hour</th>
              <th
                scope="col"
                className="text-right"
                title="Fitted change in waiting jobs over the last 5 minutes"
              >
                Trend
              </th>
              <th scope="col" className="text-right">
                Oldest wait
              </th>
              <th scope="col" className="text-right">
                Running
              </th>
              {runtime && <th scope="col">Capacity</th>}
              <th
                scope="col"
                className="text-right"
                title="Average over the last hour"
              >
                Done/min
              </th>
              <th
                scope="col"
                className={controls ? "text-right" : "pr-4 text-right"}
              >
                Retrying / failed
              </th>
              {controls && (
                <th scope="col" className="pr-4">
                  <span className="sr-only">Controls</span>
                </th>
              )}
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => {
              const { queue, assessment } = row;
              return (
                <tr
                  key={queue.queue}
                  className="border-b last:border-b-0 hover:bg-muted/50 [&>td]:px-2.5 [&>td]:py-2 [&>td]:whitespace-nowrap"
                >
                  <th
                    scope="row"
                    className="max-w-64 truncate py-2 pr-2.5 pl-4 text-left font-medium"
                  >
                    <Link
                      to="/queues/$name"
                      params={{ name: queue.queue }}
                      className="text-primary hover:underline"
                      title={queue.description ?? queue.queue}
                    >
                      {queueName(queue)}
                    </Link>
                    {queue.display_name?.trim() && (
                      <span className="ml-2 font-mono text-xs font-normal text-muted-fg">
                        {queue.queue}
                      </span>
                    )}
                    <DescriptorFlags queue={queue} />
                  </th>
                  <td>
                    <Badge
                      intent={HEALTH_BADGE[assessment.health]}
                      title={assessment.detail}
                    >
                      {assessment.label}
                    </Badge>
                  </td>
                  <td className="text-right">
                    <JobsLink
                      queue={queue.queue}
                      state="available"
                      value={queue.available}
                    />
                  </td>
                  <td className={SPARK_TONE[assessment.health]}>
                    <HistorySpark
                      data={row.series?.available ?? []}
                      width={96}
                      height={20}
                      label={`${queueName(queue)} waiting jobs over the last hour`}
                    />
                  </td>
                  <td className="text-right">
                    <TrendText row={row} />
                  </td>
                  <td className="text-right tabular-nums">
                    {queue.available > 0 ? (
                      formatDuration(queue.lag_seconds)
                    ) : (
                      <span className="text-muted-fg">—</span>
                    )}
                  </td>
                  <td className="text-right">
                    <JobsLink
                      queue={queue.queue}
                      state="running"
                      value={queue.running}
                    />
                  </td>
                  {runtime && (
                    <td>
                      <Capacity summary={runtime.get(queue.queue)} />
                    </td>
                  )}
                  <td className="text-right tabular-nums">
                    {formatCount(Math.round(queue.completed_last_hour / 60))}
                  </td>
                  <td className={controls ? "text-right" : "pr-4 text-right"}>
                    <JobsLink
                      queue={queue.queue}
                      state="retryable"
                      value={queue.retryable}
                      className="text-warning-subtle-fg"
                    />
                    <span className="text-muted-fg" aria-hidden="true">
                      {" "}
                      /{" "}
                    </span>
                    <JobsLink
                      queue={queue.queue}
                      state="failed"
                      value={queue.failed}
                      className="text-danger-subtle-fg"
                    />
                  </td>
                  {controls && (
                    <td className="pr-4 text-right">{controls(queue)}</td>
                  )}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      <ul className="divide-y @4xl:hidden" aria-label={label}>
        {rows.map((row) => {
          const { queue, assessment } = row;
          return (
            <li key={queue.queue} className="space-y-2 px-4 py-3">
              <div className="flex items-start justify-between gap-3">
                <div className="min-w-0">
                  <Link
                    to="/queues/$name"
                    params={{ name: queue.queue }}
                    className="block truncate font-medium text-primary hover:underline"
                  >
                    {queueName(queue)}
                  </Link>
                  <p className="text-xs text-muted-fg">{assessment.detail}</p>
                  {(queue.descriptor_stale || queue.descriptor_mismatch) && (
                    <div className="-ml-2 mt-1">
                      <DescriptorFlags queue={queue} />
                    </div>
                  )}
                </div>
                <Badge
                  intent={HEALTH_BADGE[assessment.health]}
                  className="shrink-0"
                >
                  {assessment.label}
                </Badge>
              </div>
              <div
                className={`flex items-center gap-3 ${SPARK_TONE[assessment.health]}`}
              >
                <HistorySpark
                  data={row.series?.available ?? []}
                  width={160}
                  height={24}
                  label={`${queueName(queue)} waiting jobs over the last hour`}
                  className="shrink-0"
                />
                <span className="text-xs text-fg">
                  <TrendText row={row} />
                </span>
              </div>
              <dl className="grid grid-cols-3 gap-x-3 gap-y-1 text-xs [&_dd]:text-sm [&_dd]:tabular-nums [&_dt]:text-muted-fg">
                <div>
                  <dt>Waiting</dt>
                  <dd>
                    <JobsLink
                      queue={queue.queue}
                      state="available"
                      value={queue.available}
                    />
                  </dd>
                </div>
                <div>
                  <dt>Oldest wait</dt>
                  <dd>
                    {queue.available > 0
                      ? formatDuration(queue.lag_seconds)
                      : "—"}
                  </dd>
                </div>
                <div>
                  <dt>Running</dt>
                  <dd>
                    <JobsLink
                      queue={queue.queue}
                      state="running"
                      value={queue.running}
                    />
                  </dd>
                </div>
                <div>
                  <dt>Done/min</dt>
                  <dd>
                    {formatCount(Math.round(queue.completed_last_hour / 60))}
                  </dd>
                </div>
                <div>
                  <dt>Retrying</dt>
                  <dd>
                    <JobsLink
                      queue={queue.queue}
                      state="retryable"
                      value={queue.retryable}
                      className="text-warning-subtle-fg"
                    />
                  </dd>
                </div>
                <div>
                  <dt>Failed</dt>
                  <dd>
                    <JobsLink
                      queue={queue.queue}
                      state="failed"
                      value={queue.failed}
                      className="text-danger-subtle-fg"
                    />
                  </dd>
                </div>
                {runtime && (
                  <div className="col-span-3">
                    <dt>Capacity</dt>
                    <dd>
                      <Capacity summary={runtime.get(queue.queue)} />
                    </dd>
                  </div>
                )}
              </dl>
              {controls && <div>{controls(queue)}</div>}
            </li>
          );
        })}
      </ul>
    </div>
  );
}
