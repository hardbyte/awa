import type {
  HealthReport,
  JobRow,
  LimitedBy,
  QueueAssessment,
  QueueHealthKind,
  QueueHistory,
  QueueRuntimeSummary,
  QueueSeries,
  QueueStats,
  RuntimeInstance,
} from "./api";

export function formatDuration(seconds: number | null): string {
  if (seconds === null || !Number.isFinite(seconds)) return "—";
  const value = Math.max(0, Math.round(seconds));
  if (value < 60) return `${value}s`;
  if (value < 3600) return `${Math.floor(value / 60)}m ${value % 60}s`;
  if (value < 86400)
    return `${Math.floor(value / 3600)}h ${Math.floor((value % 3600) / 60)}m`;
  return `${Math.floor(value / 86400)}d ${Math.floor((value % 86400) / 3600)}h`;
}

const compact = new Intl.NumberFormat("en", {
  notation: "compact",
  maximumFractionDigits: 1,
});

export function formatCount(value: number): string {
  return Math.abs(value) < 10_000
    ? value.toLocaleString()
    : compact.format(value);
}

export function formatRate(perMinute: number, signed = false): string {
  const magnitude = Math.abs(perMinute);
  const rounded =
    magnitude >= 10 ? formatCount(Math.round(magnitude)) : magnitude.toFixed(1);
  const sign = !signed ? "" : perMinute > 0 ? "+" : perMinute < 0 ? "−" : "";
  return `${sign}${rounded}/min`;
}

/** A queue joined with the server's assessment and its history series. */
export interface AssessedQueue {
  queue: QueueStats;
  /** Missing only while the health report is loading or failed. */
  assessment: QueueAssessment | undefined;
  series: QueueSeries | undefined;
}

/** Join snapshots to the health report, in the report's risk order. Queues
 * the report does not know yet (created since it was built) go last. */
export function joinQueues(
  queues: ReadonlyArray<QueueStats>,
  report: HealthReport | undefined,
  history: QueueHistory | undefined,
): AssessedQueue[] {
  const order = new Map(report?.queues.map((a, index) => [a.queue, index]));
  const byName = new Map(report?.queues.map((a) => [a.queue, a]));
  return queues
    .map((queue) => ({
      queue,
      assessment: byName.get(queue.queue),
      series: history?.queues[queue.queue],
    }))
    .sort(
      (left, right) =>
        (order.get(left.queue.queue) ?? Infinity) -
          (order.get(right.queue.queue) ?? Infinity) ||
        left.queue.queue.localeCompare(right.queue.queue),
    );
}

export const LIMITED_BY_LABEL: Record<LimitedBy, string> = {
  paused: "Paused",
  no_workers: "No workers",
  rate_limit: "Rate limit",
  worker_capacity: "Worker capacity",
  nothing: "No visible limit",
};

export const LIMITED_BY_HINT: Record<LimitedBy, string> = {
  paused: "The queue is paused, so nothing is claimed.",
  no_workers: "No live worker runtime declares this queue.",
  rate_limit:
    "Dispatch skipped polls in the last 5 minutes because the queue's rate limit had no tokens.",
  worker_capacity: "Every worker permit for this queue is in use.",
  nothing:
    "Workers had free permits. If work still waits, look at claim latency, the database, or job duration.",
};

export function needsAttention(row: AssessedQueue): boolean {
  return row.assessment?.needs_attention ?? false;
}

export function healthOf(row: AssessedQueue): QueueHealthKind {
  return row.assessment?.health ?? "unknown";
}

/** Per-bucket total of waiting jobs across queues; null where no queue was
 * sampled. */
export function totalBacklog(
  history: QueueHistory | undefined,
): (number | null)[] {
  if (!history) return [];
  const totals: (number | null)[] = history.buckets.map(() => null);
  for (const series of Object.values(history.queues)) {
    series.available.forEach((value, index) => {
      if (value === null) return;
      totals[index] = (totals[index] ?? 0) + value;
    });
  }
  return totals;
}

/** Sum a per-queue rate across the report, or null when any queue with
 * live workers has it unknown: a partial total would read as a real drop. */
export function totalRate(
  report: HealthReport | undefined,
  pick: (flow: QueueAssessment["flow"]) => number | null,
): number | null {
  if (!report) return null;
  let total = 0;
  for (const queue of report.queues) {
    const value = pick(queue.flow);
    if (value === null) {
      if (queue.live_workers > 0) return null;
      continue;
    }
    total += value;
  }
  return total;
}

/** Bucket index of each fleet event, for chart markers. */
export function eventIndexes(history: QueueHistory | undefined): number[] {
  if (!history) return [];
  const starts = history.buckets.map((bucket) => new Date(bucket).getTime());
  return history.events
    .map((event) => starts.indexOf(new Date(event.at).getTime()))
    .filter((index) => index >= 0);
}

export function lastErrorMessage(job: JobRow): string {
  const last = job.errors?.[job.errors.length - 1];
  if (typeof last === "object" && last && "error" in last) {
    return String((last as { error: unknown }).error);
  }
  return last ? JSON.stringify(last) : "No error details recorded";
}

export interface FailureGroup {
  key: string;
  kind: string;
  queue: string;
  message: string;
  count: number;
  latest: JobRow;
}

/** Group retained failed jobs by kind, queue, and error text so one recurring
 * fault reads as one line with a count. Long digit runs and UUIDs are masked
 * so per-job identifiers do not split a group. */
export function groupFailures(jobs: ReadonlyArray<JobRow>): FailureGroup[] {
  const groups = new Map<string, FailureGroup>();
  for (const job of jobs) {
    const message = lastErrorMessage(job);
    const signature = message
      .replace(
        /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi,
        "…",
      )
      .replace(/\d{4,}/g, "…");
    const key = `${job.kind}\u0000${job.queue}\u0000${signature}`;
    const group = groups.get(key);
    if (group) {
      group.count += 1;
      if ((job.finalized_at ?? "") > (group.latest.finalized_at ?? "")) {
        group.latest = job;
        group.message = message;
      }
    } else {
      groups.set(key, {
        key,
        kind: job.kind,
        queue: job.queue,
        message,
        count: 1,
        latest: job,
      });
    }
  }
  return [...groups.values()].sort(
    (left, right) =>
      right.count - left.count ||
      (right.latest.finalized_at ?? "").localeCompare(
        left.latest.finalized_at ?? "",
      ),
  );
}

/** Concurrency and rate limit as configured by the live runtimes, e.g.
 * "max 64 · 2/s". Rate limits are the usual reason a queue backs up while
 * workers sit idle, so they belong next to the backlog. */
export function capacityLabel(
  summary: QueueRuntimeSummary | undefined,
): string {
  const config = summary?.config;
  if (!config) return "—";
  const concurrency =
    config.mode === "weighted"
      ? `min ${config.min_workers ?? 0} · weight ${config.weight ?? 1}`
      : `max ${config.max_workers ?? 0}`;
  return config.rate_limit
    ? `${concurrency} · ${config.rate_limit.max_rate}/s`
    : concurrency;
}

export function runtimeConcern(instance: RuntimeInstance): string {
  if (instance.stale) return "Snapshot stale · health unknown";
  if (instance.shutting_down) return "Shutting down";
  const problems = [
    !instance.postgres_connected && "Postgres disconnected",
    !instance.poll_loop_alive && "Poll loop stopped",
    !instance.heartbeat_alive && "Heartbeat loop stopped",
    !instance.maintenance_alive && "Maintenance loop stopped",
  ].filter(Boolean);
  return problems.join(" · ") || (instance.healthy ? "Healthy" : "Degraded");
}
