import type {
  JobRow,
  QueueHistory,
  QueueRuntimeSummary,
  QueueSeries,
  QueueStats,
  RuntimeInstance,
} from "./api";

/** Trailing window used to judge whether a backlog is growing or shrinking. */
export const TREND_MINUTES = 5;
/** Work waiting this long with nothing running means no worker is claiming. */
export const STALL_AFTER_SECONDS = 60;
/** Oldest available job older than this is a long wait even if the depth holds. */
export const SLOW_WAIT_SECONDS = 300;
/**
 * A backlog only counts as growing or draining when the fitted change over
 * the trend window exceeds this many jobs or this share of the backlog,
 * whichever is larger. Below that it is noise from batch enqueues and claims.
 */
const MIN_TREND_JOBS = 20;
const MIN_TREND_SHARE = 0.05;

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

export interface Trend {
  /** Least-squares slope in jobs per minute. */
  perMinute: number;
  /** Fitted change across the observed span. */
  change: number;
  /** Minutes between the first and last sample used. */
  minutes: number;
}

/** Fit the trailing `minutes` of a bucketed series. Null until there are at
 * least three samples spanning two minutes, so a single noisy pair of
 * snapshots never reads as a trend. */
export function trailingTrend(
  series: ReadonlyArray<number | null>,
  bucketSecs: number,
  minutes = TREND_MINUTES,
): Trend | null {
  const take = Math.floor((minutes * 60) / bucketSecs) + 1;
  const start = Math.max(0, series.length - take);
  const points: [number, number][] = [];
  for (let index = start; index < series.length; index++) {
    const value = series[index];
    if (value === null || value === undefined) continue;
    points.push([(index * bucketSecs) / 60, value]);
  }
  if (points.length < 3) return null;
  const span = points[points.length - 1]![0] - points[0]![0];
  if (span < 2) return null;
  const meanX = points.reduce((sum, [x]) => sum + x, 0) / points.length;
  const meanY = points.reduce((sum, [, y]) => sum + y, 0) / points.length;
  let numerator = 0;
  let denominator = 0;
  for (const [x, y] of points) {
    numerator += (x - meanX) * (y - meanY);
    denominator += (x - meanX) ** 2;
  }
  const perMinute = denominator ? numerator / denominator : 0;
  return { perMinute, change: perMinute * span, minutes: span };
}

export type QueueHealth =
  | "stalled"
  | "behind"
  | "slow"
  | "paused"
  | "draining"
  | "unknown"
  | "steady"
  | "idle";

export interface QueueAssessment {
  health: QueueHealth;
  label: string;
  detail: string;
  trend: Trend | null;
  /** Seconds to clear the current backlog at the trend's pace, when draining. */
  clearSeconds: number | null;
  needsAttention: boolean;
  rank: number;
}

const RANK: Record<QueueHealth, number> = {
  stalled: 0,
  behind: 1,
  slow: 2,
  paused: 3,
  draining: 4,
  unknown: 5,
  steady: 6,
  idle: 7,
};

/**
 * Classify one queue from its current snapshot and sampled history.
 *
 * Only `available` jobs count as backlog: scheduled, retryable and
 * waiting_external jobs are not claimable yet, so their growth is not a sign
 * of falling behind.
 */
export function assessQueue(
  queue: QueueStats,
  series: QueueSeries | undefined,
  bucketSecs: number,
  historyEnabled = true,
): QueueAssessment {
  const backlog = queue.available;
  const wait = queue.lag_seconds ?? 0;
  const trend = series ? trailingTrend(series.available, bucketSecs) : null;
  const threshold = Math.max(MIN_TREND_JOBS, backlog * MIN_TREND_SHARE);
  const result = (
    health: QueueHealth,
    label: string,
    detail: string,
    clearSeconds: number | null = null,
  ): QueueAssessment => ({
    health,
    label,
    detail,
    trend,
    clearSeconds,
    rank: RANK[health],
    needsAttention:
      health === "stalled" ||
      health === "behind" ||
      health === "slow" ||
      (health === "paused" && backlog > 0),
  });

  if (queue.paused) {
    return backlog > 0
      ? result(
          "paused",
          "Paused",
          `${backlog.toLocaleString()} waiting while paused`,
        )
      : { ...result("paused", "Paused", "Nothing waiting"), rank: RANK.idle };
  }
  if (backlog > 0 && queue.running === 0 && wait >= STALL_AFTER_SECONDS) {
    return result(
      "stalled",
      "Not being worked",
      `Nothing running; oldest has waited ${formatDuration(wait)}`,
    );
  }
  if (trend && backlog > 0 && trend.change > threshold) {
    return result(
      "behind",
      "Falling behind",
      `Backlog ${formatRate(trend.perMinute, true)} over ${Math.round(trend.minutes)}m`,
    );
  }
  if (wait >= SLOW_WAIT_SECONDS) {
    return result(
      "slow",
      "Long wait",
      `Oldest has waited ${formatDuration(wait)}`,
    );
  }
  if (trend && backlog > 0 && trend.change < -threshold) {
    const clearSeconds = (backlog / -trend.perMinute) * 60;
    return result(
      "draining",
      "Draining",
      `Clears in about ${formatDuration(clearSeconds)} at this pace`,
      clearSeconds,
    );
  }
  if (backlog === 0) return result("idle", "Idle", "Nothing waiting");
  if (!trend) {
    return result(
      "unknown",
      "No trend yet",
      historyEnabled
        ? "Needs a few minutes of samples"
        : "Queue history is disabled on this server",
    );
  }
  return result("steady", "Steady", "Backlog holding level");
}

export interface AssessedQueue {
  queue: QueueStats;
  series: QueueSeries | undefined;
  assessment: QueueAssessment;
}

export function assessQueues(
  queues: ReadonlyArray<QueueStats>,
  history: QueueHistory | null | undefined,
): AssessedQueue[] {
  const bucketSecs = history?.bucket_secs ?? 60;
  const enabled = history?.enabled ?? false;
  return queues
    .map((queue) => {
      const series = history?.queues[queue.queue];
      return {
        queue,
        series,
        assessment: assessQueue(queue, series, bucketSecs, enabled),
      };
    })
    .sort(compareByRisk);
}

export function compareByRisk(
  left: AssessedQueue,
  right: AssessedQueue,
): number {
  return (
    left.assessment.rank - right.assessment.rank ||
    right.queue.available - left.queue.available ||
    (right.queue.lag_seconds ?? 0) - (left.queue.lag_seconds ?? 0) ||
    left.queue.queue.localeCompare(right.queue.queue)
  );
}

/** Per-bucket total of available jobs across every sampled queue. */
export function totalBacklog(
  history: QueueHistory | null | undefined,
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

export interface Verdict {
  tone: "danger" | "warning" | "info" | "success" | "muted";
  title: string;
  detail: string;
}

function plural(count: number, noun: string): string {
  return `${count.toLocaleString()} ${noun}${count === 1 ? "" : "s"}`;
}

/** One sentence answering "is the system keeping up?". */
export function overallVerdict(
  assessed: ReadonlyArray<AssessedQueue>,
  historyEnabled: boolean,
): Verdict {
  const count = (health: QueueHealth) =>
    assessed.filter((row) => row.assessment.health === health).length;
  const pausedWithWork = assessed.filter(
    (row) => row.assessment.health === "paused" && row.queue.available > 0,
  ).length;
  const names = (health: QueueHealth) =>
    assessed
      .filter((row) => row.assessment.health === health)
      .slice(0, 3)
      .map((row) => row.queue.display_name?.trim() || row.queue.queue)
      .join(", ");
  const others = [
    count("behind") && `${plural(count("behind"), "queue")} falling behind`,
    count("slow") && `${plural(count("slow"), "queue")} with long waits`,
    pausedWithWork && `${plural(pausedWithWork, "paused queue")} holding work`,
  ].filter(Boolean) as string[];

  if (count("stalled")) {
    return {
      tone: "danger",
      title: `${plural(count("stalled"), "queue")} not being worked`,
      detail: [
        `Waiting work with nothing running: ${names("stalled")}.`,
        ...others,
      ].join(" · "),
    };
  }
  if (count("behind")) {
    return {
      tone: "warning",
      title: `${plural(count("behind"), "queue")} falling behind`,
      detail: [
        `Backlog growing in ${names("behind")}.`,
        ...others.slice(1),
      ].join(" · "),
    };
  }
  if (count("slow")) {
    return {
      tone: "warning",
      title: `${plural(count("slow"), "queue")} with long waits`,
      detail: [
        `Oldest jobs waiting over ${formatDuration(SLOW_WAIT_SECONDS)} in ${names("slow")}.`,
        ...others.slice(1),
      ].join(" · "),
    };
  }
  if (pausedWithWork) {
    return {
      tone: "info",
      title: `${plural(pausedWithWork, "paused queue")} holding work`,
      detail:
        "Every running queue is keeping up. Resume paused queues when ready.",
    };
  }
  const unknown = count("unknown");
  if (
    unknown &&
    unknown === assessed.filter((row) => row.queue.available > 0).length
  ) {
    return {
      tone: "muted",
      title: "Not enough history yet",
      detail: historyEnabled
        ? "Trends appear after a few minutes of samples. No queue is stalled or waiting long."
        : "Queue history is disabled on this server, so only stalls and long waits are detected.",
    };
  }
  return {
    tone: "success",
    title: "Keeping up",
    detail: `${plural(assessed.length, "queue")} steady, draining, or idle.`,
  };
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
