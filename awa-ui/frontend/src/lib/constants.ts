/** Polling intervals in milliseconds */
export const POLL = {
  /** Jobs list, job detail — fast refresh for active monitoring */
  FAST: 2_000,
  /** Dashboard, queues, cron — moderate refresh */
  DEFAULT: 5_000,
  /** Autocomplete data (distinct kinds/queues) — changes rarely */
  SLOW: 60_000,
} as const;

/** Job states that can be retried */
export const RETRYABLE_STATES = new Set([
  "failed",
  "cancelled",
  "waiting_external",
]);

/** Terminal job states (no further transitions) */
export const TERMINAL_STATES = new Set(["completed", "failed", "cancelled"]);

/** Default page size for job lists */
export const DEFAULT_PAGE_SIZE = 50;

/** Available page sizes */
export const PAGE_SIZES = [20, 50, 100] as const;

/** Max suggestions in autocomplete */
export const MAX_SUGGESTIONS = 8;

/** Queues the Overview lists under "Queues needing attention". */
export const OVERVIEW_QUEUE_LIMIT = 5;

/** Newest failed jobs the Overview groups by kind and error. */
export const OVERVIEW_FAILED_SAMPLE = 50;

/** Newest running jobs the Overview checks for stuck ones. */
export const OVERVIEW_RUNNING_SAMPLE = 100;

/** Failure groups the attention list shows. */
export const ATTENTION_FAILURE_GROUPS = 5;

/** An unpaused queue whose oldest ready job has waited this long needs attention. */
export const LAG_ATTENTION_SECONDS = 60;

/** A running job whose heartbeat is older than this reads as stuck: three of
 * the default 30-second heartbeat intervals. */
export const STUCK_HEARTBEAT_MS = 90_000;

/** A failed bulk operation stays on the Overview for this long. */
export const BATCH_FAILURE_WINDOW_MS = 24 * 3600_000;

/** A schedule whose next fire is this far in the past reads as overdue. */
export const CRON_OVERDUE_GRACE_MS = 60_000;

/** A bulk cancel or retry over this many jobs, or over a count the server
 * could only bound from below, asks the user to type the count to confirm. */
export const TYPE_TO_CONFIRM_MIN_JOBS = 1_000;

/** Rows of a bulk action's preview sample shown in its dialog. */
export const BULK_SAMPLE_ROWS = 8;

/** The window the Overview reads outcomes over: the last hour and the one
 * before it, in one-minute buckets. */
export const OVERVIEW_OUTCOMES = { minutes: 120, bucketSeconds: 60 } as const;

/** A failure rate needs attention when, over the last hour, at least
 * `minFailed` jobs failed, at least `minRate` of finished jobs failed, and
 * the rate is at least `factor` times the previous hour's. */
export const FAILURE_RATE_ATTENTION = { minFailed: 20, minRate: 0.05, factor: 2 } as const;
