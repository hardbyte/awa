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

/** Dashboard queue table limit */
export const DASHBOARD_QUEUE_LIMIT = 10;
