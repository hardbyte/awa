/** Typed fetch wrapper for the AWA API. */

export interface JobRow {
  id: number;
  kind: string;
  queue: string;
  args: unknown;
  state: string;
  priority: number;
  attempt: number;
  max_attempts: number;
  run_at: string;
  heartbeat_at: string | null;
  deadline_at: string | null;
  attempted_at: string | null;
  finalized_at: string | null;
  created_at: string;
  errors: unknown[] | null;
  metadata: unknown;
  tags: string[];
  progress: unknown | null;
  // Webhook callback fields
  callback_id: string | null;
  callback_timeout_at: string | null;
  callback_filter: string | null;
  callback_on_complete: string | null;
  callback_on_fail: string | null;
  callback_transform: string | null;
  // Computed by the API — the priority assigned at enqueue time,
  // before maintenance-based aging.
  original_priority: number;
  queue_descriptor: DescriptorFields | null;
  kind_descriptor: DescriptorFields | null;
}

export interface DescriptorFields {
  display_name: string | null;
  description: string | null;
  owner: string | null;
  docs_url: string | null;
  tags: string[];
  extra: unknown;
}

export interface QueueOverview extends DescriptorFields {
  queue: string;
  descriptor_last_seen_at: string | null;
  descriptor_stale: boolean;
  descriptor_mismatch: boolean;
  total_queued: number;
  scheduled: number;
  available: number;
  retryable: number;
  running: number;
  failed: number;
  waiting_external: number;
  completed_last_hour: number;
  lag_seconds: number | null;
  paused: boolean;
}

export type QueueStats = QueueOverview;

export interface JobKindOverview extends DescriptorFields {
  kind: string;
  descriptor_last_seen_at: string | null;
  descriptor_stale: boolean;
  descriptor_mismatch: boolean;
  job_count: number;
  queue_count: number;
  completed_last_hour: number;
}

export interface RateLimitSnapshot {
  max_rate: number;
  burst: number;
}

export interface QueueRuntimeConfigSnapshot {
  mode: "hard_reserved" | "weighted";
  max_workers: number | null;
  min_workers: number | null;
  weight: number | null;
  global_max_workers: number | null;
  poll_interval_ms: number;
  deadline_duration_secs: number;
  priority_aging_interval_secs: number;
  rate_limit: RateLimitSnapshot | null;
}

export interface QueueRuntimeSnapshot {
  queue: string;
  in_flight: number;
  overflow_held: number | null;
  config: QueueRuntimeConfigSnapshot;
}

export interface RuntimeInstance {
  instance_id: string;
  hostname: string | null;
  pid: number;
  version: string;
  /**
   * Worker's storage capability — populated by schema v10+. On earlier
   * deployments the field is absent; treat as "canonical" when reading.
   */
  storage_capability?: string | null;
  started_at: string;
  last_seen_at: string;
  snapshot_interval_ms: number;
  stale: boolean;
  healthy: boolean;
  postgres_connected: boolean;
  poll_loop_alive: boolean;
  heartbeat_alive: boolean;
  maintenance_alive: boolean;
  shutting_down: boolean;
  leader: boolean;
  global_max_workers: number | null;
  queues: QueueRuntimeSnapshot[];
}

export interface RuntimeOverview {
  total_instances: number;
  live_instances: number;
  stale_instances: number;
  healthy_instances: number;
  leader_instances: number;
  instances: RuntimeInstance[];
}

export interface QueueRuntimeSummary {
  queue: string;
  instance_count: number;
  live_instances: number;
  stale_instances: number;
  healthy_instances: number;
  total_in_flight: number;
  overflow_held_total: number | null;
  config_mismatch: boolean;
  config: QueueRuntimeConfigSnapshot | null;
}

export interface CronJobRow {
  name: string;
  cron_expr: string;
  timezone: string;
  kind: string;
  queue: string;
  args: unknown;
  priority: number;
  max_attempts: number;
  tags: string[];
  metadata: unknown;
  last_enqueued_at: string | null;
  next_fire_at: string | null;
  created_at: string;
  updated_at: string;
}

export type StateCounts = Record<string, number>;

export interface TimeseriesBucket {
  bucket: string;
  state: string;
  count: number;
}

export interface Capabilities {
  read_only: boolean;
  /** Server-suggested polling interval in milliseconds. */
  poll_interval_ms: number;
}

export interface StorageStatusReport {
  current_engine: string;
  active_engine: string;
  prepared_engine: string | null;
  state: string;
  transition_epoch: number;
  details: unknown;
  entered_at: string;
  updated_at: string;
  finalized_at: string | null;
  canonical_live_backlog: number;
  prepared_queue_storage_schema: string | null;
  prepared_schema_ready: boolean;
  live_runtime_capability_counts: Record<string, number>;
  can_enter_mixed_transition: boolean;
  enter_mixed_transition_blockers: string[];
  can_finalize: boolean;
  finalize_blockers: string[];
}

export interface ListJobsParams {
  state?: string;
  kind?: string;
  queue?: string;
  tag?: string;
  before_id?: number;
  limit?: number;
}

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`/api${path}`, {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error((body as { error?: string }).error ?? res.statusText);
  }
  return res.json() as Promise<T>;
}

// Jobs
export function fetchJobs(params: ListJobsParams = {}): Promise<JobRow[]> {
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== null && v !== "") qs.set(k, String(v));
  }
  const query = qs.toString();
  return apiFetch(`/jobs${query ? `?${query}` : ""}`);
}

export function fetchJob(id: number): Promise<JobRow> {
  return apiFetch(`/jobs/${id}`);
}

export function retryJob(id: number): Promise<JobRow> {
  return apiFetch(`/jobs/${id}/retry`, { method: "POST" });
}

export function cancelJob(id: number): Promise<JobRow> {
  return apiFetch(`/jobs/${id}/cancel`, { method: "POST" });
}

export function bulkRetry(ids: number[]): Promise<JobRow[]> {
  return apiFetch("/jobs/bulk-retry", {
    method: "POST",
    body: JSON.stringify({ ids }),
  });
}

export function bulkCancel(ids: number[]): Promise<JobRow[]> {
  return apiFetch("/jobs/bulk-cancel", {
    method: "POST",
    body: JSON.stringify({ ids }),
  });
}

// Queues
export function fetchQueues(): Promise<QueueOverview[]> {
  return apiFetch("/queues");
}

export function fetchQueue(queue: string): Promise<QueueOverview> {
  return apiFetch(`/queues/${encodeURIComponent(queue)}`);
}

export function fetchKinds(): Promise<JobKindOverview[]> {
  return apiFetch("/kinds");
}

export function fetchQueueRuntime(): Promise<QueueRuntimeSummary[]> {
  return apiFetch("/queues/runtime");
}

export function pauseQueue(
  queue: string,
  pausedBy?: string
): Promise<{ ok: boolean }> {
  return apiFetch(`/queues/${encodeURIComponent(queue)}/pause`, {
    method: "POST",
    body: JSON.stringify({ paused_by: pausedBy }),
  });
}

export function resumeQueue(queue: string): Promise<{ ok: boolean }> {
  return apiFetch(`/queues/${encodeURIComponent(queue)}/resume`, {
    method: "POST",
  });
}

export function drainQueue(queue: string): Promise<{ drained: number }> {
  return apiFetch(`/queues/${encodeURIComponent(queue)}/drain`, {
    method: "POST",
  });
}

// Cron
export function fetchCronJobs(): Promise<CronJobRow[]> {
  return apiFetch("/cron");
}

export function triggerCronJob(name: string): Promise<JobRow> {
  return apiFetch(`/cron/${encodeURIComponent(name)}/trigger`, {
    method: "POST",
  });
}

// Stats
export function fetchStats(): Promise<StateCounts> {
  return apiFetch("/stats");
}

export function fetchRuntime(): Promise<RuntimeOverview> {
  return apiFetch("/runtime");
}

// Returns null if the backend is older than 0.5.5-alpha and lacks /storage.
export async function fetchStorage(): Promise<StorageStatusReport | null> {
  const res = await fetch("/api/storage", {
    headers: { "Content-Type": "application/json" },
  });
  if (res.status === 404) return null;
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error((body as { error?: string }).error ?? res.statusText);
  }
  return res.json() as Promise<StorageStatusReport>;
}

export function fetchCapabilities(): Promise<Capabilities> {
  return apiFetch("/capabilities");
}

export function fetchTimeseries(
  minutes?: number
): Promise<TimeseriesBucket[]> {
  const qs = minutes ? `?minutes=${minutes}` : "";
  return apiFetch(`/stats/timeseries${qs}`);
}

export function fetchDistinctKinds(): Promise<string[]> {
  return apiFetch("/stats/kinds");
}

export function fetchDistinctQueues(): Promise<string[]> {
  return apiFetch("/stats/queues");
}
