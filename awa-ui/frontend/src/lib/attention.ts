/**
 * The Overview's health strip and attention list, computed from reads other
 * pages already make. Every function here is pure; `now` is passed in.
 */
import type {
  BatchOperation,
  CronJobRow,
  DlqDepthResponse,
  JobRow,
  QueueOverview,
  QueueRuntimeSummary,
  RuntimeInstance,
  RuntimeOverview,
} from "@/lib/api";
import { groupFailures } from "@/lib/errors";
import { isOverdue } from "@/lib/cron";
import { ACTIVE_BATCH_STATES } from "@/lib/shared-queries";
import {
  ATTENTION_FAILURE_GROUPS,
  BATCH_FAILURE_WINDOW_MS,
  LAG_ATTENTION_SECONDS,
  OVERVIEW_FAILED_SAMPLE,
  OVERVIEW_RUNNING_SAMPLE,
  STUCK_HEARTBEAT_MS,
} from "@/lib/constants";
import { formatDuration } from "@/lib/time";
import { pausedLabel } from "@/lib/pause";
import { formatNumber, plural } from "@/lib/number";

/** "action" needs an operator now, "warning" soon; a "notice" is worth knowing. */
export type AttentionSeverity = "action" | "warning" | "notice";

/** Where an attention item leads: a route and its params and search. */
export type AttentionTarget =
  | { to: "/jobs"; search: { state?: string; q?: string } }
  | { to: "/queues/$name"; params: { name: string } }
  | { to: "/queues/$name/jobs"; params: { name: string }; search: { state?: string } }
  | { to: "/queues/$name/dlq"; params: { name: string } }
  | { to: "/queues/$name/config"; params: { name: string } }
  | { to: "/runtime" }
  | { to: "/runtime/$instanceId"; params: { instanceId: string } }
  | { to: "/batch-ops/$id"; params: { id: string } }
  | { to: "/cron/$name"; params: { name: string } };

export interface AttentionItem {
  /** Stable across polls, for React keys and tests. */
  id: string;
  severity: AttentionSeverity;
  title: string;
  detail?: string;
  target: AttentionTarget;
}

export interface AttentionInput {
  now: number;
  queues?: QueueOverview[];
  queueRuntime?: QueueRuntimeSummary[];
  runtime?: RuntimeOverview;
  dlqDepth?: DlqDepthResponse;
  batchOps?: BatchOperation[];
  cron?: CronJobRow[];
  /** The newest failed jobs, at most `OVERVIEW_FAILED_SAMPLE`. */
  failedJobs?: JobRow[];
  /** The newest running jobs, at most `OVERVIEW_RUNNING_SAMPLE`. The jobs
   * API lists newest first only, so older running jobs go unchecked when
   * there are more. */
  runningJobs?: JobRow[];
}

const SEVERITY_ORDER: Record<AttentionSeverity, number> = { action: 0, warning: 1, notice: 2 };

function queueName(queues: QueueOverview[] | undefined, queue: string): string {
  const name = queues?.find((q) => q.queue === queue)?.display_name?.trim();
  return name ? name : queue;
}

function kindName(jobs: JobRow[], kind: string): string {
  const name = jobs.find((job) => job.kind === kind)?.kind_descriptor?.display_name?.trim();
  return name ? name : kind;
}

function instanceLabel(instance: Pick<RuntimeInstance, "hostname" | "pid">): string {
  return instance.hostname ?? `pid ${instance.pid}`;
}

/** Whether a running job has passed its deadline or stopped heartbeating. */
export function isStuck(job: Pick<JobRow, "state" | "deadline_at" | "heartbeat_at">, now: number): boolean {
  if (job.state !== "running") return false;
  if (job.deadline_at && new Date(job.deadline_at).getTime() < now) return true;
  return job.heartbeat_at !== null && now - new Date(job.heartbeat_at).getTime() > STUCK_HEARTBEAT_MS;
}

/** The distinct versions that live instances report, newest-looking last. */
export function liveVersions(runtime: RuntimeOverview): string[] {
  return [...new Set(runtime.instances.filter((i) => !i.stale).map((i) => i.version))].sort();
}

/** Ranked problems, most urgent first. Inputs that failed to load are skipped. */
export function attentionItems(input: AttentionInput): AttentionItem[] {
  const { now, queues, queueRuntime, runtime, dlqDepth, batchOps, cron, failedJobs, runningJobs } = input;
  const items: (AttentionItem & { order: number; weight: number })[] = [];
  const push = (order: number, weight: number, item: AttentionItem) => items.push({ ...item, order, weight });

  if (runtime) {
    const ready = queues?.reduce((sum, q) => sum + (q.paused ? 0 : q.available), 0) ?? 0;
    if (runtime.live_instances === 0) {
      push(0, ready, {
        id: "fleet:none-live",
        severity: ready > 0 ? "action" : "warning",
        title: "No live workers",
        detail: ready > 0 ? `${plural(ready, "ready job")} waiting and nothing is claiming them` : "Nothing is claiming jobs",
        target: { to: "/runtime" },
      });
    } else {
      for (const instance of runtime.instances.filter((i) => !i.stale && !i.healthy)) {
        push(1, 1, {
          id: `fleet:degraded:${instance.instance_id}`,
          severity: "action",
          title: `Instance ${instanceLabel(instance)} is unhealthy`,
          detail: [
            !instance.postgres_connected && "no database connection",
            !instance.poll_loop_alive && "poll loop down",
            !instance.heartbeat_alive && "heartbeat loop down",
            !instance.maintenance_alive && instance.leader && "maintenance loop down",
            instance.shutting_down && "shutting down",
          ]
            .filter(Boolean)
            .join(", ") || undefined,
          target: { to: "/runtime/$instanceId", params: { instanceId: instance.instance_id } },
        });
      }
      if (runtime.leader_instances !== 1) {
        push(6, 1, {
          id: "fleet:leader",
          severity: "warning",
          title: runtime.leader_instances === 0 ? "No maintenance leader" : `${runtime.leader_instances} maintenance leaders`,
          detail: "Expected exactly one; scheduled promotion, rescue and cron depend on it",
          target: { to: "/runtime" },
        });
      }
      const versions = liveVersions(runtime);
      if (versions.length > 1) {
        push(12, versions.length, {
          id: "fleet:versions",
          severity: "notice",
          title: `Live workers run ${versions.length} versions`,
          detail: versions.join(", "),
          target: { to: "/runtime" },
        });
      }
    }
  }

  if (runningJobs) {
    const stuck = runningJobs.filter((job) => isStuck(job, now));
    if (stuck.length > 0) {
      push(2, stuck.length, {
        id: "jobs:stuck",
        severity: "action",
        title: `${plural(stuck.length, "running job")} past deadline or without a recent heartbeat`,
        detail:
          runningJobs.length >= OVERVIEW_RUNNING_SAMPLE
            ? `Only the ${formatNumber(runningJobs.length)} newest running jobs were checked; older ones may be stuck too`
            : `Of ${plural(runningJobs.length, "running job")}`,
        target: { to: "/jobs", search: { state: "running" } },
      });
    }
  }

  if (failedJobs) {
    const sampled = failedJobs.length >= OVERVIEW_FAILED_SAMPLE;
    for (const group of groupFailures(failedJobs).slice(0, ATTENTION_FAILURE_GROUPS)) {
      push(3, group.count, {
        id: `failures:${group.kind}:${group.key}`,
        severity: "action",
        title:
          `${formatNumber(group.count)}× ${group.key} in ${kindName(failedJobs, group.kind)}` +
          (sampled ? ` among the newest ${formatNumber(failedJobs.length)} failed jobs` : ""),
        detail: group.summary,
        target: { to: "/jobs", search: { state: "failed", q: `kind:${group.kind}` } },
      });
    }
  }

  if (batchOps) {
    for (const op of batchOps) {
      if (op.state === "failed" && now - new Date(op.finalized_at ?? op.updated_at).getTime() < BATCH_FAILURE_WINDOW_MS) {
        push(4, 1, {
          id: `batch:failed:${op.id}`,
          severity: "action",
          title: `Bulk ${op.op_kind.replace(/_/g, " ")} failed`,
          detail: op.last_error ?? undefined,
          target: { to: "/batch-ops/$id", params: { id: op.id } },
        });
      } else if (ACTIVE_BATCH_STATES.has(op.state)) {
        push(13, 1, {
          id: `batch:active:${op.id}`,
          severity: "notice",
          title: `Bulk ${op.op_kind.replace(/_/g, " ")} ${op.state}`,
          detail: op.total_matched !== null ? `${formatNumber(op.processed + op.skipped + op.errored)} of ${formatNumber(op.total_matched)} done` : undefined,
          target: { to: "/batch-ops/$id", params: { id: op.id } },
        });
      }
    }
  }

  if (queues) {
    for (const q of queues) {
      if (q.paused && q.available > 0) {
        push(5, q.available, {
          id: `queue:paused:${q.queue}`,
          severity: "warning",
          title: `${queueName(queues, q.queue)} is paused with ${plural(q.available, "ready job")}`,
          detail: [
            (q.paused_at || q.paused_by) && pausedLabel(q, now),
            q.lag_seconds !== null && `Oldest has waited ${formatDuration(q.lag_seconds)}`,
          ].filter(Boolean).join(" · ") || undefined,
          target: { to: "/queues/$name", params: { name: q.queue } },
        });
      } else if (!q.paused && q.lag_seconds !== null && q.lag_seconds >= LAG_ATTENTION_SECONDS) {
        push(7, q.lag_seconds, {
          id: `queue:lag:${q.queue}`,
          severity: "warning",
          title: `${queueName(queues, q.queue)}: oldest ready job has waited ${formatDuration(q.lag_seconds)}`,
          detail: `${plural(q.available, "ready job")}`,
          target: { to: "/queues/$name/jobs", params: { name: q.queue }, search: { state: "available" } },
        });
      }
      if (q.descriptor_mismatch) {
        push(11, 1, {
          id: `queue:drift:${q.queue}`,
          severity: "notice",
          title: `${queueName(queues, q.queue)}: workers declare different descriptors`,
          target: { to: "/queues/$name", params: { name: q.queue } },
        });
      }
    }
  }

  if (dlqDepth) {
    for (const row of dlqDepth.by_queue) {
      if (row.count <= 0) continue;
      push(8, row.count, {
        id: `dlq:${row.queue}`,
        severity: "warning",
        title: `${plural(row.count, "dead letter")} in ${queueName(queues, row.queue)}`,
        target: { to: "/queues/$name/dlq", params: { name: row.queue } },
      });
    }
  }

  if (queueRuntime) {
    const mismatched = queueRuntime.filter((q) => q.config_mismatch);
    if (mismatched.length === 1) {
      const q = mismatched[0]!;
      push(9, 1, {
        id: `queue:config:${q.queue}`,
        severity: "warning",
        title: `${queueName(queues, q.queue)}: workers run different configs`,
        target: { to: "/queues/$name/config", params: { name: q.queue } },
      });
    } else if (mismatched.length > 1) {
      push(9, mismatched.length, {
        id: "queue:config",
        severity: "warning",
        title: `Workers run different configs for ${mismatched.length} queues`,
        detail: mismatched.map((q) => queueName(queues, q.queue)).join(", "),
        target: { to: "/runtime" },
      });
    }
  }

  if (cron) {
    for (const schedule of cron) {
      if (schedule.retired_at) continue;
      if (isOverdue(schedule, now)) {
        push(10, 1, {
          id: `cron:overdue:${schedule.name}`,
          severity: "warning",
          title: `Schedule ${schedule.name} is overdue`,
          detail: "Its next fire has passed without an enqueue; check the maintenance leader",
          target: { to: "/cron/$name", params: { name: schedule.name } },
        });
      } else if (schedule.paused_at) {
        push(14, 1, {
          id: `cron:paused:${schedule.name}`,
          severity: "notice",
          title: `Schedule ${schedule.name} is paused`,
          detail: schedule.paused_by ? `By ${schedule.paused_by}` : undefined,
          target: { to: "/cron/$name", params: { name: schedule.name } },
        });
      }
    }
  }

  return items
    .sort(
      (a, b) =>
        SEVERITY_ORDER[a.severity] - SEVERITY_ORDER[b.severity] || a.order - b.order || b.weight - a.weight
    )
    .map(({ order: _order, weight: _weight, ...item }) => item);
}

/** One figure of the health strip and the line under it; null where unknown. */
export interface HealthFigures {
  /** Ready jobs in unpaused queues. */
  ready: number | null;
  /** The longest wait among unpaused queues' ready jobs. */
  oldestReady: { seconds: number; queue: string } | null;
  /** Ready jobs held in paused queues, and how many such queues. */
  pausedReady: { jobs: number; queues: number } | null;
  /** The queue with the most retryable jobs. */
  topRetrying: { queue: string; count: number } | null;
  /** Jobs completed in the last hour across queues. */
  completedLastHour: number | null;
  /** The queue with the most dead letters. */
  topDeadLetters: { queue: string; count: number } | null;
  /** Stuck jobs among the running sample, and the sample's size. */
  stuck: { count: number; sampled: number } | null;
  /** Bulk operations still changing jobs. */
  activeBatchOps: number | null;
  /** The newest failed bulk operation within the failure window. */
  latestFailedBatchOp: BatchOperation | null;
}

export function healthFigures(input: AttentionInput): HealthFigures {
  const { now, queues, dlqDepth, batchOps, runningJobs } = input;
  let oldestReady: HealthFigures["oldestReady"] = null;
  let topRetrying: HealthFigures["topRetrying"] = null;
  for (const q of queues ?? []) {
    if (!q.paused && q.available > 0 && q.lag_seconds !== null && (!oldestReady || q.lag_seconds > oldestReady.seconds)) {
      oldestReady = { seconds: q.lag_seconds, queue: q.queue };
    }
    if (q.retryable > 0 && (!topRetrying || q.retryable > topRetrying.count)) {
      topRetrying = { queue: q.queue, count: q.retryable };
    }
  }
  const pausedQueues = (queues ?? []).filter((q) => q.paused && q.available > 0);
  const topDlq = [...(dlqDepth?.by_queue ?? [])].sort((a, b) => b.count - a.count)[0];
  const failedOps = (batchOps ?? [])
    .filter((op) => op.state === "failed" && now - new Date(op.finalized_at ?? op.updated_at).getTime() < BATCH_FAILURE_WINDOW_MS)
    .sort((a, b) => (b.finalized_at ?? b.updated_at).localeCompare(a.finalized_at ?? a.updated_at));
  return {
    ready: queues ? queues.reduce((sum, q) => sum + (q.paused ? 0 : q.available), 0) : null,
    oldestReady,
    pausedReady: queues
      ? { jobs: pausedQueues.reduce((sum, q) => sum + q.available, 0), queues: pausedQueues.length }
      : null,
    topRetrying,
    completedLastHour: queues ? queues.reduce((sum, q) => sum + q.completed_last_hour, 0) : null,
    topDeadLetters: topDlq && topDlq.count > 0 ? topDlq : null,
    stuck: runningJobs ? { count: runningJobs.filter((job) => isStuck(job, now)).length, sampled: runningJobs.length } : null,
    activeBatchOps: batchOps ? batchOps.filter((op) => ACTIVE_BATCH_STATES.has(op.state)).length : null,
    latestFailedBatchOp: failedOps[0] ?? null,
  };
}

/**
 * Failed jobs still in the jobs tables. The server's `failed` counts include
 * dead letters, so a figure shown beside a dead-letter count subtracts them.
 */
export function liveFailedCount(failed: number, deadLetters: number | null | undefined): number {
  return Math.max(0, failed - (deadLetters ?? 0));
}

/**
 * Queues with something to look at, most pressing first. `liveFailed` is the
 * queue's failed jobs less its dead letters, or all its failed jobs when the
 * dead-letter depth is unknown.
 */
export function queuesNeedingAttention(
  queues: QueueOverview[],
  dlqDepth: DlqDepthResponse | undefined,
  limit: number
): (QueueOverview & { deadLetters: number | null; liveFailed: number })[] {
  const dlq = new Map((dlqDepth?.by_queue ?? []).map((row) => [row.queue, row.count]));
  const score = (q: QueueOverview) =>
    liveFailedCount(q.failed, dlq.get(q.queue)) * 3 +
    (dlq.get(q.queue) ?? 0) * 2 +
    q.retryable +
    (q.paused && q.available > 0 ? q.available : 0) +
    (!q.paused && (q.lag_seconds ?? 0) >= LAG_ATTENTION_SECONDS ? q.available : 0);
  return queues
    .map((q) => ({
      ...q,
      deadLetters: dlqDepth ? dlq.get(q.queue) ?? 0 : null,
      liveFailed: liveFailedCount(q.failed, dlq.get(q.queue)),
      score: score(q),
    }))
    .filter((q) => q.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, limit)
    .map(({ score: _score, ...q }) => q);
}
