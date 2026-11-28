import { formatDuration } from "@/lib/time";

export interface SummaryFact {
  key: string;
  text: string;
  /** The timestamp the fact is measured from or to, for a tooltip. */
  at?: string;
  /** Draws attention: a passed deadline, an expired callback. */
  warning?: boolean;
}

export interface SummaryJob {
  state: string;
  attempt: number;
  max_attempts: number;
  run_at: string;
  heartbeat_at: string | null;
  deadline_at: string | null;
  attempted_at: string | null;
  finalized_at: string | null;
  created_at: string;
  callback_timeout_at?: string | null;
  progress?: unknown;
  /** Set for a dead-lettered row. */
  dlq_at?: string | null;
}

function ms(value: string | null | undefined): number | null {
  if (!value) return null;
  const parsed = new Date(value).getTime();
  return Number.isFinite(parsed) ? parsed : null;
}

function span(fromMs: number, toMs: number): string {
  return formatDuration(Math.abs(toMs - fromMs) / 1000);
}

/** "in 3m" or "3m ago". */
function relative(atMs: number, nowMs: number): string {
  return atMs >= nowMs ? `in ${span(nowMs, atMs)}` : `${span(atMs, nowMs)} ago`;
}

function attempts(job: SummaryJob): string {
  return `${job.attempt} attempt${job.attempt === 1 ? "" : "s"}`;
}

function percent(progress: unknown): number | null {
  if (!progress || typeof progress !== "object") return null;
  const value = (progress as Record<string, unknown>).percent;
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

/**
 * The facts that say where a job stands, measured against `nowMs`: how long
 * it has run or waited, what comes next and when. Which runtime instance
 * holds an attempt isn't recorded, so it isn't shown.
 */
export function jobSummary(job: SummaryJob, nowMs: number): SummaryFact[] {
  const facts: SummaryFact[] = [];
  const attemptedMs = ms(job.attempted_at);
  const finalizedMs = ms(job.finalized_at);
  const runAtMs = ms(job.run_at);
  const createdMs = ms(job.created_at);

  switch (job.state) {
    case "running": {
      if (attemptedMs !== null) facts.push({ key: "running", text: `Running for ${span(attemptedMs, nowMs)}`, at: job.attempted_at! });
      const heartbeatMs = ms(job.heartbeat_at);
      facts.push(
        heartbeatMs !== null
          ? { key: "heartbeat", text: `heartbeat ${relative(heartbeatMs, nowMs)}`, at: job.heartbeat_at! }
          : { key: "heartbeat", text: "no heartbeat yet (short-job claim)" }
      );
      const deadlineMs = ms(job.deadline_at);
      if (deadlineMs !== null) {
        facts.push(
          deadlineMs >= nowMs
            ? { key: "deadline", text: `deadline in ${span(nowMs, deadlineMs)}`, at: job.deadline_at! }
            : { key: "deadline", text: `deadline passed ${span(deadlineMs, nowMs)} ago`, at: job.deadline_at!, warning: true }
        );
      }
      facts.push({ key: "attempt", text: `attempt ${job.attempt} of ${job.max_attempts}` });
      const done = percent(job.progress);
      if (done !== null) facts.push({ key: "progress", text: `${Math.round(done)}% done` });
      break;
    }
    case "retryable":
    case "scheduled": {
      if (runAtMs !== null) {
        const label = job.state === "retryable" ? "Next attempt" : "Runs";
        facts.push(
          runAtMs > nowMs
            ? { key: "next", text: `${label} in ${span(nowMs, runAtMs)}`, at: job.run_at }
            : { key: "next", text: `${label} due ${span(runAtMs, nowMs)} ago`, at: job.run_at }
        );
      }
      if (job.state === "retryable") facts.push({ key: "attempt", text: `after attempt ${job.attempt} of ${job.max_attempts}` });
      break;
    }
    case "waiting_external": {
      if (attemptedMs !== null) facts.push({ key: "waiting", text: `Waiting for a callback for ${span(attemptedMs, nowMs)}`, at: job.attempted_at! });
      const timeoutMs = ms(job.callback_timeout_at);
      if (timeoutMs !== null) {
        facts.push(
          timeoutMs >= nowMs
            ? { key: "timeout", text: `callback times out in ${span(nowMs, timeoutMs)}`, at: job.callback_timeout_at! }
            : { key: "timeout", text: `callback timed out ${span(timeoutMs, nowMs)} ago`, at: job.callback_timeout_at!, warning: true }
        );
      }
      break;
    }
    case "available": {
      if (runAtMs !== null) facts.push({ key: "ready", text: `Ready for ${span(runAtMs, nowMs)}`, at: job.run_at });
      if (job.attempt > 0) facts.push({ key: "attempt", text: `${attempts(job)} so far` });
      break;
    }
    case "completed": {
      if (attemptedMs !== null && finalizedMs !== null) facts.push({ key: "took", text: `Took ${span(attemptedMs, finalizedMs)}` });
      if (createdMs !== null && attemptedMs !== null) facts.push({ key: "waited", text: `waited ${span(createdMs, attemptedMs)} to start` });
      if (finalizedMs !== null) facts.push({ key: "finished", text: `finished ${relative(finalizedMs, nowMs)}`, at: job.finalized_at! });
      if (job.attempt > 1) facts.push({ key: "attempt", text: `on attempt ${job.attempt}` });
      break;
    }
    case "failed":
    case "cancelled": {
      const verb = job.state === "failed" ? "Failed" : "Cancelled";
      const after = job.attempt > 0 ? ` after ${attempts(job)}` : "";
      facts.push(
        finalizedMs !== null
          ? { key: "final", text: `${verb} ${relative(finalizedMs, nowMs)}${after}`, at: job.finalized_at! }
          : { key: "final", text: `${verb}${after}` }
      );
      break;
    }
    case "dead_lettered": {
      const dlqMs = ms(job.dlq_at);
      facts.push(
        dlqMs !== null
          ? { key: "final", text: `Dead-lettered ${relative(dlqMs, nowMs)} after ${attempts(job)}`, at: job.dlq_at! }
          : { key: "final", text: `Dead-lettered after ${attempts(job)}` }
      );
      break;
    }
  }
  return facts;
}
