/**
 * Vertical timeline showing a job's lifecycle.
 *
 * Important: this reconstructs history from available timestamps and the
 * errors[] array. AWA does not store a full state-transition log, so the
 * timeline shows what we *know* (timestamps that exist) rather than
 * guessing intermediate states.
 */

import type { JobRow } from "@/lib/api";
import { StateBadge } from "./StateBadge";
import { Time } from "./Time";
import { jobStateMeta, TONE_DOT } from "@/lib/states";
import { errorSummary, parseErrorEntry } from "@/lib/errors";

interface TimelineEvent {
  timestamp: string;
  label: string;
  state?: string;
  detail?: string;
  isError?: boolean;
}

function buildTimeline(job: JobRow): TimelineEvent[] {
  const events: TimelineEvent[] = [];

  // Created — this is always known
  events.push({
    timestamp: job.created_at,
    label: "Created",
  });

  // Consecutive identical errors share one line; the Errors card has each one.
  let run: { first: number | null; last: number | null; summary: string; event: TimelineEvent } | null = null;
  for (const entry of job.errors ?? []) {
    const error = parseErrorEntry(entry);
    if (!error.at) continue;
    const summary = errorSummary(error);
    const continues =
      run !== null &&
      run.summary === summary &&
      error.attempt !== null &&
      run.last !== null &&
      error.attempt === run.last + 1;
    if (continues && run) {
      run.last = error.attempt;
      run.event.label = `Attempts ${run.first}–${run.last} failed`;
      continue;
    }
    const event: TimelineEvent = {
      timestamp: error.at,
      label: error.attempt !== null ? `Attempt ${error.attempt} failed` : "Error",
      state: "failed",
      detail: summary || undefined,
      isError: true,
    };
    events.push(event);
    run = { first: error.attempt, last: error.attempt, summary, event };
  }

  // Attempt started — shown whenever attempted_at exists
  if (job.attempted_at) {
    const isTerminal = ["completed", "failed", "cancelled"].includes(job.state);
    events.push({
      timestamp: job.attempted_at,
      label: job.state === "waiting_external"
        ? `Attempt ${job.attempt} — waiting for callback`
        : isTerminal
          ? `Attempt ${job.attempt} started`
          : `Attempt ${job.attempt} running`,
      state: isTerminal ? "running" : job.state,
    });
  }

  // Finalized — only shown if we have finalized_at
  if (job.finalized_at) {
    const label =
      job.state === "completed"
        ? `Completed (attempt ${job.attempt})`
        : job.state === "failed"
          ? `Failed after ${job.attempt} attempt${job.attempt !== 1 ? "s" : ""}`
          : job.state === "cancelled"
            ? "Cancelled"
            : "Finalized";
    events.push({
      timestamp: job.finalized_at,
      label,
      state: job.state,
    });
  }

  // Sort chronologically
  events.sort(
    (a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
  );

  return events;
}


export function JobTimeline({ job }: { job: JobRow }) {
  const events = buildTimeline(job);

  if (events.length === 0) return null;

  return (
    <div className="space-y-0">
      {events.map((event, i) => {
        const isLast = i === events.length - 1;
        const dotColor = event.state
          ? TONE_DOT[jobStateMeta(event.state).tone]
          : "bg-muted-fg/30";

        return (
          <div key={`${event.timestamp}-${i}`} className="relative flex gap-4 pb-6 last:pb-0">
            {/* Vertical line */}
            {!isLast && (
              <div className="absolute left-[7px] top-4 bottom-0 w-px bg-border" />
            )}

            {/* Dot */}
            <div className="relative flex-none pt-1">
              <div
                className={`size-[15px] rounded-full border-2 border-bg ${dotColor} ${
                  event.isError ? "ring-2 ring-danger/20" : ""
                }`}
              />
            </div>

            {/* Content */}
            <div className="min-w-0 flex-1">
              <div className="flex flex-wrap items-baseline gap-2">
                <span className="text-sm font-medium">{event.label}</span>
                {event.state && <StateBadge state={event.state} />}
                <Time
                  value={event.timestamp}
                  format="absolute"
                  className="ml-auto text-xs tabular-nums text-muted-fg"
                />
              </div>
              {event.detail && (
                <p className="mt-1 truncate text-sm text-danger-subtle-fg" title={event.detail}>
                  {event.detail}
                </p>
              )}
            </div>
          </div>
        );
      })}

      {/* Transparency note */}
      <p className="pt-2 text-xs text-muted-fg/60">
        Timeline reconstructed from job timestamps and error history.
      </p>
    </div>
  );
}
