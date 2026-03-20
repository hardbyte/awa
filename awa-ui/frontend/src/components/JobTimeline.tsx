/**
 * Vertical timeline showing a job's lifecycle progression.
 * Reconstructs history from timestamps and the errors array.
 */

import type { JobRow } from "@/lib/api";
import { StateBadge } from "./StateBadge";

interface TimelineEvent {
  timestamp: string;
  label: string;
  state?: string;
  detail?: string;
  isError?: boolean;
}

function buildTimeline(job: JobRow): TimelineEvent[] {
  const events: TimelineEvent[] = [];

  // Created
  events.push({
    timestamp: job.created_at,
    label: "Created",
    state: job.run_at !== job.created_at ? "scheduled" : "available",
  });

  // Errors (each attempt that failed)
  if (job.errors && job.errors.length > 0) {
    for (const err of job.errors) {
      if (err && typeof err === "object" && err !== null) {
        const e = err as Record<string, unknown>;
        const at = typeof e.at === "string" ? e.at : undefined;
        const error = typeof e.error === "string" ? e.error : undefined;
        const attempt = typeof e.attempt === "number" ? e.attempt : undefined;
        if (at) {
          events.push({
            timestamp: at,
            label: `Attempt ${attempt ?? "?"}`,
            state: "retryable",
            detail: error,
            isError: true,
          });
        }
      }
    }
  }

  // Last attempt (if running or completed without errors)
  if (job.attempted_at && job.state === "running") {
    events.push({
      timestamp: job.attempted_at,
      label: `Running (attempt ${job.attempt})`,
      state: "running",
    });
  }

  // Waiting external
  if (job.state === "waiting_external" && job.attempted_at) {
    events.push({
      timestamp: job.attempted_at,
      label: "Waiting for callback",
      state: "waiting_external",
    });
  }

  // Finalized
  if (job.finalized_at) {
    events.push({
      timestamp: job.finalized_at,
      label:
        job.state === "completed"
          ? "Completed"
          : job.state === "failed"
            ? "Failed (exhausted attempts)"
            : job.state === "cancelled"
              ? "Cancelled"
              : "Finalized",
      state: job.state,
    });
  }

  // Sort by timestamp
  events.sort(
    (a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
  );

  return events;
}

const STATE_DOT_COLOR: Record<string, string> = {
  available: "bg-info",
  scheduled: "bg-secondary-fg/30",
  running: "bg-success",
  completed: "bg-success",
  retryable: "bg-warning",
  failed: "bg-danger",
  cancelled: "bg-muted-fg/40",
  waiting_external: "bg-[oklch(0.45_0.15_280)]",
};

export function JobTimeline({ job }: { job: JobRow }) {
  const events = buildTimeline(job);

  if (events.length === 0) return null;

  return (
    <div className="relative space-y-0">
      {events.map((event, i) => {
        const isLast = i === events.length - 1;
        const dotColor = event.state
          ? STATE_DOT_COLOR[event.state] ?? "bg-muted-fg/30"
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
              <div className="flex items-baseline gap-2">
                <span className="text-sm font-medium">{event.label}</span>
                {event.state && <StateBadge state={event.state} />}
                <time className="ml-auto text-xs text-muted-fg tabular-nums">
                  {new Date(event.timestamp).toLocaleString()}
                </time>
              </div>
              {event.detail && (
                <p className="mt-1 text-sm text-danger">{event.detail}</p>
              )}
            </div>
          </div>
        );
      })}
    </div>
  );
}
