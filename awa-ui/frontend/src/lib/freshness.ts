import type { Freshness } from "@/lib/observation/scheduler";
import { formatDuration } from "@/lib/time";

/** live: current; stale: some reads failing; down: nothing refreshes; paused: a view stopped itself or
 * waits for a refresh after the server refused its request. */
export type FreshnessTone = "live" | "stale" | "down" | "paused";

export interface FreshnessStatus {
  tone: FreshnessTone;
  /** A word or two, for narrow screens. */
  short: string;
  /** The whole sentence. */
  label: string;
}

function age(ms: number): string {
  return ms < 1000 ? "just now" : `${formatDuration(ms / 1000)} ago`;
}

/** What the header's freshness pill says about the views on screen. */
export function freshnessStatus(f: Freshness): FreshnessStatus {
  if (f.halted) {
    return { tone: "down", short: "Refused", label: "Access refused — refresh to try again" };
  }
  if (f.views > 0 && f.failed === f.views) {
    return { tone: "down", short: "Unreachable", label: "Server unreachable" };
  }
  if (f.failed > 0) {
    return {
      tone: "stale",
      short: "Reconnecting",
      label: f.ageMs !== null ? `Last updated ${age(f.ageMs)} — reconnecting` : "Some views failed to load — reconnecting",
    };
  }
  if (f.rejected > 0) {
    return { tone: "stale", short: "Too many views", label: "Too many live views on this page; some don't refresh" };
  }
  if (f.blocked > 0) {
    return { tone: "paused", short: "Stopped", label: "A view's request was refused — refresh to retry" };
  }
  if (f.paused) {
    return { tone: "paused", short: "Paused", label: "Paused while rows are selected" };
  }
  if (f.views > 0 && f.ageMs === null) {
    return { tone: "live", short: "Loading", label: "Loading…" };
  }
  return { tone: "live", short: "Live", label: f.ageMs !== null ? `Live · updated ${age(f.ageMs)}` : "Live" };
}
