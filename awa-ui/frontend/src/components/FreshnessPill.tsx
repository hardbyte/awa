import { useEffect, useState } from "react";
import { getFreshness, subscribeFreshness } from "@/lib/observation/coordinator";
import { freshnessStatus, type FreshnessStatus, type FreshnessTone } from "@/lib/freshness";

const TICK_MS = 1_000;

const TONE: Record<FreshnessTone, { dot: string; text: string }> = {
  live: { dot: "bg-success", text: "text-muted-fg" },
  paused: { dot: "bg-muted-fg", text: "text-muted-fg" },
  stale: { dot: "bg-warning", text: "text-warning-subtle-fg" },
  down: { dot: "bg-danger", text: "text-danger-subtle-fg" },
};

/** The freshness of the views on screen, re-read every second so the age moves. */
export function useFreshness(): FreshnessStatus {
  const [status, setStatus] = useState(() => freshnessStatus(getFreshness()));
  useEffect(() => {
    const update = () => setStatus(freshnessStatus(getFreshness()));
    const unsubscribe = subscribeFreshness(update);
    const timer = setInterval(update, TICK_MS);
    return () => {
      unsubscribe();
      clearInterval(timer);
    };
  }, []);
  return status;
}

/** "Live · updated 3s ago", "Last updated 2m ago — reconnecting" or "Server unreachable". */
export function FreshnessPill() {
  const status = useFreshness();
  const tone = TONE[status.tone];
  return (
    <span
      role="status"
      aria-label={status.label}
      title={status.label}
      data-testid="freshness"
      data-tone={status.tone}
      className={`inline-flex min-w-0 items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs/none font-medium tabular-nums ${tone.text}`}
    >
      <span className={`size-2 shrink-0 rounded-full ${tone.dot}`} aria-hidden="true" />
      <span className="hidden truncate sm:inline" aria-hidden="true">{status.label}</span>
      <span className="sm:hidden" aria-hidden="true">{status.short}</span>
    </span>
  );
}

/** Dims its children while every read is failing, since they show old data. */
export function DimWhenUnreachable({ children }: { children: React.ReactNode }) {
  const down = useFreshness().tone === "down";
  return (
    <div className={down ? "opacity-60 transition-opacity" : "transition-opacity"} data-stale={down ? "" : undefined}>
      {children}
    </div>
  );
}
