import { useEffect, useMemo, useRef, useState } from "react";
import {
  ExclamationTriangleIcon,
  LockClosedIcon,
  ArrowUturnLeftIcon,
  CheckCircleIcon,
} from "@heroicons/react/20/solid";
import type { StorageStatusReport } from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Sparkline } from "@/components/Sparkline";
import { durationSince } from "@/lib/time";

/**
 * Maximum samples retained per `transition_epoch`. Long transitions (multi-hour
 * drains) at the default ~5s poll interval easily exceed a few hundred
 * samples; cap so the sparkline stays performant and the in-memory state
 * doesn't grow unbounded if the tab is left open across a multi-day window.
 */
const MAX_HISTORY_SAMPLES = 720;

function engineLabel(engine: string | null | undefined): string {
  if (!engine) return "—";
  return engine.replace(/_/g, " ");
}

function stateBadgeIntent(state: string): "secondary" | "warning" | "success" | "danger" {
  switch (state) {
    case "prepared":
      return "warning";
    case "mixed_transition":
      return "warning";
    case "active":
      return "success";
    case "aborted":
      return "danger";
    default:
      return "secondary";
  }
}

/**
 * Rollback boundary classification. The exact post-#180 boundaries
 * (mirroring `docs/upgrade-0.5-to-0.6.md` § Rollback boundaries):
 *
 *  - `reversible-via-abort` — `awa storage abort` will return the cluster
 *    to canonical. Covers `canonical`, `prepared`, and `aborted` states.
 *  - `restore-only` — the cluster has accepted queue-storage work and
 *    abort is no longer safe. We don't have a precise "any queue-storage
 *    row exists" signal exposed via the report yet, so any
 *    `mixed_transition` state is treated as restore-only. The doc itself
 *    describes mixed-transition as the one-way door, and the
 *    conservative classification is the operator-safe default.
 *  - `finalized` — `state=active` on `queue_storage`; transition is
 *    complete and the canonical engine has been retired.
 */
export type RollbackBoundary = "reversible-via-abort" | "restore-only" | "finalized";

export function rollbackBoundary(report: StorageStatusReport): RollbackBoundary {
  if (report.state === "active") return "finalized";
  if (report.state === "mixed_transition") return "restore-only";
  // canonical | prepared | aborted | anything else falls back here.
  return "reversible-via-abort";
}

/**
 * Reduce poll responses into a stable, deduped, per-epoch backlog series.
 *
 * Anchored to `transition_epoch`: when the operator aborts or finalizes,
 * the epoch ticks and the series resets. We dedupe consecutive identical
 * (timestamp, value) pairs because the dashboard cache TTL means the same
 * sample is sometimes served to two polls in a row.
 */
export function appendBacklogSample(
  prev: ReadonlyArray<{ at: number; value: number }>,
  prevEpoch: number,
  report: StorageStatusReport,
  nowMs: number = Date.now()
): { samples: { at: number; value: number }[]; epoch: number } {
  // Epoch change resets the series — this is a different cutover, the
  // sparkline must not concatenate values across epochs.
  const epochChanged = report.transition_epoch !== prevEpoch;
  const base = epochChanged ? [] : prev;
  const next = base.slice();
  const last = next[next.length - 1];
  // Drop consecutive identical samples — the cache replays them.
  if (!last || last.value !== report.canonical_live_backlog) {
    next.push({ at: nowMs, value: report.canonical_live_backlog });
  }
  // Cap retained samples — sparkline rendering and memory bounded.
  if (next.length > MAX_HISTORY_SAMPLES) {
    next.splice(0, next.length - MAX_HISTORY_SAMPLES);
  }
  return { samples: next, epoch: report.transition_epoch };
}

interface StorageTransitionCardProps {
  report: StorageStatusReport;
}

// Shows storage transition readiness when an engine is prepared, a mixed
// cutover is in flight, or a finalized migration is on a non-canonical
// engine. On a canonical-only cluster this card does not render; the caller
// should gate on `shouldShowStorageTransitionCard(report)`.
export function StorageTransitionCard({ report }: StorageTransitionCardProps) {
  const hasPrepared = report.prepared_engine !== null;
  const inMixedTransition = report.state === "mixed_transition";
  const liveCounts = report.live_runtime_capability_counts;
  const totalLive = Object.values(liveCounts).reduce((sum, n) => sum + n, 0);

  const phase = hasPrepared
    ? inMixedTransition
      ? "Finalize readiness"
      : "Enter-mixed-transition readiness"
    : "Storage state";

  const blockers = inMixedTransition
    ? report.finalize_blockers
    : report.enter_mixed_transition_blockers;
  const canProceed = inMixedTransition
    ? report.can_finalize
    : report.can_enter_mixed_transition;

  // -- Time-in-state stamp: refresh at 1Hz so the operator watches the
  // clock advance during a long drain. `entered_at` is what the SQL
  // function returns when state last changed.
  const [nowMs, setNowMs] = useState(() => Date.now());
  useEffect(() => {
    const interval = setInterval(() => setNowMs(Date.now()), 1000);
    return () => clearInterval(interval);
  }, []);
  const timeInState = durationSince(report.entered_at, nowMs);

  // -- Epoch-anchored canonical-live-backlog sparkline. We accumulate
  // client-side (sample-source decision A per #180: no audit table, no
  // backend ring buffer). Anchored to `transition_epoch` — the series
  // resets when the operator aborts or finalizes.
  const [history, setHistory] = useState<{
    samples: { at: number; value: number }[];
    epoch: number;
  }>(() => ({ samples: [], epoch: report.transition_epoch }));
  // Track which (epoch, updated_at) we've already recorded so each
  // distinct poll contributes at most one sample even when the
  // component re-renders (1Hz ticker above).
  const lastRecordedRef = useRef<{ epoch: number; updatedAt: string } | null>(null);
  useEffect(() => {
    const key = { epoch: report.transition_epoch, updatedAt: report.updated_at };
    const last = lastRecordedRef.current;
    if (last && last.epoch === key.epoch && last.updatedAt === key.updatedAt) {
      return;
    }
    lastRecordedRef.current = key;
    setHistory((prev) =>
      appendBacklogSample(prev.samples, prev.epoch, report, Date.now())
    );
  }, [report]);

  const sparklineValues = useMemo(
    () => history.samples.map((s) => s.value),
    [history.samples]
  );
  const showSparkline = inMixedTransition && sparklineValues.length >= 2;

  const boundary = rollbackBoundary(report);

  return (
    <Card>
      <CardHeader
        title="Storage transition"
        description="Readiness gates for rolling forward a prepared storage engine"
      />
      <CardContent className="space-y-4">
        <div className="grid grid-cols-2 gap-x-6 gap-y-2 text-sm sm:grid-cols-4">
          <div className="flex flex-col gap-0.5">
            <span className="text-xs uppercase tracking-wide text-muted-fg">
              Active engine
            </span>
            <span className="font-medium capitalize">
              {engineLabel(report.active_engine)}
            </span>
          </div>
          <div className="flex flex-col gap-0.5">
            <span className="text-xs uppercase tracking-wide text-muted-fg">
              Prepared engine
            </span>
            <span className="font-medium capitalize">
              {engineLabel(report.prepared_engine)}
            </span>
          </div>
          <div className="flex flex-col gap-0.5">
            <span className="text-xs uppercase tracking-wide text-muted-fg">
              State
            </span>
            <span className="flex flex-wrap items-baseline gap-1.5">
              <Badge intent={stateBadgeIntent(report.state)}>{report.state}</Badge>
              <span
                className="text-xs text-muted-fg tabular-nums"
                title={`Entered ${new Date(report.entered_at).toLocaleString()}`}
              >
                for {timeInState}
              </span>
            </span>
          </div>
          <div className="flex flex-col gap-0.5">
            <span className="text-xs uppercase tracking-wide text-muted-fg">
              Canonical backlog
            </span>
            <div className="flex items-center gap-2">
              <span className="font-medium tabular-nums">
                {report.canonical_live_backlog.toLocaleString()}
              </span>
              {showSparkline && (
                <Sparkline
                  data={sparklineValues}
                  width={72}
                  height={20}
                  label={`canonical backlog over the last ${sparklineValues.length} samples in this transition epoch`}
                  className="text-fg/70"
                />
              )}
            </div>
            {inMixedTransition && (
              <span className="text-[10px] uppercase tracking-wide text-muted-fg">
                {sparklineValues.length < 2
                  ? "collecting samples…"
                  : `epoch ${report.transition_epoch} · ${sparklineValues.length} samples`}
              </span>
            )}
          </div>
        </div>

        {hasPrepared && (
          <div className="rounded-lg border p-3 text-sm">
            <div className="mb-2 flex flex-wrap items-baseline justify-between gap-2">
              <span className="font-medium">{phase}</span>
              <Badge intent={canProceed ? "success" : "warning"}>
                {canProceed ? "ready" : `${blockers.length} blocker${blockers.length === 1 ? "" : "s"}`}
              </Badge>
            </div>
            {blockers.length > 0 ? (
              <ul className="list-disc space-y-1 pl-5 text-muted-fg">
                {blockers.map((reason) => (
                  <li key={reason}>{reason}</li>
                ))}
              </ul>
            ) : (
              <p className="text-muted-fg">
                All gates satisfied. Operator can proceed when ready.
              </p>
            )}
          </div>
        )}

        {/* Prepared-schema-readiness footgun warning. The JSON field has been
            here since 0.5.5-alpha; surfacing it visually closes the issue
            called out in docs/upgrade-0.5-to-0.6.md step 2. */}
        {report.prepared_queue_storage_schema && !report.prepared_schema_ready && (
          <div
            role="alert"
            className="flex items-start gap-2 rounded-lg border border-warning bg-warning/10 p-3 text-sm text-warning-fg"
          >
            <ExclamationTriangleIcon className="mt-0.5 size-4 shrink-0 text-warning" aria-hidden />
            <div className="space-y-1">
              <p className="font-medium">
                Prepared queue-storage schema is not installed
              </p>
              <p className="text-muted-fg">
                Schema{" "}
                <code className="rounded bg-muted px-1 py-0.5">
                  {report.prepared_queue_storage_schema}
                </code>{" "}
                is missing required tables. The first queue-storage write
                would pay the install cost — and a runtime upgrade may
                stall at <code>enter-mixed-transition</code>. Run{" "}
                <code className="rounded bg-muted px-1 py-0.5">
                  awa storage prepare-queue-storage-schema --schema{" "}
                  {report.prepared_queue_storage_schema}
                </code>{" "}
                before the routing flip.
              </p>
            </div>
          </div>
        )}

        {/* Rollback boundaries panel. Tells the operator — in plain words —
            whether `awa storage abort` is still a way back, or whether the
            cluster has crossed into restore-only territory. */}
        <RollbackBoundariesPanel boundary={boundary} report={report} />

        <div className="flex flex-col gap-2 text-sm sm:flex-row sm:items-center sm:justify-between">
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-xs uppercase tracking-wide text-muted-fg">
              Live runtimes
            </span>
            {totalLive === 0 ? (
              <span className="text-muted-fg">none</span>
            ) : (
              Object.entries(liveCounts)
                .sort(([a], [b]) => a.localeCompare(b))
                .map(([capability, count]) => (
                  <Badge key={capability} intent="secondary">
                    <span className="capitalize">{capability.replace(/_/g, " ")}</span>
                    <span className="ml-1 tabular-nums text-muted-fg">×{count}</span>
                  </Badge>
                ))
            )}
          </div>
          {report.prepared_queue_storage_schema && (
            <div className="text-xs text-muted-fg">
              Prepared schema{" "}
              <code className="rounded bg-muted px-1 py-0.5">
                {report.prepared_queue_storage_schema}
              </code>{" "}
              {report.prepared_schema_ready ? "ready" : "missing required tables"}
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  );
}

interface RollbackBoundariesPanelProps {
  boundary: RollbackBoundary;
  report: StorageStatusReport;
}

function RollbackBoundariesPanel({ boundary, report }: RollbackBoundariesPanelProps) {
  // Pre-compute the copy for each branch so the operator doesn't have to
  // play "guess the intent" — explicit words, no cute icons-only states.
  if (boundary === "finalized") {
    return (
      <div className="flex items-start gap-2 rounded-lg border p-3 text-sm">
        <CheckCircleIcon className="mt-0.5 size-4 shrink-0 text-success" aria-hidden />
        <div className="space-y-0.5">
          <p className="font-medium">Finalized · queue-storage is the live engine</p>
          <p className="text-muted-fg">
            The transition is complete. <code>awa storage abort</code> is
            not available from this state — use a database restore if you
            need to roll all the way back.
          </p>
        </div>
      </div>
    );
  }

  if (boundary === "restore-only") {
    return (
      <div
        role="alert"
        className="flex items-start gap-2 rounded-lg border border-danger bg-danger/10 p-3 text-sm text-danger-fg"
      >
        <LockClosedIcon className="mt-0.5 size-4 shrink-0 text-danger" aria-hidden />
        <div className="space-y-1">
          <p className="font-medium">From here it&apos;s restore-only</p>
          <p className="text-muted-fg">
            The cluster is in <code>mixed_transition</code>. Once any
            queue-storage row has been accepted, <code>awa storage abort</code>{" "}
            is rejected and the only path back to canonical is a database
            restore. A pure fleet downgrade to 0.5 is{" "}
            <strong>not supported</strong> — 0.5 workers can&apos;t claim
            queue-storage work. Finish forward with{" "}
            <code>awa storage finalize</code> when{" "}
            <span className="tabular-nums">
              {report.canonical_live_backlog.toLocaleString()}
            </span>{" "}
            canonical-backlog jobs have drained, or restore from backup.
          </p>
        </div>
      </div>
    );
  }

  // reversible-via-abort
  return (
    <div className="flex items-start gap-2 rounded-lg border border-muted bg-muted/40 p-3 text-sm">
      <ArrowUturnLeftIcon className="mt-0.5 size-4 shrink-0 text-fg/70" aria-hidden />
      <div className="space-y-0.5">
        <p className="font-medium">Rollback is still available</p>
        <p className="text-muted-fg">
          Cluster state is <code>{report.state}</code>. Running{" "}
          <code>awa storage abort</code> returns the cluster to canonical
          and clears the prepared engine metadata. This stays true until
          the routing flip (<code>enter-mixed-transition</code>).
        </p>
      </div>
    </div>
  );
}

export function shouldShowStorageTransitionCard(
  report: StorageStatusReport | null | undefined
): boolean {
  if (!report) return false;
  if (report.prepared_engine !== null) return true;
  // Also surface when the cluster is past canonical — e.g. finalized onto
  // queue_storage — so operators see the engine name and live fleet.
  if (report.state !== "canonical" && report.state !== "active") return true;
  if (report.active_engine !== "canonical") return true;
  return false;
}
