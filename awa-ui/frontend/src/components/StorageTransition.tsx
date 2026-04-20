import type { StorageStatusReport } from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader } from "@/components/ui/card";

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
            <span>
              <Badge intent={stateBadgeIntent(report.state)}>{report.state}</Badge>
            </span>
          </div>
          <div className="flex flex-col gap-0.5">
            <span className="text-xs uppercase tracking-wide text-muted-fg">
              Canonical backlog
            </span>
            <span className="font-medium tabular-nums">
              {report.canonical_live_backlog.toLocaleString()}
            </span>
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
