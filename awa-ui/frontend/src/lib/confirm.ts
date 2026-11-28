import type { BatchOperationKind, JobCount } from "@/lib/api";
import { TYPE_TO_CONFIRM_MIN_JOBS } from "@/lib/constants";

/**
 * How much confirmation an action takes:
 * - 0: reversible, one object; no dialog, an Undo toast.
 * - 1: side effects on one job; a dialog naming it and the risk.
 * - 2: many jobs; the dialog shows the count and the jobs.
 * - 3: many jobs and irreversible, or very many; the user types to confirm.
 */
export type ConfirmTier = 1 | 2 | 3;

export const READ_ONLY_REASON = "Read-only server — admin actions are off";

/** Tier for acting on rows already selected on the page. */
export function selectionTier(count: number): ConfirmTier {
  return count <= 1 ? 1 : 2;
}


/** Kinds that take tier 3 from `TYPE_TO_CONFIRM_MIN_JOBS` jobs: a cancel stops
 * work, and a retry runs handlers again, repeating their side effects. */
const TIER_3_AT_SCALE: ReadonlySet<BatchOperationKind> = new Set(["cancel", "retry", "retry_from_dlq"]);

/** Tier for a bulk operation over a filter, given what its preview matched. */
export function bulkTier(opKind: BatchOperationKind, count: JobCount | undefined): ConfirmTier {
  if (opKind === "discard" || opKind === "purge_dlq") return 3;
  if (
    TIER_3_AT_SCALE.has(opKind) &&
    (!count || count.kind === "at_least" || count.value >= TYPE_TO_CONFIRM_MIN_JOBS)
  ) {
    return 3;
  }
  return 2;
}
