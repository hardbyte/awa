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

