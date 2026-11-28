import { useEffect, useRef, useState } from "react";
import { useNavigate } from "@tanstack/react-router";
import {
  toWireFilter,
  type BatchOperationKind,
  type JobCount,
  type JobFilterParams,
} from "@/lib/api";
import { bulkActionReason, bulkVerb } from "@/lib/batch";
import { AdminButton } from "@/components/AdminAction";
import { BulkActionDialog } from "@/components/BulkActionDialog";

const JOB_ACTIONS: BatchOperationKind[] = ["retry", "cancel", "discard", "move_to_dlq"];
const DLQ_ACTIONS: BatchOperationKind[] = ["retry_from_dlq", "purge_dlq"];
const DANGER = new Set<BatchOperationKind>(["cancel", "discard", "purge_dlq"]);

export interface BulkScopeBarProps {
  target: "jobs" | "dlq";
  /** The list's filter, without paging. */
  filter: JobFilterParams;
  /** What the filter matches, as the list counted it. */
  count?: JobCount;
  /** Selected rows and the actions for them, which act on those rows only. */
  selection?: { count: number; actions: React.ReactNode };
  /** The new-operation form's fields for this filter, for actions that take
   * a setting; a string says why the filter can't be carried there. */
  formPrefill?: Record<string, string> | string;
  /** Why no action can run yet, e.g. while the list is loading. */
  unavailableReason?: string;
}

function countLabel(count: JobCount | undefined): string {
  if (!count) return "All matching";
  const value = count.value.toLocaleString();
  return `All ${count.kind === "at_least" ? `${value}+` : value} matching`;
}

/**
 * Bulk actions for a list: on the selected rows, or as a bulk operation on
 * everything the list's filter matches. Each action says why when it can't
 * apply to this filter.
 */
export function BulkScopeBar({ target, filter, count, selection, formPrefill, unavailableReason }: BulkScopeBarProps) {
  const navigate = useNavigate();
  const selectedCount = selection?.count ?? 0;
  const [scope, setScope] = useState<"selected" | "all">(selectedCount > 0 ? "selected" : "all");
  const previousSelected = useRef(selectedCount);
  useEffect(() => {
    if (previousSelected.current === 0 && selectedCount > 0) setScope("selected");
    if (selectedCount === 0) setScope("all");
    previousSelected.current = selectedCount;
  }, [selectedCount]);

  const [action, setAction] = useState<BatchOperationKind | null>(null);
  const [lastAction, setLastAction] = useState<BatchOperationKind>(target === "dlq" ? "retry_from_dlq" : "retry");
  const wire = toWireFilter(filter);
  const context = {
    states: filter.states,
    filter: wire,
    count,
  };
  const actions = target === "dlq" ? DLQ_ACTIONS : JOB_ACTIONS;
  const formReason =
    unavailableReason ?? (typeof formPrefill === "string" ? formPrefill : bulkActionReason("set_priority", context));

  return (
    <div
      role="toolbar"
      aria-label="Bulk actions"
      className="flex flex-wrap items-center gap-2 rounded-lg border bg-muted/40 px-3 py-2"
      data-testid="bulk-scope-bar"
    >
      <span className="text-sm text-muted-fg">Act on</span>
      {selectedCount > 0 ? (
        <div className="inline-flex rounded-md border bg-bg p-0.5" role="group" aria-label="Scope">
          {(["selected", "all"] as const).map((option) => (
            <button
              key={option}
              type="button"
              aria-pressed={scope === option}
              onClick={() => setScope(option)}
              className={[
                "rounded px-2 py-0.5 text-sm",
                scope === option ? "bg-primary text-primary-fg" : "hover:bg-secondary",
              ].join(" ")}
            >
              {option === "selected" ? `Selected (${selectedCount.toLocaleString()})` : countLabel(count)}
            </button>
          ))}
        </div>
      ) : (
        <span className="text-sm font-medium" data-testid="bulk-scope-all">
          {countLabel(count)}
        </span>
      )}
      <span className="mx-1 h-5 w-px bg-border" aria-hidden="true" />
      {scope === "selected" && selection ? (
        selection.actions
      ) : (
        <>
          {actions.map((kind) => {
            const reason = unavailableReason ?? bulkActionReason(kind, context);
            return (
              <AdminButton
                key={kind}
                size="sm"
                intent="outline"
                className={DANGER.has(kind) ? "text-danger" : undefined}
                isDisabled={reason !== null}
                disabledReason={reason ?? undefined}
                onPress={() => {
                  setLastAction(kind);
                  setAction(kind);
                }}
              >
                {bulkVerb(kind)}…
              </AdminButton>
            );
          })}
          {target === "jobs" &&
            (["set_priority", "move_queue"] as const).map((kind) => (
              <AdminButton
                key={kind}
                size="sm"
                intent="outline"
                isDisabled={formReason !== null}
                disabledReason={formReason ?? undefined}
                onPress={() =>
                  void navigate({
                    to: "/batch-ops/new",
                    search: { ...(typeof formPrefill === "object" ? formPrefill : {}), op: kind },
                  })
                }
              >
                {bulkVerb(kind)}…
              </AdminButton>
            ))}
        </>
      )}
      <BulkActionDialog
        isOpen={action !== null}
        onOpenChange={(open) => !open && setAction(null)}
        payload={{ op_kind: lastAction, filter: wire, spec: {}, submitted_by: "ui" }}
      />
    </div>
  );
}
