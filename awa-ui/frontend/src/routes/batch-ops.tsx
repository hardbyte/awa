import { useManagedQuery } from "@/hooks/use-managed-query";
import { Time } from "@/components/Time";
import { useMemo, useState } from "react";
import { useSearch } from "@tanstack/react-router";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import {
  cancelBatchOperation,
  fetchBatchOperations,
  previewBatchOperation,
  submitBatchOperation,
  toWireFilter,
  type BatchOperation,
  type BatchOperationFilter,
  type BatchOperationKind,
  type BatchOperationPayload,
  type BatchOperationPreview,
} from "@/lib/api";
import { isJobId, toRfc3339 } from "@/components/SearchBar";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { ListState } from "@/components/ListState";
import { describeFilter, formatCount, isNarrowing, previewCount, sameFilter } from "@/lib/batch";
import { useReadOnly } from "@/hooks/use-read-only";
import { toast } from "@/components/ui/toast";
import { Heading } from "@/components/ui/heading";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableHeader,
  TableBody,
  TableRow,
  TableCell,
  TableColumn,
} from "@/components/ui/table";
import { formatNumber } from "@/lib/number";

/** A completed operation that matched rows but changed none of them. */
function changedNothing(operation: BatchOperation): boolean {
  return operation.state === "completed" && operation.processed === 0 && operation.skipped + operation.errored > 0;
}

function stateTone(operation: BatchOperation): "success" | "warning" | "danger" | "secondary" {
  const state = operation.state;
  if (state === "completed") return changedNothing(operation) ? "warning" : "success";
  if (state === "failed") return "danger";
  if (state === "cancelled" || state === "cancelling") return "warning";
  return "secondary";
}

function progressText(operation: BatchOperation): string {
  const total = operation.total_matched ?? 0;
  const accounted = operation.processed + operation.skipped + operation.errored;
  if (total === 0) return formatNumber(accounted);
  return `${formatNumber(accounted)}/${formatNumber(total)}`;
}

function parsePriority(value: string): number | null {
  const trimmed = value.trim();
  if (!trimmed) return null;
  const priority = Number(trimmed);
  if (!Number.isInteger(priority) || priority < 1 || priority > 4) return null;
  return priority;
}

interface KindOption {
  kind: BatchOperationKind;
  label: string;
  /** The job states the kind acts on. */
  states: string[];
  /** The jobs a filter without a state selects, in words. */
  scope: string;
}

/** Every operation kind and the jobs it acts on (ADR-030). */
const KIND_OPTIONS: KindOption[] = [
  { kind: "set_priority", label: "Set priority", states: ["available", "scheduled"], scope: "available or scheduled" },
  { kind: "move_queue", label: "Move queue", states: ["available", "scheduled"], scope: "available or scheduled" },
  { kind: "retry", label: "Retry", states: ["failed", "cancelled"], scope: "failed or cancelled" },
  {
    kind: "cancel",
    label: "Cancel",
    states: ["available", "scheduled", "retryable", "running", "waiting_external"],
    scope: "available, scheduled, retryable, running or waiting",
  },
  { kind: "discard", label: "Discard failed", states: ["failed"], scope: "failed" },
  { kind: "move_to_dlq", label: "Move failed to DLQ", states: ["failed"], scope: "failed" },
  { kind: "retry_from_dlq", label: "Retry from DLQ", states: [], scope: "dead-lettered" },
  { kind: "purge_dlq", label: "Purge DLQ", states: [], scope: "dead-lettered" },
];

function kindOption(kind: BatchOperationKind): KindOption {
  const option = KIND_OPTIONS.find((candidate) => candidate.kind === kind);
  if (!option) throw new Error(`unknown batch operation kind: ${kind}`);
  return option;
}

function isPayloadFormValid(form: BatchForm): boolean {
  const priority = parsePriority(form.priority);
  if (form.opKind === "set_priority") return priority !== null;
  if (form.opKind === "move_queue") {
    if (!form.destinationQueue.trim()) return false;
    return !form.priority.trim() || priority !== null;
  }
  return true;
}

interface BatchForm {
  opKind: BatchOperationKind;
  queue: string;
  kind: string;
  ids: string;
  /** Comma-separated; every tag must match. */
  tags: string;
  /** "" for every state the operation kind acts on. */
  state: string;
  createdGte: string;
  createdLt: string;
  priority: string;
  destinationQueue: string;
  /** move_to_dlq's reason; "" for the server's default. */
  reason: string;
}

function splitList(value: string): string[] {
  return value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

/**
 * Why the form's filter can't be sent: a field that doesn't parse would
 * otherwise be left out, widening the jobs the operation changes.
 */
function filterError(form: Pick<BatchForm, "ids" | "createdGte" | "createdLt">): string | null {
  if (!splitList(form.ids).every(isJobId)) return "IDs must be whole job ids";
  if (form.createdGte.trim() && !toRfc3339(form.createdGte.trim())) return "Created from is not a date";
  if (form.createdLt.trim() && !toRfc3339(form.createdLt.trim())) return "Created before is not a date";
  return null;
}

/** The shared job filter (ADR-049) the form describes. */
function buildFilter(form: BatchForm): BatchOperationFilter {
  return toWireFilter({
    queue: form.queue.trim() || undefined,
    kind: form.kind.trim() || undefined,
    ids: splitList(form.ids).map(Number),
    tags: splitList(form.tags),
    states: form.state ? [form.state] : [],
    created_at_gte: toRfc3339(form.createdGte.trim()),
    created_at_lt: toRfc3339(form.createdLt.trim()),
  });
}

function buildPayload(form: BatchForm): BatchOperationPayload {
  const filter = buildFilter(form);
  const priority = parsePriority(form.priority);
  switch (form.opKind) {
    case "set_priority":
      return {
        op_kind: "set_priority",
        filter,
        spec: { priority: priority ?? 1 },
        submitted_by: "ui",
      };
    case "move_queue":
      return {
        op_kind: "move_queue",
        filter,
        spec: {
          queue: form.destinationQueue.trim(),
          ...(priority !== null ? { priority } : {}),
        },
        submitted_by: "ui",
      };
    case "move_to_dlq": {
      const reason = form.reason.trim();
      return {
        op_kind: "move_to_dlq",
        filter,
        spec: reason ? { reason } : {},
        submitted_by: "ui",
      };
    }
    default:
      return { op_kind: form.opKind, filter, spec: {}, submitted_by: "ui" };
  }
}

/** What submitting does, in words, for the confirm step. */
function describeOperation(payload: BatchOperationPayload, preview: BatchOperationPreview): string {
  const jobs = `${formatCount(previewCount(preview))} ${describeFilter(
    payload.filter,
    kindOption(payload.op_kind).scope
  )}`;
  const priority = payload.spec.priority as number | undefined;
  switch (payload.op_kind) {
    case "set_priority":
      return `Set priority to ${priority} on ${jobs}.`;
    case "move_queue": {
      const atPriority = priority === undefined ? "" : ` at priority ${priority}`;
      return `Move ${jobs} to queue ${String(payload.spec.queue)}${atPriority}.`;
    }
    case "retry":
    case "retry_from_dlq":
      return `Retry ${jobs}.`;
    case "cancel":
      return `Cancel ${jobs}.`;
    case "discard":
      return `Delete ${jobs}.`;
    case "move_to_dlq":
      return `Move ${jobs} to the DLQ.`;
    case "purge_dlq":
      return `Delete ${jobs} from the DLQ.`;
  }
}

/** Form fields pre-filled from the jobs page's "batch change" link. The
 * router parses query values as JSON, so `?queue=123` arrives as a number. */
function initialForm(search: Record<string, unknown>): BatchForm {
  const text = (value: unknown) => (value === undefined || value === null ? "" : String(value));
  const state = search.state === "available" || search.state === "scheduled" ? search.state : "";
  return {
    opKind: "set_priority",
    queue: text(search.queue),
    kind: text(search.kind),
    ids: text(search.ids),
    tags: text(search.tags),
    state,
    createdGte: text(search.created_at_gte),
    createdLt: text(search.created_at_lt),
    priority: "1",
    destinationQueue: "",
    reason: "",
  };
}

export function BatchOpsPage() {
  const queryClient = useQueryClient();
  const readOnly = useReadOnly();
  const search = useSearch({ strict: false }) as Record<string, unknown>;
  const [form, setForm] = useState<BatchForm>(() => initialForm(search));
  // A preview holds the operation kind and filter it was taken with: it
  // stands for the form only while both are the same.
  const [preview, setPreview] = useState<{
    opKind: BatchOperationKind;
    filter: BatchOperationFilter;
    result: BatchOperationPreview;
  } | null>(null);
  const [confirming, setConfirming] = useState(false);
  const formFilterError = useMemo(() => filterError(form), [form]);
  const selectedKind = kindOption(form.opKind);
  const payload = useMemo(() => buildPayload(form), [form]);
  const narrowing = isNarrowing(payload.filter);
  const isPayloadValid = isPayloadFormValid(form) && formFilterError === null && narrowing;
  const currentPreview =
    preview && preview.opKind === payload.op_kind && sameFilter(preview.filter, payload.filter)
      ? preview.result
      : null;
  const currentCount = currentPreview ? previewCount(currentPreview) : null;
  const submitHint = !isPayloadValid || currentPreview
    ? null
    : preview
      ? "Operation or filter changed; preview again"
      : "Preview first";

  const operationsQuery = useManagedQuery({
    queryKey: ["batch-ops"],
    queryFn: () => fetchBatchOperations({ limit: 50 }),
  });

  const previewMutation = useMutation({
    mutationFn: (previewed: BatchOperationPayload) => {
      if (!isPayloadValid) throw new Error("Invalid batch operation payload");
      return previewBatchOperation(previewed);
    },
    onSuccess: (result, previewed) => {
      setPreview({ opKind: previewed.op_kind, filter: previewed.filter, result });
      toast.success(`Preview matched ${formatCount(previewCount(result))} job(s)`);
    },
    onError: (error) => toast.error(error.message),
  });

  const submitMutation = useMutation({
    mutationFn: () => {
      if (!isPayloadValid || !currentPreview) throw new Error("Invalid batch operation payload");
      return submitBatchOperation(payload);
    },
    onSuccess: (operation) => {
      setPreview(null);
      void queryClient.invalidateQueries({ queryKey: ["batch-ops"] });
      toast.success(`Submitted batch operation ${operation.id.slice(0, 8)}`);
    },
    onError: (error) => toast.error(error.message),
  });

  const cancelMutation = useMutation({
    mutationFn: (id: string) => cancelBatchOperation(id),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["batch-ops"] });
      toast.success("Cancellation requested");
    },
    onError: (error) => toast.error(error.message),
  });

  const operations = operationsQuery.data ?? [];

  return (
    <div className="space-y-6">
      <div>
        <Heading level={1}>Batch Operations</Heading>
        <p className="mt-2 text-sm text-muted-fg">
          Preview, submit, and monitor durable bulk mutations, applied a chunk at a time.
        </p>
      </div>

      <section className="rounded-xl border bg-bg p-4 shadow-sm">
        <Heading level={2} className="text-base">New Operation</Heading>
        <div className="mt-4 grid gap-3 md:grid-cols-3">
          <label className="space-y-1 text-sm">
            <span className="font-medium">Operation</span>
            <select
              className="w-full rounded-lg border border-input bg-bg px-3 py-2 text-sm"
              value={form.opKind}
              onChange={(event) => {
                const opKind = event.target.value as BatchOperationKind;
                setForm((current) => ({
                  ...current,
                  opKind,
                  state: kindOption(opKind).states.includes(current.state) ? current.state : "",
                }));
              }}
            >
              {KIND_OPTIONS.map((option) => (
                <option key={option.kind} value={option.kind}>{option.label}</option>
              ))}
            </select>
          </label>
          <label className="space-y-1 text-sm">
            <span className="font-medium">Queue filter</span>
            <Input value={form.queue} onChange={(event) => setForm((current) => ({ ...current, queue: event.target.value }))} placeholder="default" />
          </label>
          <label className="space-y-1 text-sm">
            <span className="font-medium">Kind filter</span>
            <Input value={form.kind} onChange={(event) => setForm((current) => ({ ...current, kind: event.target.value }))} placeholder="send_email" />
          </label>
          <label className="space-y-1 text-sm">
            <span className="font-medium">IDs filter</span>
            <Input value={form.ids} onChange={(event) => setForm((current) => ({ ...current, ids: event.target.value }))} placeholder="123, 456" />
          </label>
          <label className="space-y-1 text-sm">
            <span className="font-medium">Tags filter</span>
            <Input value={form.tags} onChange={(event) => setForm((current) => ({ ...current, tags: event.target.value }))} placeholder="order:123, tenant:acme" />
          </label>
          <label className="space-y-1 text-sm">
            <span className="font-medium">State filter</span>
            <select
              className="w-full rounded-lg border border-input bg-bg px-3 py-2 text-sm"
              value={form.state}
              onChange={(event) => setForm((current) => ({ ...current, state: event.target.value }))}
            >
              <option value="">{selectedKind.scope.charAt(0).toUpperCase() + selectedKind.scope.slice(1)}</option>
              {selectedKind.states.length > 1 &&
                selectedKind.states.map((state) => (
                  <option key={state} value={state}>{state.charAt(0).toUpperCase() + state.slice(1).replace(/_/g, " ")}</option>
                ))}
            </select>
          </label>
          <label className="space-y-1 text-sm">
            <span className="font-medium">Created from</span>
            <Input value={form.createdGte} onChange={(event) => setForm((current) => ({ ...current, createdGte: event.target.value }))} placeholder="2026-10-01" />
          </label>
          <label className="space-y-1 text-sm">
            <span className="font-medium">Created before</span>
            <Input value={form.createdLt} onChange={(event) => setForm((current) => ({ ...current, createdLt: event.target.value }))} placeholder="2026-10-02" />
          </label>
          {(form.opKind === "set_priority" || form.opKind === "move_queue") && (
            <label className="space-y-1 text-sm">
              <span className="font-medium">Priority</span>
              <Input value={form.priority} onChange={(event) => setForm((current) => ({ ...current, priority: event.target.value }))} placeholder="1" />
            </label>
          )}
          {form.opKind === "move_to_dlq" && (
            <label className="space-y-1 text-sm">
              <span className="font-medium">DLQ reason</span>
              <Input value={form.reason} onChange={(event) => setForm((current) => ({ ...current, reason: event.target.value }))} placeholder="batch operation" />
            </label>
          )}
          {form.opKind === "move_queue" && (
            <label className="space-y-1 text-sm md:col-span-3">
              <span className="font-medium">Destination queue</span>
              <Input value={form.destinationQueue} onChange={(event) => setForm((current) => ({ ...current, destinationQueue: event.target.value }))} placeholder="escalations" />
            </label>
          )}
        </div>
        <p className="mt-3 text-xs text-muted-fg">
          {selectedKind.label} acts only on {selectedKind.scope} jobs; other jobs
          in the same filter are left alone.
        </p>
        {formFilterError && (
          <p className="mt-2 text-sm text-danger" role="alert">
            {formFilterError}
          </p>
        )}
        {!formFilterError && !narrowing && (
          <p className="mt-2 text-sm text-muted-fg" data-testid="batch-narrowing">
            Add a queue, kind, tag, ID or created bound.
          </p>
        )}
        {currentCount && (
          <p className="mt-2 text-sm" data-testid="batch-preview-count">
            {currentCount.kind === "at_least" ? "At least " : ""}
            {formatNumber(currentCount.value)} eligible job(s) would change.
          </p>
        )}
        <div className="mt-4 flex items-center gap-2">
          <Button onPress={() => previewMutation.mutate(payload)} isDisabled={readOnly || previewMutation.isPending || !isPayloadValid}>Preview</Button>
          <Button intent="primary" onPress={() => setConfirming(true)} isDisabled={readOnly || submitMutation.isPending || !isPayloadValid || !currentPreview}>Submit</Button>
          {submitHint && (
            <span className="text-xs text-muted-fg" data-testid="batch-submit-hint">{submitHint}</span>
          )}
        </div>
      </section>

      <ConfirmDialog
        isOpen={confirming && currentPreview !== null}
        onOpenChange={setConfirming}
        title="Submit batch operation"
        description={currentPreview ? describeOperation(payload, currentPreview) : ""}
        confirmLabel="Submit"
        onConfirm={() => submitMutation.mutate()}
        isPending={submitMutation.isPending}
      />

      <Table aria-label="Batch operations">
        <TableHeader>
          <TableColumn>ID</TableColumn>
          <TableColumn>Kind</TableColumn>
          <TableColumn>State</TableColumn>
          <TableColumn>Progress</TableColumn>
          <TableColumn>Processed</TableColumn>
          <TableColumn>Skipped</TableColumn>
          <TableColumn>Errors</TableColumn>
          <TableColumn>Submitted</TableColumn>
          <TableColumn>Action</TableColumn>
        </TableHeader>
        <TableBody
          renderEmptyState={() => (
            <ListState
              isLoading={operationsQuery.isLoading}
              error={operationsQuery.isError ? operationsQuery.error : null}
              onRetry={() => void operationsQuery.refetch()}
              noun="batch operations"
              empty="No batch operations yet."
            />
          )}
        >
          {operations.map((operation) => (
            <TableRow key={operation.id} id={operation.id}>
              <TableCell className="font-mono text-xs">{operation.id.slice(0, 8)}</TableCell>
              <TableCell>{operation.op_kind.replace(/_/g, " ")}</TableCell>
              <TableCell>
                <Badge intent={stateTone(operation)}>{operation.state}</Badge>
                {changedNothing(operation) && (
                  <div className="mt-1 text-xs text-muted-fg" data-testid="batch-op-changed-nothing">
                    No jobs changed
                  </div>
                )}
                {operation.last_error && (
                  <div className="mt-1 max-w-xs whitespace-normal break-words text-xs text-danger-subtle-fg" data-testid="batch-op-last-error">
                    {operation.last_error}
                  </div>
                )}
              </TableCell>
              <TableCell className="tabular-nums">{progressText(operation)}</TableCell>
              <TableCell className="tabular-nums">{formatNumber(operation.processed)}</TableCell>
              <TableCell className="tabular-nums">{formatNumber(operation.skipped)}</TableCell>
              <TableCell className="tabular-nums">{formatNumber(operation.errored)}</TableCell>
              <TableCell><Time value={operation.submitted_at} /></TableCell>
              <TableCell>
                {operation.state === "pending" || operation.state === "scanning" || operation.state === "running" ? (
                  <Button size="sm" intent="outline" onPress={() => cancelMutation.mutate(operation.id)} isDisabled={readOnly || cancelMutation.isPending}>Cancel</Button>
                ) : null}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}
