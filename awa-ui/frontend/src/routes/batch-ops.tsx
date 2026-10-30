import { useEffect, useMemo, useState } from "react";
import { useSearch } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
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
import { toRfc3339 } from "@/components/SearchBar";
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
import { timeAgo } from "@/lib/time";

function stateTone(state: string): "success" | "warning" | "danger" | "secondary" {
  if (state === "completed") return "success";
  if (state === "failed") return "danger";
  if (state === "cancelled" || state === "cancelling") return "warning";
  return "secondary";
}

function progressText(operation: BatchOperation): string {
  const total = operation.total_matched ?? 0;
  const accounted = operation.processed + operation.skipped + operation.errored;
  if (total === 0) return `${accounted}`;
  return `${accounted}/${total}`;
}

function parsePriority(value: string): number | null {
  const trimmed = value.trim();
  if (!trimmed) return null;
  const priority = Number(trimmed);
  if (!Number.isInteger(priority) || priority < 1 || priority > 4) return null;
  return priority;
}

function isPayloadFormValid(form: BatchForm): boolean {
  const priority = parsePriority(form.priority);
  if (form.opKind === "set_priority") return priority !== null;
  if (!form.destinationQueue.trim()) return false;
  return !form.priority.trim() || priority !== null;
}

interface BatchForm {
  opKind: BatchOperationKind;
  queue: string;
  kind: string;
  ids: string;
  /** Comma-separated; every tag must match. */
  tags: string;
  /** "" for both eligible states. */
  state: "" | "available" | "scheduled";
  createdGte: string;
  createdLt: string;
  priority: string;
  destinationQueue: string;
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
  if (splitList(form.ids).some((id) => !/^\d+$/.test(id))) return "IDs must be whole numbers";
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
  if (form.opKind === "set_priority") {
    const priority = parsePriority(form.priority);
    return {
      op_kind: "set_priority",
      filter,
      spec: { priority: priority ?? 1 },
      submitted_by: "ui",
    };
  }
  const priority = parsePriority(form.priority);
  return {
    op_kind: "move_queue",
    filter,
    spec: {
      queue: form.destinationQueue.trim(),
      ...(priority !== null ? { priority } : {}),
    },
    submitted_by: "ui",
  };
}

/** Form fields pre-filled from the jobs page's "batch change" link. */
function initialForm(search: Record<string, string | undefined>): BatchForm {
  const state = search.state === "available" || search.state === "scheduled" ? search.state : "";
  return {
    opKind: "set_priority",
    queue: search.queue ?? "",
    kind: search.kind ?? "",
    ids: search.ids ?? "",
    tags: search.tags ?? "",
    state,
    createdGte: search.created_at_gte ?? "",
    createdLt: search.created_at_lt ?? "",
    priority: "1",
    destinationQueue: "",
  };
}

export function BatchOpsPage() {
  const queryClient = useQueryClient();
  const readOnly = useReadOnly();
  const search = useSearch({ strict: false }) as Record<string, string | undefined>;
  const [form, setForm] = useState<BatchForm>(() => initialForm(search));
  const [preview, setPreview] = useState<BatchOperationPreview | null>(null);
  const formFilterError = useMemo(() => filterError(form), [form]);
  const isPayloadValid = useMemo(
    () => isPayloadFormValid(form) && formFilterError === null,
    [form, formFilterError]
  );
  const payload = useMemo(() => buildPayload(form), [form]);
  // A preview describes the form it was taken from.
  useEffect(() => setPreview(null), [payload]);

  const operationsQuery = useQuery({
    queryKey: ["batch-ops"],
    queryFn: () => fetchBatchOperations({ limit: 50 }),
    refetchInterval: 2_000,
  });

  const previewMutation = useMutation({
    mutationFn: () => {
      if (!isPayloadValid) throw new Error("Invalid batch operation payload");
      return previewBatchOperation(payload);
    },
    onSuccess: (result) => {
      setPreview(result);
      toast.success(`Preview matched ${result.total_matched} job(s)`);
    },
    onError: (error) => toast.error(error.message),
  });

  const submitMutation = useMutation({
    mutationFn: () => {
      if (!isPayloadValid) throw new Error("Invalid batch operation payload");
      return submitBatchOperation(payload);
    },
    onSuccess: (operation) => {
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
          Preview, submit, and monitor durable bulk mutations for queued jobs.
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
              onChange={(event) => setForm((current) => ({ ...current, opKind: event.target.value as BatchOperationKind }))}
            >
              <option value="set_priority">Set priority</option>
              <option value="move_queue">Move queue</option>
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
              onChange={(event) => setForm((current) => ({ ...current, state: event.target.value as BatchForm["state"] }))}
            >
              <option value="">Available or scheduled</option>
              <option value="available">Available</option>
              <option value="scheduled">Scheduled</option>
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
          <label className="space-y-1 text-sm">
            <span className="font-medium">Priority</span>
            <Input value={form.priority} onChange={(event) => setForm((current) => ({ ...current, priority: event.target.value }))} placeholder="1" />
          </label>
          {form.opKind === "move_queue" && (
            <label className="space-y-1 text-sm md:col-span-3">
              <span className="font-medium">Destination queue</span>
              <Input value={form.destinationQueue} onChange={(event) => setForm((current) => ({ ...current, destinationQueue: event.target.value }))} placeholder="escalations" />
            </label>
          )}
        </div>
        <p className="mt-3 text-xs text-muted-fg">
          Batch operations act only on available and scheduled jobs; running,
          waiting and finished jobs in the same filter are left alone.
        </p>
        {formFilterError && (
          <p className="mt-2 text-sm text-danger" role="alert">
            {formFilterError}
          </p>
        )}
        {preview && (
          <p className="mt-2 text-sm" data-testid="batch-preview-count">
            {preview.total_matched.toLocaleString()} eligible job(s) would change.
          </p>
        )}
        <div className="mt-4 flex gap-2">
          <Button onPress={() => previewMutation.mutate()} isDisabled={readOnly || previewMutation.isPending || !isPayloadValid}>Preview</Button>
          <Button intent="primary" onPress={() => submitMutation.mutate()} isDisabled={readOnly || submitMutation.isPending || !isPayloadValid}>Submit</Button>
        </div>
      </section>

      <Table aria-label="Batch operations">
        <TableHeader>
          <TableColumn>ID</TableColumn>
          <TableColumn>Kind</TableColumn>
          <TableColumn>State</TableColumn>
          <TableColumn>Progress</TableColumn>
          <TableColumn>Errors</TableColumn>
          <TableColumn>Submitted</TableColumn>
          <TableColumn>Action</TableColumn>
        </TableHeader>
        <TableBody>
          {operations.map((operation) => (
            <TableRow key={operation.id}>
              <TableCell className="font-mono text-xs">{operation.id.slice(0, 8)}</TableCell>
              <TableCell>{operation.op_kind.replace("_", " ")}</TableCell>
              <TableCell><Badge intent={stateTone(operation.state)}>{operation.state}</Badge></TableCell>
              <TableCell>{progressText(operation)}</TableCell>
              <TableCell>{operation.errored}</TableCell>
              <TableCell>{timeAgo(operation.submitted_at)}</TableCell>
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
