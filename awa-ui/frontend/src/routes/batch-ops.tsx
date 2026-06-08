import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  cancelBatchOperation,
  fetchBatchOperations,
  previewBatchOperation,
  submitBatchOperation,
  type BatchOperation,
  type BatchOperationKind,
  type BatchOperationPayload,
} from "@/lib/api";
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

function isPayloadFormValid(form: {
  opKind: BatchOperationKind;
  priority: string;
  destinationQueue: string;
}): boolean {
  const priority = parsePriority(form.priority);
  if (form.opKind === "set_priority") return priority !== null;
  if (!form.destinationQueue.trim()) return false;
  return !form.priority.trim() || priority !== null;
}

function buildPayload(form: {
  opKind: BatchOperationKind;
  queue: string;
  kind: string;
  ids: string;
  priority: string;
  destinationQueue: string;
}): BatchOperationPayload {
  const ids = form.ids
    .split(",")
    .map((id) => id.trim())
    .filter(Boolean)
    .map(Number)
    .filter(Number.isFinite);
  const filter = {
    ...(form.queue.trim() ? { queue: form.queue.trim() } : {}),
    ...(form.kind.trim() ? { kind: form.kind.trim() } : {}),
    ...(ids.length > 0 ? { ids } : {}),
  };
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

export function BatchOpsPage() {
  const queryClient = useQueryClient();
  const readOnly = useReadOnly();
  const [form, setForm] = useState({
    opKind: "set_priority" as BatchOperationKind,
    queue: "",
    kind: "",
    ids: "",
    priority: "1",
    destinationQueue: "",
  });
  const isPayloadValid = useMemo(() => isPayloadFormValid(form), [form]);
  const payload = useMemo(() => buildPayload(form), [form]);

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
    onSuccess: (preview) => {
      toast.success(`Preview matched ${preview.total_matched} job(s)`);
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
          <label className="space-y-1 text-sm md:col-span-2">
            <span className="font-medium">IDs filter</span>
            <Input value={form.ids} onChange={(event) => setForm((current) => ({ ...current, ids: event.target.value }))} placeholder="123, 456" />
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
