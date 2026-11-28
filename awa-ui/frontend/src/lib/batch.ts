/** Rules the batch operations form and the jobs page share. */

import {
  toWireFilter,
  type BatchOperation,
  type BatchOperationFilter,
  type BatchOperationKind,
  type BatchOperationPayload,
  type BatchOperationPreview,
  type JobCount,
} from "@/lib/api";
import { isJobId, toRfc3339, type SearchFilters } from "@/components/SearchBar";
import { formatNumber } from "@/lib/number";

/**
 * Whether a filter names which jobs a batch operation changes. State and
 * `dead_lettered` don't: each operation kind acts on a fixed set of states
 * and either live or dead-lettered jobs, so a state alone still names every
 * job in it and the server refuses a filter that has nothing else.
 */
export function isNarrowing(filter: BatchOperationFilter): boolean {
  const tags = filter.tag === undefined ? [] : [filter.tag].flat();
  return Boolean(
    filter.kind ||
      filter.queue ||
      tags.length > 0 ||
      (filter.ids && filter.ids.length > 0) ||
      filter.created_at_gte ||
      filter.created_at_lt
  );
}

function canonical(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .filter(([, v]) => v !== undefined)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([k, v]) => [k, canonical(v)])
    );
  }
  return value;
}

/** Whether two filters are the same, field for field. */
export function sameFilter(a: BatchOperationFilter, b: BatchOperationFilter): boolean {
  return JSON.stringify(canonical(a)) === JSON.stringify(canonical(b));
}

/** The preview's match count; a plain number is exact. */
export function previewCount(preview: BatchOperationPreview): JobCount {
  const total = preview.total_matched;
  return typeof total === "number" ? { kind: "exact", value: total } : total;
}

export function formatCount(count: JobCount): string {
  const value = count.value.toLocaleString();
  return count.kind === "at_least" ? `at least ${value}` : value;
}

function list(value: string | string[] | number[]): string {
  return [value].flat().join(", ");
}

/** The jobs a filter selects, in words. `scope` names the jobs a filter
 * without a state selects. */
export function describeFilter(
  filter: BatchOperationFilter,
  scope: string = "available or scheduled"
): string {
  const states = filter.state === undefined ? [] : [filter.state].flat();
  const parts: string[] = [];
  if (filter.queue) parts.push(`queue ${filter.queue}`);
  if (filter.kind) parts.push(`kind ${filter.kind}`);
  if (filter.tag !== undefined) parts.push(`tagged ${list(filter.tag)}`);
  if (filter.ids && filter.ids.length > 0) parts.push(`id ${list(filter.ids)}`);
  if (filter.created_at_gte) parts.push(`created from ${filter.created_at_gte}`);
  if (filter.created_at_lt) parts.push(`created before ${filter.created_at_lt}`);
  const selected = states.length > 0 ? states.join(" or ") : scope;
  return parts.length > 0 ? `${selected} jobs with ${parts.join(", ")}` : `${selected} jobs`;
}

/** A completed operation that matched rows but changed none of them. */
export function changedNothing(operation: BatchOperation): boolean {
  return operation.state === "completed" && operation.processed === 0 && operation.skipped + operation.errored > 0;
}

export function progressText(operation: BatchOperation): string {
  const total = operation.total_matched ?? 0;
  const accounted = operation.processed + operation.skipped + operation.errored;
  if (total === 0) return formatNumber(accounted);
  return `${formatNumber(accounted)}/${formatNumber(total)}`;
}

export function parsePriority(value: string): number | null {
  const trimmed = value.trim();
  if (!trimmed) return null;
  const priority = Number(trimmed);
  if (!Number.isInteger(priority) || priority < 1 || priority > 4) return null;
  return priority;
}

export interface KindOption {
  kind: BatchOperationKind;
  label: string;
  /** The job states the kind acts on. */
  states: string[];
  /** The jobs a filter without a state selects, in words. */
  scope: string;
}

/** Every operation kind and the jobs it acts on (ADR-030). */
export const KIND_OPTIONS: KindOption[] = [
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

export function kindOption(kind: BatchOperationKind): KindOption {
  const option = KIND_OPTIONS.find((candidate) => candidate.kind === kind);
  if (!option) throw new Error(`unknown batch operation kind: ${kind}`);
  return option;
}

export function isPayloadFormValid(form: BatchForm): boolean {
  const priority = parsePriority(form.priority);
  if (form.opKind === "set_priority") return priority !== null;
  if (form.opKind === "move_queue") {
    if (!form.destinationQueue.trim()) return false;
    return !form.priority.trim() || priority !== null;
  }
  return true;
}

export interface BatchForm {
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

export function splitList(value: string): string[] {
  return value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

/**
 * Why the form's filter can't be sent: a field that doesn't parse would
 * otherwise be left out, widening the jobs the operation changes.
 */
export function filterError(form: Pick<BatchForm, "ids" | "createdGte" | "createdLt">): string | null {
  if (!splitList(form.ids).every(isJobId)) return "IDs must be whole job ids";
  if (form.createdGte.trim() && !toRfc3339(form.createdGte.trim())) return "Created from is not a date";
  if (form.createdLt.trim() && !toRfc3339(form.createdLt.trim())) return "Created before is not a date";
  return null;
}

/** The shared job filter (ADR-049) the form describes. */
export function buildFilter(form: BatchForm): BatchOperationFilter {
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

export function buildPayload(form: BatchForm): BatchOperationPayload {
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
export function describeOperation(payload: BatchOperationPayload, preview: BatchOperationPreview): string {
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

/** Keys a link from another page fills in. Any of them on /batch-ops opens
 * the new-operation form with them. */
export const BATCH_PREFILL_KEYS = [
  "queue",
  "kind",
  "tags",
  "ids",
  "state",
  "created_at_gte",
  "created_at_lt",
] as const;

const DEFAULT_OP: BatchOperationKind = "set_priority";
const DEFAULT_PRIORITY = "1";

function searchText(value: unknown): string {
  return value === undefined || value === null ? "" : String(value);
}

/** The new-operation form described by its URL. The router parses query
 * values as JSON, so `?queue=123` arrives as a number. */
export function formFromSearch(search: Record<string, unknown>): BatchForm {
  const op = KIND_OPTIONS.some((option) => option.kind === search.op)
    ? (search.op as BatchOperationKind)
    : DEFAULT_OP;
  const state = searchText(search.state);
  return {
    opKind: op,
    queue: searchText(search.queue),
    kind: searchText(search.kind),
    ids: searchText(search.ids),
    tags: searchText(search.tags),
    state: kindOption(op).states.includes(state) ? state : "",
    createdGte: searchText(search.created_at_gte),
    createdLt: searchText(search.created_at_lt),
    priority: search.priority === undefined ? DEFAULT_PRIORITY : searchText(search.priority),
    destinationQueue: searchText(search.destination),
    reason: searchText(search.reason),
  };
}

/** The URL search that restores a form; defaults and blanks are left out. */
export function searchFromForm(form: BatchForm): Record<string, string> {
  const entries: Record<string, string> = {
    op: form.opKind === DEFAULT_OP ? "" : form.opKind,
    queue: form.queue,
    kind: form.kind,
    tags: form.tags,
    ids: form.ids,
    state: form.state,
    created_at_gte: form.createdGte,
    created_at_lt: form.createdLt,
    priority: form.priority === DEFAULT_PRIORITY ? "" : form.priority,
    destination: form.destinationQueue,
    reason: form.reason,
  };
  return Object.fromEntries(Object.entries(entries).filter(([, value]) => value !== ""));
}

/** The label of an operation kind, e.g. "Set priority". */
export function operationLabel(kind: string): string {
  const option = KIND_OPTIONS.find((candidate) => candidate.kind === kind);
  return option ? option.label : kind.replace(/_/g, " ");
}

/** Whether an operation acts on dead-lettered rows rather than live jobs. */
export function actsOnDlq(kind: string): boolean {
  return kind === "retry_from_dlq" || kind === "purge_dlq";
}

/** A wire filter as a Jobs or Dead letters search, with the one state the
 * Jobs page can show when the filter names exactly one. */
export function filterToSearch(filter: BatchOperationFilter): { filter: SearchFilters; state?: string } {
  const tags = filter.tag === undefined ? [] : [filter.tag].flat();
  const states = filter.state === undefined ? [] : [filter.state].flat();
  const search: SearchFilters = {};
  if (filter.kind) search.kind = filter.kind;
  if (filter.queue) search.queue = filter.queue;
  if (tags.length > 0) search.tags = tags;
  if (filter.ids && filter.ids.length > 0) search.ids = filter.ids;
  if (filter.created_at_gte) search.createdGte = filter.created_at_gte;
  if (filter.created_at_lt) search.createdLt = filter.created_at_lt;
  return { filter: search, state: states.length === 1 ? states[0] : undefined };
}

/** What an operation sets, in words, or null when its kind takes nothing. */
export function describeSpec(kind: string, spec: unknown): string | null {
  const fields = spec && typeof spec === "object" ? (spec as Record<string, unknown>) : {};
  switch (kind) {
    case "set_priority":
      return fields.priority === undefined ? null : `Priority ${String(fields.priority)}`;
    case "move_queue": {
      const priority = fields.priority === undefined ? "" : `, priority ${String(fields.priority)}`;
      return `To queue ${String(fields.queue ?? "?")}${priority}`;
    }
    case "move_to_dlq":
      return fields.reason ? `Reason "${String(fields.reason)}"` : "Default reason";
    default:
      return null;
  }
}

/** "1,200 changed, 3 skipped", plus errors when there were any. */
export function batchOutcome(operation: Pick<BatchOperation, "processed" | "skipped" | "errored">): string {
  const parts = [`${formatNumber(operation.processed)} changed`, `${formatNumber(operation.skipped)} skipped`];
  if (operation.errored > 0) parts.push(`${formatNumber(operation.errored)} errored`);
  return parts.join(", ");
}

/** Whether an operation can still be cancelled. */
export function isCancellable(state: string): boolean {
  return state === "pending" || state === "scanning" || state === "running";
}

/** A batch operation id, which the API takes only as a UUID. */
export function isBatchOperationId(value: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value);
}
