/** Rules the batch operations form and the jobs page share. */

import type { BatchOperationFilter, BatchOperationPreview, JobCount } from "@/lib/api";

/**
 * Whether a filter names which jobs a batch operation changes. State and
 * `dead_lettered` don't: every eligible job is available or scheduled and
 * live, so the server refuses a filter that has nothing else.
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

/** The jobs a filter selects, in words. */
export function describeFilter(filter: BatchOperationFilter): string {
  const states = filter.state === undefined ? [] : [filter.state].flat();
  const parts: string[] = [];
  if (filter.queue) parts.push(`queue ${filter.queue}`);
  if (filter.kind) parts.push(`kind ${filter.kind}`);
  if (filter.tag !== undefined) parts.push(`tagged ${list(filter.tag)}`);
  if (filter.ids && filter.ids.length > 0) parts.push(`id ${list(filter.ids)}`);
  if (filter.created_at_gte) parts.push(`created from ${filter.created_at_gte}`);
  if (filter.created_at_lt) parts.push(`created before ${filter.created_at_lt}`);
  const scope = states.length > 0 ? states.join(" or ") : "available or scheduled";
  return parts.length > 0 ? `${scope} jobs with ${parts.join(", ")}` : `${scope} jobs`;
}
