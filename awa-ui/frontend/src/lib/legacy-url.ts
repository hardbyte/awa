import { buildSearch, parseSearch } from "@/components/SearchBar";

function values(value: unknown): string[] {
  if (value === undefined || value === null || value === "") return [];
  return (Array.isArray(value) ? value : [value]).map(String).filter(Boolean);
}

/**
 * Folds the older `kind=`, `queue=` and `tag=` query parameters into the
 * `q` search the Jobs and Dead letters lists read, so a link written for
 * them filters instead of quietly listing everything. Returns null when
 * there is nothing to fold.
 */
export function foldLegacyFilter(search: Record<string, unknown>): Record<string, unknown> | null {
  const kinds = values(search.kind);
  const queues = values(search.queue);
  const tags = values(search.tag);
  if (kinds.length + queues.length + tags.length === 0) return null;

  const existing = parseSearch(typeof search.q === "string" ? search.q : "");
  const q = buildSearch({
    ...existing,
    kind: existing.kind ?? kinds[0],
    queue: existing.queue ?? queues[0],
    tags: [...(existing.tags ?? []), ...tags.filter((tag) => !(existing.tags ?? []).includes(tag))],
  });
  const { kind: _kind, queue: _queue, tag: _tag, ...rest } = search;
  return q ? { ...rest, q } : rest;
}
