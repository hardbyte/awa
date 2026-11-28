import { filterPrefix, invalidTerms, isJobId, parseSearch, type SearchView } from "@/components/SearchBar";
import { SECTION_LABELS, type ListRoute } from "@/lib/sections";

/** Where a palette entry goes, in the shape the router's `navigate` takes. */
export interface PaletteTarget {
  to: string;
  params?: Record<string, string>;
  search?: Record<string, string>;
}

export interface PaletteItem {
  /** Stable while typing for entries built from the input, so keyboard focus stays on them. */
  id: string;
  section: "Go to" | "Jobs" | "Queues" | "Job kinds" | "Schedules" | "Bulk operations" | "Pages";
  label: string;
  /** The raw key when the label is a display name. */
  detail?: string;
  target: PaletteTarget;
}

export interface PaletteData {
  queues?: { queue: string; display_name?: string | null }[];
  kinds?: { kind: string; display_name?: string | null }[];
  schedules?: { name: string; kind: string }[];
  batchOps?: { id: string; op_kind: string }[];
}

const PAGES: { to: "/" | ListRoute; label: string }[] = [
  { to: "/", label: "Overview" },
  ...(Object.entries(SECTION_LABELS) as [ListRoute, string][]).map(([to, label]) => ({ to, label })),
];

/** Entries listed per section, so one long list can't push the others out of view. */
export const PALETTE_SECTION_LIMIT = 6;

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const UUID_PREFIX = /^[0-9a-f]{8}$/i;

function matches(input: string, ...values: (string | null | undefined)[]): boolean {
  const needle = input.toLowerCase();
  return values.some((value) => value != null && value.toLowerCase().includes(needle));
}

/** Exact matches first, then prefix matches, then the rest in their given order. */
function rank<T>(items: T[], input: string, key: (item: T) => string[]): T[] {
  const needle = input.toLowerCase();
  const score = (item: T) => {
    const keys = key(item).map((k) => k.toLowerCase());
    if (keys.includes(needle)) return 0;
    if (keys.some((k) => k.startsWith(needle))) return 1;
    return 2;
  };
  return items
    .map((item, index) => ({ item, index, score: score(item) }))
    .sort((a, b) => a.score - b.score || a.index - b.index)
    .map(({ item }) => item);
}

function label(key: string, displayName?: string | null): { label: string; detail?: string } {
  const name = displayName?.trim();
  return name && name !== key ? { label: name, detail: key } : { label: key };
}

/**
 * Why the input can't be a jobs search, when it uses a filter prefix (e.g.
 * `id:abc` or `kind:`) that doesn't validate; undefined otherwise.
 */
export function paletteSearchProblem(rawInput: string): string | undefined {
  const input = rawInput.trim();
  return invalidTerms(input, searchView(input)).find(({ term }) => filterPrefix(term))?.error;
}

/** The list a search belongs on: a `reason:` term filters dead letters. */
function searchView(input: string): SearchView {
  return parseSearch(input).reason ? "dlq" : "jobs";
}

/** The palette's entries for what has been typed, in display order. */
export function paletteItems(rawInput: string, data: PaletteData): PaletteItem[] {
  const input = rawInput.trim();
  const items: PaletteItem[] = [];

  if (input && isJobId(input.replace(/^#/, ""))) {
    const id = input.replace(/^#/, "");
    items.push({ id: "open-job", section: "Go to", label: `Open job #${id}`, target: { to: "/jobs/$id", params: { id } } });
  }

  if (UUID.test(input)) {
    const id = input.toLowerCase();
    items.push({
      id: "open-batch-op",
      section: "Go to",
      label: `Open bulk operation ${id.slice(0, 8)}`,
      detail: id,
      target: { to: "/batch-ops/$id", params: { id } },
    });
  } else if (UUID_PREFIX.test(input)) {
    for (const op of (data.batchOps ?? []).filter((op) => op.id.toLowerCase().startsWith(input.toLowerCase()))) {
      items.push({
        id: `batch-op:${op.id}`,
        section: "Bulk operations",
        label: `Open bulk operation ${op.id.slice(0, 8)}`,
        detail: op.op_kind,
        target: { to: "/batch-ops/$id", params: { id: op.id } },
      });
    }
  }

  if (input.includes(":") || filterPrefix(input)) {
    const view = searchView(input);
    const valid = invalidTerms(input, view).length === 0 && Object.keys(parseSearch(input)).length > 0;
    if (valid && view === "dlq") {
      items.push({ id: "dlq-search", section: "Jobs", label: `Dead letters matching ${input}`, target: { to: "/dlq", search: { q: input } } });
    } else if (valid) {
      items.push({ id: "jobs-search", section: "Jobs", label: `Jobs matching ${input}`, target: { to: "/jobs", search: { q: input } } });
    } else if (!/\s/.test(input) && !filterPrefix(input)) {
      items.push({ id: "jobs-tag", section: "Jobs", label: `Jobs tagged ${input}`, target: { to: "/jobs", search: { q: `tag:${input}` } } });
    }
  }

  const sections: PaletteItem[][] = [];

  const queues = (data.queues ?? []).filter((q) => !input || matches(input, q.queue, q.display_name));
  sections.push(
    rank(queues, input, (q) => [q.queue, q.display_name ?? ""]).map((q) => ({
      id: `queue:${q.queue}`,
      section: "Queues",
      ...label(q.queue, q.display_name),
      target: { to: "/queues/$name", params: { name: q.queue } },
    }))
  );

  const kinds = (data.kinds ?? []).filter((k) => !input || matches(input, k.kind, k.display_name));
  sections.push(
    rank(kinds, input, (k) => [k.kind, k.display_name ?? ""]).map((k) => ({
      id: `kind:${k.kind}`,
      section: "Job kinds",
      ...label(k.kind, k.display_name),
      target: { to: "/kinds/$kind", params: { kind: k.kind } },
    }))
  );

  const schedules = (data.schedules ?? []).filter((s) => !input || matches(input, s.name));
  sections.push(
    rank(schedules, input, (s) => [s.name]).map((s) => ({
      id: `schedule:${s.name}`,
      section: "Schedules",
      label: s.name,
      detail: s.kind,
      target: { to: "/cron/$name", params: { name: s.name } },
    }))
  );

  const pages = PAGES.filter((p) => !input || matches(input, p.label));
  const pageItems: PaletteItem[] = pages.map((p) => ({ id: `page:${p.to}`, section: "Pages", label: p.label, target: { to: p.to } }));

  // With nothing typed, pages lead; once typing, objects named by the input do.
  if (!input) items.push(...pageItems);
  for (const section of sections) items.push(...section.slice(0, PALETTE_SECTION_LIMIT));
  if (input) items.push(...pageItems);
  return items;
}
