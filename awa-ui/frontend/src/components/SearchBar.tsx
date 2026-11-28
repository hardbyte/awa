import { useState, useCallback, useRef, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchCronJobs, fetchDistinctKinds, fetchDistinctQueues, fetchDlqReasons } from "@/lib/api";
import { POLL, MAX_SUGGESTIONS } from "@/lib/constants";
import { LockClosedIcon } from "@heroicons/react/16/solid";

export interface SearchFilters {
  kind?: string;
  queue?: string;
  /** All of these tags (ADR-049). */
  tags?: string[];
  ids?: number[];
  /** Created at or after, as typed (a date or an RFC 3339 time). */
  createdGte?: string;
  /** Created before, as typed. */
  createdLt?: string;
  /** The cron schedule that enqueued the job. */
  cron?: string;
  /** The reason a dead letter was dead-lettered, matched exactly. */
  reason?: string;
}

/** Which list a search filters. `reason:` only means something for dead
 * letters, so the jobs list refuses it rather than listing only them. */
export type SearchView = "jobs" | "dlq";

/** The prefixes a search term can start with. */
export const FILTER_PREFIXES = [
  "kind:",
  "queue:",
  "tag:",
  "cron:",
  "reason:",
  "id:",
  "created>=",
  "created<",
] as const;

type FilterPrefix = (typeof FILTER_PREFIXES)[number];

/** The filter prefix `term` starts with, if any. */
export function filterPrefix(term: string): FilterPrefix | undefined {
  return FILTER_PREFIXES.find((p) => term.startsWith(p));
}

/**
 * Split a search into terms at whitespace. A value written in double quotes
 * (`reason:"bad input"`, with `\"` and `\\` for a quote and a backslash)
 * stays one term, spaces and all.
 */
export function splitTerms(search: string): string[] {
  const out: string[] = [];
  const isSpace = (at: number) => /\s/.test(search[at] ?? "");
  let at = 0;
  while (at < search.length) {
    while (at < search.length && isSpace(at)) at++;
    if (at >= search.length) break;
    const start = at;
    const prefix = FILTER_PREFIXES.find((p) => search.startsWith(p, at));
    if (prefix && search[at + prefix.length] === '"') {
      at += prefix.length + 1;
      while (at < search.length && search[at] !== '"') at += search[at] === "\\" ? 2 : 1;
      at = Math.min(at + 1, search.length);
    }
    while (at < search.length && !isSpace(at)) at++;
    out.push(search.slice(start, at));
  }
  return out;
}

/** A term's value without its quotes and escapes, or undefined when a quote
 * is left open or text follows the closing one. Unquoted values are taken as
 * written. */
function unquote(raw: string): string | undefined {
  if (!raw.startsWith('"')) return raw;
  let out = "";
  for (let at = 1; at < raw.length; at++) {
    const c = raw[at]!;
    if (c === '"') return at === raw.length - 1 ? out : undefined;
    if (c === "\\") {
      at++;
      if (at >= raw.length) return undefined;
      out += raw[at];
    } else {
      out += c;
    }
  }
  return undefined;
}

/** A value as a search term writes it: quoted when it holds whitespace or
 * starts with a quote, so it reads back as one term. */
export function quoteValue(value: string): string {
  if (!/\s/.test(value) && !value.startsWith('"')) return value;
  return `"${value.replace(/[\\"]/g, (c) => `\\${c}`)}"`;
}

/**
 * Parse a search string of `kind:`, `queue:`, `tag:`, `cron:`, `reason:`,
 * `id:`, `created>=` and `created<` terms. `tag:` and `id:` repeat (`id:`
 * also takes a comma list); for the others the last value wins.
 */
export function parseSearch(search: string): SearchFilters {
  const filters: SearchFilters = {};
  for (const part of splitTerms(search)) {
    const prefix = filterPrefix(part);
    if (!prefix) continue;
    const value = unquote(part.slice(prefix.length));
    if (!value) continue;
    switch (prefix) {
      case "kind:":
        filters.kind = value;
        break;
      case "queue:":
        filters.queue = value;
        break;
      case "tag:":
        filters.tags = [...(filters.tags ?? []).filter((t) => t !== value), value];
        break;
      case "cron:":
        filters.cron = value;
        break;
      case "reason:":
        filters.reason = value;
        break;
      case "id:": {
        const ids = value.split(",").filter(isJobId).map(Number);
        if (ids.length > 0) {
          filters.ids = [...new Set([...(filters.ids ?? []), ...ids])];
        }
        break;
      }
      case "created>=":
        filters.createdGte = value;
        break;
      case "created<":
        filters.createdLt = value;
        break;
    }
  }
  return filters;
}

/** Build a search string from individual filter values. */
export function buildSearch(filters: SearchFilters): string {
  const parts: string[] = [];
  const add = (prefix: FilterPrefix, value: string) => parts.push(`${prefix}${quoteValue(value)}`);
  if (filters.kind) add("kind:", filters.kind);
  if (filters.queue) add("queue:", filters.queue);
  for (const tag of filters.tags ?? []) add("tag:", tag);
  if (filters.cron) add("cron:", filters.cron);
  if (filters.reason) add("reason:", filters.reason);
  for (const id of filters.ids ?? []) parts.push(`id:${id}`);
  if (filters.createdGte) add("created>=", filters.createdGte);
  if (filters.createdLt) add("created<", filters.createdLt);
  return parts.join(" ");
}

/** A typed date or time as RFC 3339, or undefined when it does not parse. */
export function toRfc3339(value: string | undefined): string | undefined {
  if (!value) return undefined;
  const at = new Date(value);
  return Number.isNaN(at.getTime()) ? undefined : at.toISOString();
}

/** A job id the UI can carry exactly: a positive whole number no larger
 * than JavaScript's safe-integer limit. */
export function isJobId(value: string): boolean {
  return /^[1-9]\d*$/.test(value) && Number.isSafeInteger(Number(value));
}

/** One search term that isn't a complete, valid filter term, and why. */
export interface InvalidTerm {
  term: string;
  error: string;
}

function prefixList(view: SearchView): string {
  const prefixes = FILTER_PREFIXES.filter((p) => view === "dlq" || p !== "reason:");
  return `${prefixes.slice(0, -1).join(", ")} or ${prefixes[prefixes.length - 1]}`;
}

/**
 * The terms of a search, typed or from the URL, that `parseSearch` would
 * drop, or that `view` can't apply. Dropping one leaves a wider filter than
 * the one written, so a page must not list, count or act on the search while
 * any remain.
 */
export function invalidTerms(search: string, view: SearchView = "jobs"): InvalidTerm[] {
  const out: InvalidTerm[] = [];
  for (const term of splitTerms(search)) {
    const prefix = filterPrefix(term);
    const raw = prefix ? term.slice(prefix.length) : "";
    const value = unquote(raw);
    let error: string | undefined;
    if (!prefix) {
      error = `${term} isn't a filter; use ${prefixList(view)}`;
    } else if (value === undefined) {
      error = `${prefix} has an unclosed quote; write ${prefix}"two words"`;
    } else if (!value) {
      error = `${prefix} needs a value`;
    } else if (prefix === "reason:" && view !== "dlq") {
      error = "reason: filters dead letters; search for it on the Dead letters page";
    } else if (prefix === "id:" && !value.split(",").every(isJobId)) {
      error = "id: takes whole job ids, e.g. id:123 or id:123,456";
    } else if (prefix.startsWith("created") && !toRfc3339(value)) {
      error = `${prefix} takes a date or time, e.g. ${prefix}2026-10-30`;
    }
    if (error) out.push({ term, error });
  }
  return out;
}

/** Why a search can't be applied as written, or undefined when it can. */
export function termError(search: string, view: SearchView = "jobs"): string | undefined {
  return invalidTerms(search, view)[0]?.error;
}

/** One removable term: which filter it belongs to, and its value. */
type Term =
  | { key: "kind" | "queue" | "cron" | "reason" | "createdGte" | "createdLt"; value: string }
  | { key: "tags"; value: string }
  | { key: "ids"; value: number };

/** A suggestion that starts a term the user still has to complete. */
function isPrefixHint(value: string): boolean {
  return value.endsWith(":") || value.endsWith(">=") || value.endsWith("<");
}

function terms(filters: SearchFilters): Term[] {
  const out: Term[] = [];
  if (filters.kind) out.push({ key: "kind", value: filters.kind });
  if (filters.queue) out.push({ key: "queue", value: filters.queue });
  for (const tag of filters.tags ?? []) out.push({ key: "tags", value: tag });
  if (filters.cron) out.push({ key: "cron", value: filters.cron });
  if (filters.reason) out.push({ key: "reason", value: filters.reason });
  for (const id of filters.ids ?? []) out.push({ key: "ids", value: id });
  if (filters.createdGte) out.push({ key: "createdGte", value: filters.createdGte });
  if (filters.createdLt) out.push({ key: "createdLt", value: filters.createdLt });
  return out;
}

function termLabel(term: Term): string {
  switch (term.key) {
    case "tags":
      return `tag:${quoteValue(term.value)}`;
    case "ids":
      return `id:${term.value}`;
    case "createdGte":
      return `created>=${quoteValue(term.value)}`;
    case "createdLt":
      return `created<${quoteValue(term.value)}`;
    default:
      return `${term.key}:${quoteValue(term.value)}`;
  }
}

function withoutTerm(filters: SearchFilters, term: Term): SearchFilters {
  const next = { ...filters };
  if (term.key === "tags") {
    next.tags = (next.tags ?? []).filter((t) => t !== term.value);
    if (next.tags.length === 0) delete next.tags;
  } else if (term.key === "ids") {
    next.ids = (next.ids ?? []).filter((id) => id !== term.value);
    if (next.ids.length === 0) delete next.ids;
  } else {
    delete next[term.key];
  }
  return next;
}

export function mergeFilters(base: SearchFilters, added: SearchFilters): SearchFilters {
  return parseSearch(`${buildSearch(base)} ${buildSearch(added)}`);
}

interface Suggestion {
  label: string;
  value: string;
}

interface SearchBarProps {
  /** The full filter string (e.g. "queue:ui_demo kind:data_import") */
  value: string;
  onChange: (value: string) => void;
  /** Terms the page fixes, shown as chips that can't be removed. */
  locked?: string[];
  /** The list the search filters; `reason:` applies only to dead letters. */
  view?: SearchView;
}

/**
 * Filter bar with chip-based applied filters and autocomplete input.
 * Applied filters appear as removable chips. The text input is for
 * adding new filters only — it clears after each filter is applied.
 */
export function SearchBar({ value, onChange, locked = [], view = "jobs" }: SearchBarProps) {
  const [inputValue, setInputValue] = useState("");
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [suggestions, setSuggestions] = useState<Suggestion[]>([]);
  const [selectedIdx, setSelectedIdx] = useState(-1);
  const [error, setError] = useState<string | undefined>();
  const containerRef = useRef<HTMLDivElement>(null);

  const activeFilters = parseSearch(value);
  // Kept beside the valid terms, so editing the search never quietly drops
  // one and widens it.
  const malformed = invalidTerms(value, view);
  const withMalformed = useCallback(
    (search: string) => [search, ...malformed.map((m) => m.term)].filter(Boolean).join(" "),
    [malformed]
  );

  const kindsQuery = useQuery<string[]>({
    queryKey: ["distinct-kinds"],
    queryFn: fetchDistinctKinds,
    staleTime: POLL.SLOW,
    refetchInterval: false,
  });

  const queuesQuery = useQuery<string[]>({
    queryKey: ["distinct-queues"],
    queryFn: fetchDistinctQueues,
    staleTime: POLL.SLOW,
    refetchInterval: false,
  });

  // Schedule names and DLQ reasons are fetched only once a term asks for them.
  const typed = inputValue.trim();
  const schedulesQuery = useQuery<string[]>({
    queryKey: ["search-schedules"],
    queryFn: async () => (await fetchCronJobs()).map((cron) => cron.name),
    enabled: typed.startsWith("cron:"),
    staleTime: POLL.SLOW,
    refetchInterval: false,
    retry: false,
  });
  const reasonsQuery = useQuery<string[]>({
    queryKey: ["search-dlq-reasons", activeFilters.queue ?? null, activeFilters.kind ?? null],
    queryFn: async () => {
      const groups = await fetchDlqReasons({ queue: activeFilters.queue, kind: activeFilters.kind });
      return [...new Set((groups ?? []).map((group) => group.reason))];
    },
    enabled: view === "dlq" && typed.startsWith("reason:"),
    staleTime: POLL.SLOW,
    refetchInterval: false,
    retry: false,
  });

  // Global "/" shortcut to focus search
  useEffect(() => {
    function handleGlobalKeyDown(e: KeyboardEvent) {
      if (
        e.key === "/" &&
        !e.ctrlKey &&
        !e.metaKey &&
        document.activeElement?.tagName !== "INPUT" &&
        document.activeElement?.tagName !== "TEXTAREA"
      ) {
        e.preventDefault();
        containerRef.current?.querySelector("input")?.focus();
      }
    }
    document.addEventListener("keydown", handleGlobalKeyDown);
    return () => document.removeEventListener("keydown", handleGlobalKeyDown);
  }, []);

  const computeSuggestions = useCallback(
    (text: string) => {
      const results: Suggestion[] = [];
      const trimmed = text.trim();

      if (trimmed.startsWith("kind:")) {
        const prefix = trimmed.slice(5).toLowerCase();
        for (const kind of kindsQuery.data ?? []) {
          if (kind.toLowerCase().includes(prefix)) {
            results.push({ label: `kind:${kind}`, value: `kind:${kind}` });
          }
        }
      } else if (trimmed.startsWith("queue:")) {
        const prefix = trimmed.slice(6).toLowerCase();
        for (const queue of queuesQuery.data ?? []) {
          if (queue.toLowerCase().includes(prefix)) {
            results.push({ label: `queue:${queue}`, value: `queue:${queue}` });
          }
        }
      } else if (trimmed.startsWith("cron:")) {
        const prefix = trimmed.slice(5).toLowerCase();
        for (const name of schedulesQuery.data ?? []) {
          if (name.toLowerCase().includes(prefix)) {
            const term = `cron:${quoteValue(name)}`;
            results.push({ label: term, value: term });
          }
        }
      } else if (trimmed.startsWith("reason:")) {
        const prefix = trimmed.slice(7).toLowerCase();
        for (const reason of view === "dlq" ? reasonsQuery.data ?? [] : []) {
          if (reason.toLowerCase().includes(prefix)) {
            const term = `reason:${quoteValue(reason)}`;
            results.push({ label: term, value: term });
          }
        }
      } else if (
        trimmed.startsWith("tag:") ||
        trimmed.startsWith("id:") ||
        trimmed.startsWith("created")
      ) {
        // Free-form values: nothing to suggest.
      } else {
        // Show prefix hints for filters not yet applied
        if (!activeFilters.kind) {
          results.push({ label: "kind:<name>", value: "kind:" });
        }
        if (!activeFilters.queue) {
          results.push({ label: "queue:<name>", value: "queue:" });
        }
        results.push({ label: "tag:<name>", value: "tag:" });
        if (!activeFilters.cron) {
          results.push({ label: "cron:<schedule>", value: "cron:" });
        }
        if (view === "dlq" && !activeFilters.reason) {
          results.push({ label: "reason:<dlq reason>", value: "reason:" });
        }
        results.push({ label: "id:<job id>", value: "id:" });
        if (!activeFilters.createdGte) {
          results.push({ label: "created>=<date>", value: "created>=" });
        }
      }

      return results.slice(0, MAX_SUGGESTIONS);
    },
    [kindsQuery.data, queuesQuery.data, schedulesQuery.data, reasonsQuery.data, activeFilters, view]
  );

  // Schedule names and reasons arrive after their term is typed; offer them
  // once they do, while the input still has focus.
  useEffect(() => {
    if (!containerRef.current?.contains(document.activeElement)) return;
    const suggs = computeSuggestions(inputValue);
    setSuggestions(suggs);
    setShowSuggestions(suggs.length > 0);
    // Only new data should reopen the list, not every keystroke.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [schedulesQuery.data, reasonsQuery.data]);

  const handleInputChange = (text: string) => {
    setInputValue(text);
    setError(undefined);
    const suggs = computeSuggestions(text);
    setSuggestions(suggs);
    setShowSuggestions(suggs.length > 0);
    setSelectedIdx(-1);
  };

  /** Apply a filter token — merges into existing filters, clears input */
  const applyToken = useCallback(
    (token: string) => {
      const problem = termError(token, view);
      if (problem) {
        setError(problem);
        setShowSuggestions(false);
        return;
      }
      onChange(withMalformed(buildSearch(mergeFilters(activeFilters, parseSearch(token)))));
      setInputValue("");
      setShowSuggestions(false);
      setSelectedIdx(-1);
    },
    [activeFilters, onChange, withMalformed, view]
  );

  /** Remove a single term */
  const removeTerm = useCallback(
    (term: Term) => {
      onChange(withMalformed(buildSearch(withoutTerm(activeFilters, term))));
    },
    [activeFilters, onChange, withMalformed]
  );

  /** Remove a term that doesn't parse */
  const removeMalformed = useCallback(
    (term: string) => {
      const rest = malformed.filter((m) => m.term !== term).map((m) => m.term);
      onChange([buildSearch(activeFilters), ...rest].filter(Boolean).join(" "));
    },
    [activeFilters, malformed, onChange]
  );

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (showSuggestions && suggestions.length > 0) {
      switch (e.key) {
        case "ArrowDown":
          e.preventDefault();
          setSelectedIdx((prev) =>
            prev < suggestions.length - 1 ? prev + 1 : 0
          );
          return;
        case "ArrowUp":
          e.preventDefault();
          setSelectedIdx((prev) =>
            prev > 0 ? prev - 1 : suggestions.length - 1
          );
          return;
        case "Escape":
          setShowSuggestions(false);
          setSelectedIdx(-1);
          return;
        case "Enter": {
          e.preventDefault();
          if (selectedIdx >= 0 && selectedIdx < suggestions.length) {
            const s = suggestions[selectedIdx];
            if (s && !isPrefixHint(s.value)) {
              applyToken(s.value);
            } else if (s) {
              setInputValue(s.value);
              const suggs = computeSuggestions(s.value);
              setSuggestions(suggs);
              setSelectedIdx(-1);
            }
          } else if (inputValue.trim()) {
            applyToken(inputValue.trim());
          }
          return;
        }
      }
    }

    if (e.key === "Enter" && inputValue.trim()) {
      e.preventDefault();
      applyToken(inputValue.trim());
    }

    // Backspace on empty input removes last filter
    if (e.key === "Backspace" && inputValue === "") {
      const applied = terms(activeFilters);
      const lastMalformed = malformed[malformed.length - 1];
      if (lastMalformed) {
        removeMalformed(lastMalformed.term);
      } else if (applied.length > 0) {
        removeTerm(applied[applied.length - 1]!);
      }
    }
  };

  // Close dropdown on click outside
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (
        containerRef.current &&
        !containerRef.current.contains(event.target as Node)
      ) {
        setShowSuggestions(false);
        setSelectedIdx(-1);
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  const applied = terms(activeFilters);
  const hasFilters = applied.length > 0 || malformed.length > 0 || locked.length > 0;

  return (
    <div ref={containerRef} className="relative">
      {/* Chip input container — looks like one field */}
      <div
        className="flex flex-wrap items-center gap-1.5 rounded-lg border border-input bg-bg px-3 py-1.5 text-sm focus-within:ring-2 focus-within:ring-ring/20 focus-within:border-ring"
        onClick={() => containerRef.current?.querySelector("input")?.focus()}
      >
        {locked.map((term) => (
          <span
            key={`locked:${term}`}
            className="inline-flex items-center gap-1 rounded-md bg-secondary px-2 py-0.5 text-xs font-medium text-secondary-fg"
            title="Set by this page"
            data-testid="locked-filter"
          >
            <LockClosedIcon aria-hidden="true" className="size-3" />
            {term}
          </span>
        ))}
        {/* Applied filter chips */}
        {applied.map((term) => (
          <FilterChip
            key={termLabel(term)}
            label={termLabel(term)}
            onRemove={() => removeTerm(term)}
          />
        ))}
        {malformed.map(({ term, error }) => (
          <FilterChip
            key={`invalid:${term}`}
            label={term}
            title={error}
            invalid
            onRemove={() => removeMalformed(term)}
          />
        ))}

        {/* Text input */}
        <input
          type="text"
          value={inputValue}
          onChange={(e) => handleInputChange(e.target.value)}
          onFocus={() => {
            const suggs = computeSuggestions(inputValue);
            setSuggestions(suggs);
            setShowSuggestions(suggs.length > 0);
          }}
          onKeyDown={handleKeyDown}
          placeholder={
            hasFilters
              ? "Add filter..."
              : `Filter by kind:, queue:, tag:, cron:${view === "dlq" ? ", reason:" : ""}, id:, created>= ...  (press /)`
          }
          className="min-w-[120px] flex-1 border-0 bg-transparent p-0 text-sm text-fg outline-none placeholder:text-muted-fg"
          aria-label="Search jobs"
          aria-invalid={error ? true : undefined}
        />
      </div>
      {error && (
        <p className="mt-1 text-xs text-danger" role="alert">
          {error}
        </p>
      )}

      {/* Suggestions dropdown */}
      {showSuggestions && suggestions.length > 0 && (
        <ul
          className="absolute z-20 mt-1 w-full rounded-lg border border-border bg-overlay p-1 shadow-lg"
          role="listbox"
        >
          {suggestions.map((suggestion, index) => (
            <li key={index} role="option" aria-selected={index === selectedIdx}>
              <button
                type="button"
                className={[
                  "w-full cursor-pointer rounded-md px-3 py-1.5 text-left text-sm",
                  index === selectedIdx
                    ? "bg-primary text-primary-fg"
                    : "text-fg hover:bg-secondary",
                ].join(" ")}
                onMouseDown={(e) => {
                  e.preventDefault();
                  if (!isPrefixHint(suggestion.value)) {
                    applyToken(suggestion.value);
                  } else {
                    setInputValue(suggestion.value);
                    const suggs = computeSuggestions(suggestion.value);
                    setSuggestions(suggs);
                    setSelectedIdx(-1);
                  }
                }}
                onMouseEnter={() => setSelectedIdx(index)}
              >
                {suggestion.label}
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

/** A removable filter chip */
function FilterChip({
  label,
  onRemove,
  title,
  invalid = false,
}: {
  label: string;
  onRemove: () => void;
  title?: string;
  /** A term that doesn't parse, so nothing is listed until it's fixed. */
  invalid?: boolean;
}) {
  return (
    <span
      title={title}
      data-invalid={invalid || undefined}
      className={[
        "inline-flex items-center gap-1 rounded-md px-2 py-0.5 text-xs font-medium",
        invalid ? "bg-danger-subtle text-danger-subtle-fg" : "bg-primary-subtle text-primary-subtle-fg",
      ].join(" ")}
    >
      {label}
      <button
        type="button"
        onClick={(e) => {
          e.stopPropagation();
          onRemove();
        }}
        className="ml-0.5 rounded-sm hover:bg-primary-subtle-fg/10"
        aria-label={`Remove ${label} filter`}
      >
        <svg className="size-3" viewBox="0 0 16 16" fill="currentColor">
          <path d="M12.207 4.793a1 1 0 0 1 0 1.414L9.414 9l2.793 2.793a1 1 0 0 1-1.414 1.414L8 10.414l-2.793 2.793a1 1 0 0 1-1.414-1.414L6.586 9 3.793 6.207a1 1 0 0 1 1.414-1.414L8 7.586l2.793-2.793a1 1 0 0 1 1.414 0z" />
        </svg>
      </button>
    </span>
  );
}
