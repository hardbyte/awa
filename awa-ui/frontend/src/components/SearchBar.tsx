import { useState, useCallback, useRef, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchDistinctKinds, fetchDistinctQueues } from "@/lib/api";
import { POLL, MAX_SUGGESTIONS } from "@/lib/constants";

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
}

/**
 * Parse a search string of `kind:`, `queue:`, `tag:`, `id:`, `created>=` and
 * `created<` terms. `tag:` and `id:` repeat (`id:` also takes a comma
 * list); for the others the last value wins.
 */
export function parseSearch(search: string): SearchFilters {
  const filters: SearchFilters = {};
  const parts = search.trim().split(/\s+/);
  for (const part of parts) {
    if (part.startsWith("kind:") && part.length > 5) {
      filters.kind = part.slice(5);
    } else if (part.startsWith("queue:") && part.length > 6) {
      filters.queue = part.slice(6);
    } else if (part.startsWith("tag:") && part.length > 4) {
      const tag = part.slice(4);
      filters.tags = [...(filters.tags ?? []).filter((t) => t !== tag), tag];
    } else if (part.startsWith("id:") && part.length > 3) {
      const ids = part
        .slice(3)
        .split(",")
        .filter(isJobId)
        .map(Number);
      if (ids.length > 0) {
        filters.ids = [...new Set([...(filters.ids ?? []), ...ids])];
      }
    } else if (part.startsWith("created>=") && part.length > 9) {
      filters.createdGte = part.slice(9);
    } else if (part.startsWith("created<") && part.length > 8) {
      filters.createdLt = part.slice(8);
    }
  }
  return filters;
}

/** Build a search string from individual filter values. */
export function buildSearch(filters: SearchFilters): string {
  const parts: string[] = [];
  if (filters.kind) parts.push(`kind:${filters.kind}`);
  if (filters.queue) parts.push(`queue:${filters.queue}`);
  for (const tag of filters.tags ?? []) parts.push(`tag:${tag}`);
  for (const id of filters.ids ?? []) parts.push(`id:${id}`);
  if (filters.createdGte) parts.push(`created>=${filters.createdGte}`);
  if (filters.createdLt) parts.push(`created<${filters.createdLt}`);
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

/**
 * The terms of a search, typed or from the URL, that `parseSearch` would
 * drop. Dropping one leaves a wider filter than the one written, so a page
 * must not list, count or act on the search while any remain.
 */
export function invalidTerms(search: string): InvalidTerm[] {
  const out: InvalidTerm[] = [];
  for (const term of search.trim().split(/\s+/).filter(Boolean)) {
    const prefix = ["kind:", "queue:", "tag:", "id:", "created>=", "created<"].find((p) =>
      term.startsWith(p)
    );
    const value = prefix ? term.slice(prefix.length) : "";
    let error: string | undefined;
    if (!prefix) {
      error = `${term} isn't a filter; use kind:, queue:, tag:, id:, created>= or created<`;
    } else if (!value) {
      error = `${prefix} needs a value`;
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
export function termError(search: string): string | undefined {
  return invalidTerms(search)[0]?.error;
}

/** One removable term: which filter it belongs to, and its value. */
type Term =
  | { key: "kind" | "queue" | "createdGte" | "createdLt"; value: string }
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
  for (const id of filters.ids ?? []) out.push({ key: "ids", value: id });
  if (filters.createdGte) out.push({ key: "createdGte", value: filters.createdGte });
  if (filters.createdLt) out.push({ key: "createdLt", value: filters.createdLt });
  return out;
}

function termLabel(term: Term): string {
  switch (term.key) {
    case "tags":
      return `tag:${term.value}`;
    case "ids":
      return `id:${term.value}`;
    case "createdGte":
      return `created>=${term.value}`;
    case "createdLt":
      return `created<${term.value}`;
    default:
      return `${term.key}:${term.value}`;
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

function mergeFilters(base: SearchFilters, added: SearchFilters): SearchFilters {
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
}

/**
 * Filter bar with chip-based applied filters and autocomplete input.
 * Applied filters appear as removable chips. The text input is for
 * adding new filters only — it clears after each filter is applied.
 */
export function SearchBar({ value, onChange }: SearchBarProps) {
  const [inputValue, setInputValue] = useState("");
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [suggestions, setSuggestions] = useState<Suggestion[]>([]);
  const [selectedIdx, setSelectedIdx] = useState(-1);
  const [error, setError] = useState<string | undefined>();
  const containerRef = useRef<HTMLDivElement>(null);

  const activeFilters = parseSearch(value);
  // Kept beside the valid terms, so editing the search never quietly drops
  // one and widens it.
  const malformed = invalidTerms(value);
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
        results.push({ label: "id:<job id>", value: "id:" });
        if (!activeFilters.createdGte) {
          results.push({ label: "created>=<date>", value: "created>=" });
        }
      }

      return results.slice(0, MAX_SUGGESTIONS);
    },
    [kindsQuery.data, queuesQuery.data, activeFilters]
  );

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
      const problem = termError(token);
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
    [activeFilters, onChange, withMalformed]
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
  const hasFilters = applied.length > 0 || malformed.length > 0;

  return (
    <div ref={containerRef} className="relative">
      {/* Chip input container — looks like one field */}
      <div
        className="flex flex-wrap items-center gap-1.5 rounded-lg border border-input bg-bg px-3 py-1.5 text-sm focus-within:ring-2 focus-within:ring-ring/20 focus-within:border-ring"
        onClick={() => containerRef.current?.querySelector("input")?.focus()}
      >
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
          placeholder={hasFilters ? "Add filter..." : "Filter by kind:, queue:, tag:, id:, created>= ...  (press /)"}
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
