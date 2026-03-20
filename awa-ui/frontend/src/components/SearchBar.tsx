import { useState, useCallback, useRef, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchDistinctKinds, fetchDistinctQueues } from "@/lib/api";

interface SearchFilters {
  kind?: string;
  queue?: string;
  tag?: string;
}

/** Parse a search string that may contain kind:, queue:, tag: prefixes. */
export function parseSearch(search: string): SearchFilters {
  const filters: SearchFilters = {};
  const parts = search.trim().split(/\s+/);
  for (const part of parts) {
    if (part.startsWith("kind:") && part.length > 5) {
      filters.kind = part.slice(5);
    } else if (part.startsWith("queue:") && part.length > 6) {
      filters.queue = part.slice(6);
    } else if (part.startsWith("tag:") && part.length > 4) {
      filters.tag = part.slice(4);
    }
  }
  return filters;
}

/** Build a search string from individual filter values. */
function buildSearch(filters: SearchFilters): string {
  const parts: string[] = [];
  if (filters.kind) parts.push(`kind:${filters.kind}`);
  if (filters.queue) parts.push(`queue:${filters.queue}`);
  if (filters.tag) parts.push(`tag:${filters.tag}`);
  return parts.join(" ");
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
  const containerRef = useRef<HTMLDivElement>(null);

  const activeFilters = parseSearch(value);

  const kindsQuery = useQuery<string[]>({
    queryKey: ["distinct-kinds"],
    queryFn: fetchDistinctKinds,
    staleTime: 60_000,
    refetchInterval: false,
  });

  const queuesQuery = useQuery<string[]>({
    queryKey: ["distinct-queues"],
    queryFn: fetchDistinctQueues,
    staleTime: 60_000,
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
      } else if (trimmed.startsWith("tag:")) {
        // No autocomplete for tags
      } else {
        // Show prefix hints for filters not yet applied
        if (!activeFilters.kind) {
          results.push({ label: "kind:<name>", value: "kind:" });
        }
        if (!activeFilters.queue) {
          results.push({ label: "queue:<name>", value: "queue:" });
        }
        if (!activeFilters.tag) {
          results.push({ label: "tag:<name>", value: "tag:" });
        }
      }

      return results.slice(0, 8);
    },
    [kindsQuery.data, queuesQuery.data, activeFilters]
  );

  const handleInputChange = (text: string) => {
    setInputValue(text);
    const suggs = computeSuggestions(text);
    setSuggestions(suggs);
    setShowSuggestions(suggs.length > 0);
    setSelectedIdx(-1);
  };

  /** Apply a filter token — merges into existing filters, clears input */
  const applyToken = useCallback(
    (token: string) => {
      const parsed = parseSearch(token);
      const merged = { ...activeFilters, ...parsed };
      onChange(buildSearch(merged));
      setInputValue("");
      setShowSuggestions(false);
      setSelectedIdx(-1);
    },
    [activeFilters, onChange]
  );

  /** Remove a single filter */
  const removeFilter = useCallback(
    (key: keyof SearchFilters) => {
      const next = { ...activeFilters };
      delete next[key];
      onChange(buildSearch(next));
    },
    [activeFilters, onChange]
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
            if (s && !s.value.endsWith(":")) {
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
      const keys = (Object.keys(activeFilters) as (keyof SearchFilters)[]).filter(
        (k) => activeFilters[k]
      );
      if (keys.length > 0) {
        removeFilter(keys[keys.length - 1]!);
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

  const hasFilters =
    activeFilters.kind || activeFilters.queue || activeFilters.tag;

  return (
    <div ref={containerRef} className="relative">
      {/* Chip input container — looks like one field */}
      <div
        className="flex flex-wrap items-center gap-1.5 rounded-lg border border-input bg-bg px-3 py-1.5 text-sm focus-within:ring-2 focus-within:ring-ring/20 focus-within:border-ring"
        onClick={() => containerRef.current?.querySelector("input")?.focus()}
      >
        {/* Applied filter chips */}
        {activeFilters.kind && (
          <FilterChip
            label={`kind:${activeFilters.kind}`}
            onRemove={() => removeFilter("kind")}
          />
        )}
        {activeFilters.queue && (
          <FilterChip
            label={`queue:${activeFilters.queue}`}
            onRemove={() => removeFilter("queue")}
          />
        )}
        {activeFilters.tag && (
          <FilterChip
            label={`tag:${activeFilters.tag}`}
            onRemove={() => removeFilter("tag")}
          />
        )}

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
          placeholder={hasFilters ? "Add filter..." : "Filter by kind:, queue:, tag: ...  (press /)"}
          className="min-w-[120px] flex-1 border-0 bg-transparent p-0 text-sm text-fg outline-none placeholder:text-muted-fg"
          aria-label="Search jobs"
        />
      </div>

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
                  if (!suggestion.value.endsWith(":")) {
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
}: {
  label: string;
  onRemove: () => void;
}) {
  return (
    <span className="inline-flex items-center gap-1 rounded-md bg-primary-subtle px-2 py-0.5 text-xs font-medium text-primary-subtle-fg">
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
