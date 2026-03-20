import { useState, useCallback, useRef, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { SearchField, SearchInput } from "@/components/ui/search-field";
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
    if (part.startsWith("kind:")) {
      filters.kind = part.slice(5);
    } else if (part.startsWith("queue:")) {
      filters.queue = part.slice(6);
    } else if (part.startsWith("tag:")) {
      filters.tag = part.slice(4);
    }
  }
  return filters;
}

interface Suggestion {
  label: string;
  value: string;
}

interface SearchBarProps {
  value: string;
  onChange: (value: string) => void;
}

export function SearchBar({ value, onChange }: SearchBarProps) {
  const [localValue, setLocalValue] = useState(value);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [suggestions, setSuggestions] = useState<Suggestion[]>([]);
  const [selectedIdx, setSelectedIdx] = useState(-1);
  const containerRef = useRef<HTMLDivElement>(null);

  const kindsQuery = useQuery<string[]>({
    queryKey: ["distinct-kinds"],
    queryFn: fetchDistinctKinds,
  });

  const queuesQuery = useQuery<string[]>({
    queryKey: ["distinct-queues"],
    queryFn: fetchDistinctQueues,
  });

  // Sync external value changes
  useEffect(() => {
    setLocalValue(value);
  }, [value]);

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
      const lastPart = trimmed.split(/\s+/).pop() ?? "";

      if (lastPart.startsWith("kind:")) {
        const prefix = lastPart.slice(5).toLowerCase();
        for (const kind of kindsQuery.data ?? []) {
          if (kind.toLowerCase().includes(prefix)) {
            results.push({ label: `kind:${kind}`, value: `kind:${kind}` });
          }
        }
      } else if (lastPart.startsWith("queue:")) {
        const prefix = lastPart.slice(6).toLowerCase();
        for (const queue of queuesQuery.data ?? []) {
          if (queue.toLowerCase().includes(prefix)) {
            results.push({
              label: `queue:${queue}`,
              value: `queue:${queue}`,
            });
          }
        }
      } else if (!lastPart.startsWith("tag:") && lastPart.length === 0) {
        results.push(
          { label: "kind:<name>", value: "kind:" },
          { label: "queue:<name>", value: "queue:" },
          { label: "tag:<name>", value: "tag:" }
        );
      }

      return results.slice(0, 8);
    },
    [kindsQuery.data, queuesQuery.data]
  );

  const handleInputChange = useCallback(
    (newValue: string) => {
      setLocalValue(newValue);
      const suggs = computeSuggestions(newValue);
      setSuggestions(suggs);
      setShowSuggestions(suggs.length > 0);
      setSelectedIdx(-1);
    },
    [computeSuggestions]
  );

  const handleSubmit = useCallback(
    (submitValue?: string) => {
      const finalValue = submitValue ?? localValue;
      onChange(finalValue);
      setShowSuggestions(false);
      setSelectedIdx(-1);
    },
    [localValue, onChange]
  );

  const applySuggestion = useCallback(
    (suggestion: Suggestion) => {
      const parts = localValue.trim().split(/\s+/);
      parts.pop();
      parts.push(suggestion.value);
      const newValue = parts.join(" ");
      setLocalValue(newValue);
      setShowSuggestions(false);
      setSelectedIdx(-1);
      if (!suggestion.value.endsWith(":")) {
        onChange(newValue);
      }
    },
    [localValue, onChange]
  );

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (!showSuggestions || suggestions.length === 0) {
        if (e.key === "Enter") {
          handleSubmit();
        }
        return;
      }

      switch (e.key) {
        case "ArrowDown":
          e.preventDefault();
          setSelectedIdx((prev) =>
            prev < suggestions.length - 1 ? prev + 1 : 0
          );
          break;
        case "ArrowUp":
          e.preventDefault();
          setSelectedIdx((prev) =>
            prev > 0 ? prev - 1 : suggestions.length - 1
          );
          break;
        case "Enter": {
          e.preventDefault();
          if (selectedIdx >= 0 && selectedIdx < suggestions.length) {
            const s = suggestions[selectedIdx];
            if (s) applySuggestion(s);
          } else {
            handleSubmit();
          }
          break;
        }
        case "Escape":
          setShowSuggestions(false);
          setSelectedIdx(-1);
          break;
      }
    },
    [showSuggestions, suggestions, selectedIdx, applySuggestion, handleSubmit]
  );

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

  return (
    <div ref={containerRef} className="relative">
      <SearchField
        value={localValue}
        onChange={handleInputChange}
        onSubmit={() => handleSubmit()}
        onFocus={() => {
          const suggs = computeSuggestions(localValue);
          setSuggestions(suggs);
          setShowSuggestions(suggs.length > 0);
        }}
        onClear={() => {
          setLocalValue("");
          onChange("");
          setShowSuggestions(false);
        }}
        aria-label="Search jobs"
      >
        <SearchInput
          placeholder="Search by kind:, queue:, tag: ...  (press / to focus)"
          onKeyDown={handleKeyDown}
        />
      </SearchField>

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
                  applySuggestion(suggestion);
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
