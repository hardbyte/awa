import { useEffect, useState } from "react";

// Persists a small piece of UI state across refreshes using localStorage.
// Values are JSON-encoded so the hook works for booleans, strings, enums,
// and small objects alike. Falls back to defaultValue on SSR, when the
// key is missing, or when the stored value fails to parse.
export function useLocalStorageState<T>(
  key: string,
  defaultValue: T,
): [T, (value: T) => void] {
  const [value, setValue] = useState<T>(() => {
    if (typeof window === "undefined") return defaultValue;
    try {
      const stored = window.localStorage.getItem(key);
      return stored === null ? defaultValue : (JSON.parse(stored) as T);
    } catch {
      return defaultValue;
    }
  });

  useEffect(() => {
    if (typeof window === "undefined") return;
    try {
      window.localStorage.setItem(key, JSON.stringify(value));
    } catch {
      // Ignore quota / permissions errors — persistence is best-effort.
    }
  }, [key, value]);

  return [value, setValue];
}
