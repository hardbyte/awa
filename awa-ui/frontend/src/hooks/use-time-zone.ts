import { useSyncExternalStore } from "react";
import type { TimeZonePreference } from "@/lib/time";

const STORAGE_KEY = "time-zone";
const listeners = new Set<() => void>();

function read(): TimeZonePreference {
  if (typeof window === "undefined") return "local";
  try {
    return window.localStorage.getItem(STORAGE_KEY) === "\"UTC\"" ? "UTC" : "local";
  } catch {
    return "local";
  }
}

let current: TimeZonePreference = read();

export function setTimeZonePreference(value: TimeZonePreference) {
  current = value;
  try {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(value));
  } catch {
    // Persistence is best-effort.
  }
  for (const listener of listeners) listener();
}

function subscribe(listener: () => void) {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

/** The zone every timestamp is shown in, shared across the app. */
export function useTimeZone(): [TimeZonePreference, (value: TimeZonePreference) => void] {
  const value = useSyncExternalStore(subscribe, () => current, () => "local" as const);
  return [value, setTimeZonePreference];
}
