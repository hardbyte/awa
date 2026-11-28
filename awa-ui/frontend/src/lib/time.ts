/** Relative time display (e.g. "3m ago", "2h ago") */
export function timeAgo(dateStr: string): string {
  const seconds = Math.floor(
    (Date.now() - new Date(dateStr).getTime()) / 1000
  );
  if (seconds < 0) return "just now";
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

/** Relative future time display (e.g. "in 23m", "in 2h") */
export function timeUntil(dateStr: string): string {
  const seconds = Math.floor(
    (new Date(dateStr).getTime() - Date.now()) / 1000
  );
  if (seconds < 0) return "overdue";
  if (seconds < 60) return `in ${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `in ${minutes}m`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `in ${hours}h`;
  const days = Math.floor(hours / 24);
  return `in ${days}d`;
}

/**
 * Compact duration since `dateStr`, sized for operator displays where
 * "how long" is the question (e.g. "in `prepared` for 2h 14m"). Differs
 * from `timeAgo` in two ways: no "ago" suffix, and the largest unit is
 * always paired with the next-largest non-zero unit so a long transition
 * reads like "2h 14m" rather than "2h".
 *
 * `nowMs` is injectable for deterministic tests.
 */
export function durationSince(dateStr: string, nowMs: number = Date.now()): string {
  const startMs = new Date(dateStr).getTime();
  if (!Number.isFinite(startMs)) return "—";
  return formatDuration((nowMs - startMs) / 1000);
}

/**
 * Compact duration for a number of seconds, pairing the largest unit with
 * the next non-zero one: "45s", "25m 25s", "2h 14m", "3d 4h".
 */
export function formatDuration(seconds: number): string {
  const totalSeconds = Math.max(0, Math.floor(seconds));
  if (totalSeconds < 60) return `${totalSeconds}s`;
  const totalMinutes = Math.floor(totalSeconds / 60);
  if (totalMinutes < 60) {
    const secs = totalSeconds % 60;
    return secs > 0 ? `${totalMinutes}m ${secs}s` : `${totalMinutes}m`;
  }
  const totalHours = Math.floor(totalMinutes / 60);
  if (totalHours < 24) {
    const mins = totalMinutes % 60;
    return mins > 0 ? `${totalHours}h ${mins}m` : `${totalHours}h`;
  }
  const days = Math.floor(totalHours / 24);
  const hours = totalHours % 24;
  return hours > 0 ? `${days}d ${hours}h` : `${days}d`;
}

/** Compact duration between two timestamps, "—" when either is missing. */
export function durationBetween(
  start: string | null | undefined,
  end: string | null | undefined
): string {
  if (!start || !end) return "—";
  const startMs = new Date(start).getTime();
  const endMs = new Date(end).getTime();
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) return "—";
  return formatDuration((endMs - startMs) / 1000);
}

/** "3m ago" for the past, "in 3m" for the future. */
export function formatRelative(dateStr: string, nowMs: number = Date.now()): string {
  const deltaSeconds = (new Date(dateStr).getTime() - nowMs) / 1000;
  if (!Number.isFinite(deltaSeconds)) return "—";
  if (Math.abs(deltaSeconds) < 1) return "just now";
  const amount = formatDuration(Math.abs(deltaSeconds)).split(" ")[0];
  return deltaSeconds < 0 ? `${amount} ago` : `in ${amount}`;
}

/** "local" follows the browser; anything else is an IANA zone such as "UTC". */
export type TimeZonePreference = "local" | "UTC";

/**
 * A full timestamp with its zone, e.g. "28 Nov 2026, 14:03:05 UTC". `tz` is
 * "local", "UTC" or any IANA zone name.
 */
export function formatAbsolute(dateStr: string, tz: string = "local"): string {
  const date = new Date(dateStr);
  if (!Number.isFinite(date.getTime())) return "—";
  const options: Intl.DateTimeFormatOptions = {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
    timeZoneName: "short",
  };
  try {
    return date.toLocaleString(undefined, tz === "local" ? options : { ...options, timeZone: tz });
  } catch {
    return date.toLocaleString(undefined, options);
  }
}

/** Shown relative within this window, absolute beyond it. */
export const RELATIVE_WINDOW_MS = 24 * 3600_000;

/** Format a UTC timestamp in a specific IANA timezone */
export function formatInTimezone(dateStr: string, timezone: string): string {
  try {
    return new Date(dateStr).toLocaleString(undefined, {
      timeZone: timezone,
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
      timeZoneName: "short",
    });
  } catch {
    // Fallback if timezone is invalid
    return new Date(dateStr).toLocaleString();
  }
}
