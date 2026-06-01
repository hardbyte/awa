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
  const totalSeconds = Math.max(0, Math.floor((nowMs - startMs) / 1000));
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
