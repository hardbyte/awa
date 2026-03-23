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
