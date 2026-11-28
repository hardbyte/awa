import { useNow } from "@/hooks/use-now";
import { useTimeZone } from "@/hooks/use-time-zone";
import { formatAbsolute, formatRelative, RELATIVE_WINDOW_MS } from "@/lib/time";

interface TimeProps {
  value: string | null | undefined;
  /** "auto" is relative within a day and absolute beyond it. */
  format?: "auto" | "relative" | "absolute";
  fallback?: React.ReactNode;
  className?: string;
}

/**
 * A timestamp: relative when recent, absolute otherwise. The tooltip always
 * carries the absolute time with its zone.
 */
export function Time({ value, format = "auto", fallback = "—", className }: TimeProps) {
  const now = useNow();
  const [tz] = useTimeZone();
  if (!value || !Number.isFinite(new Date(value).getTime())) {
    return <span className={className}>{fallback}</span>;
  }
  const absolute = formatAbsolute(value, tz);
  const recent = Math.abs(new Date(value).getTime() - now) < RELATIVE_WINDOW_MS;
  const relative = format === "relative" || (format === "auto" && recent);
  return (
    <time dateTime={value} title={absolute} className={className}>
      {relative ? formatRelative(value, now) : absolute}
    </time>
  );
}
