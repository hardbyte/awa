/**
 * Renders job progress as a visual bar with percentage and message.
 * Progress format: { percent: number, message?: string, metadata?: object }
 */

import { JsonView } from "./JsonView";

interface ProgressDisplayProps {
  progress: unknown;
}

export function ProgressDisplay({ progress }: ProgressDisplayProps) {
  if (progress == null || typeof progress !== "object") return null;

  const p = progress as Record<string, unknown>;
  const percent =
    typeof p.percent === "number" ? Math.min(100, Math.max(0, p.percent)) : null;
  const message = typeof p.message === "string" ? p.message : null;
  const metadata = p.metadata != null ? p.metadata : null;

  return (
    <div className="space-y-3">
      {/* Progress bar */}
      {percent != null && (
        <div className="space-y-1.5">
          <div className="flex items-baseline justify-between">
            <span className="text-sm font-medium tabular-nums">
              {percent}%
            </span>
            {message && (
              <span className="text-sm text-muted-fg">{message}</span>
            )}
          </div>
          <div className="h-2.5 w-full overflow-hidden rounded-full bg-secondary">
            <div
              className="h-full rounded-full bg-primary transition-all duration-500"
              style={{ width: `${percent}%` }}
            />
          </div>
        </div>
      )}

      {/* Message without percent */}
      {percent == null && message && (
        <p className="text-sm text-muted-fg">{message}</p>
      )}

      {/* Progress metadata */}
      {metadata != null && (
        <div>
          <p className="mb-1 text-xs font-medium text-muted-fg">
            Progress metadata
          </p>
          <JsonView data={metadata} />
        </div>
      )}
    </div>
  );
}
