import type { QueueOverview } from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { useNow } from "@/hooks/use-now";
import { useTimeZone } from "@/hooks/use-time-zone";
import { pausedLabel } from "@/lib/pause";
import { formatAbsolute } from "@/lib/time";

/** A paused queue's badge, saying when and by whom it was paused when known. */
export function PausedBadge({
  queue,
  "data-testid": testId,
}: {
  queue: Pick<QueueOverview, "paused_at" | "paused_by">;
  "data-testid"?: string;
}) {
  const now = useNow();
  const [tz] = useTimeZone();
  return (
    <Badge
      intent="warning"
      data-testid={testId}
      title={queue.paused_at ? formatAbsolute(queue.paused_at, tz) : undefined}
    >
      {pausedLabel(queue, now)}
    </Badge>
  );
}
