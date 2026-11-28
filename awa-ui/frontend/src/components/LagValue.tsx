import { formatDuration } from "@/lib/time";
import { LAG_ATTENTION_SECONDS } from "@/lib/constants";

/** How long a queue's oldest ready job has waited; amber once that needs attention. */
export function LagValue({ seconds }: { seconds: number | null }) {
  if (seconds == null) return <span className="text-muted-fg">—</span>;
  return (
    <span className={seconds >= LAG_ATTENTION_SECONDS ? "text-warning-subtle-fg" : undefined}>
      {seconds < 10 ? `${seconds.toFixed(1)}s` : formatDuration(seconds)}
    </span>
  );
}
