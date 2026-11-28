import { Badge, type BadgeProps } from "@/components/ui/badge";

const STATE_INTENT_MAP: Record<string, BadgeProps["intent"]> = {
  available: "info",
  scheduled: "secondary",
  running: "success",
  completed: "success",
  retryable: "warning",
  failed: "danger",
  cancelled: "secondary",
  waiting_external: "outline",
};

// Custom styling for waiting_external (indigo/purple per design spec)
const WAITING_EXTERNAL_CLASS =
  "[--badge-bg:oklch(0.87_0.07_280)] [--badge-fg:oklch(0.45_0.15_280)] [--badge-border:transparent] dark:[--badge-bg:oklch(0.55_0.15_280/0.2)] dark:[--badge-fg:oklch(0.82_0.1_280)]";

interface StateBadgeProps {
  state: string;
}

export function StateBadge({ state }: StateBadgeProps) {
  const intent = STATE_INTENT_MAP[state] ?? "secondary";
  const className = state === "waiting_external" ? WAITING_EXTERNAL_CLASS : undefined;
  return (
    <Badge intent={intent} className={className}>
      {state}
    </Badge>
  );
}
