import { Badge } from "@/components/ui/badge";
import { batchStateMeta, jobStateMeta, TONE_BADGE, type StateMeta, type StateTone } from "@/lib/states";

function ToneBadge({ state, meta, short, tone }: { state: string; meta: StateMeta; short?: boolean; tone?: StateTone }) {
  const { Icon } = meta;
  return (
    <Badge className={TONE_BADGE[tone ?? meta.tone]} data-state={state}>
      <Icon data-slot="icon" aria-hidden="true" />
      {short ? meta.shortLabel : meta.label}
    </Badge>
  );
}

interface StateBadgeProps {
  state: string;
  /** Use the short label ("Waiting"). */
  short?: boolean;
}

/** A job state, with its colour, icon and sentence-case label. */
export function StateBadge({ state, short }: StateBadgeProps) {
  return <ToneBadge state={state} meta={jobStateMeta(state)} short={short} />;
}

/** A batch operation state, in the job state palette. `tone` overrides the
 * colour, e.g. amber for a completed operation that changed nothing. */
export function BatchStateBadge({ state, tone }: { state: string; tone?: StateTone }) {
  return <ToneBadge state={state} meta={batchStateMeta(state)} tone={tone} />;
}
