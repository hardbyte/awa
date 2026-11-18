import { useEffect, useRef, useState } from "react";
import { Button } from "@/components/ui/button";
import { toast } from "@/components/ui/toast";
import { runBulk, describeBulkRun } from "@/lib/bulk-run";
import type { BulkBatchResult, BulkCursor, BulkRunOutcome } from "@/lib/bulk-run";

export interface BulkRunState {
  label: string;
  count: number;
  total?: number;
  totalCapped?: boolean;
  stopping: boolean;
}

/**
 * Run a bulk action in bounded batches, one at a time per page. The run stops
 * between batches when asked or when the page unmounts.
 */
export function useBulkRun() {
  const [state, setState] = useState<BulkRunState | null>(null);
  const stop = useRef(false);
  useEffect(
    () => () => {
      stop.current = true;
    },
    []
  );

  const start = async (options: {
    label: string;
    verb: string;
    noun: string;
    total?: number;
    totalCapped?: boolean;
    runBatch: (batchSize: number, cursor?: BulkCursor) => Promise<BulkBatchResult>;
    onFinish?: (outcome: BulkRunOutcome) => void;
  }) => {
    stop.current = false;
    setState({
      label: options.label,
      count: 0,
      total: options.total,
      totalCapped: options.totalCapped,
      stopping: false,
    });
    const outcome = await runBulk(options.runBatch, {
      shouldStop: () => stop.current,
      onProgress: (progress) =>
        setState((current) => current && { ...current, count: progress.count }),
    });
    setState(null);
    options.onFinish?.(outcome);
    const message = describeBulkRun(options.verb, options.noun, outcome);
    if (outcome.kind === "failed" || outcome.skipped > 0 || outcome.leftAboveSequence > 0) {
      toast.error(message);
    } else {
      toast.success(message);
    }
    return outcome;
  };

  const requestStop = () => {
    stop.current = true;
    setState((current) => current && { ...current, stopping: true });
  };

  return { state, start, requestStop };
}

export function BulkRunProgress({
  state,
  onStop,
}: {
  state: BulkRunState | null;
  onStop: () => void;
}) {
  if (!state) return null;
  const total =
    state.total !== undefined
      ? ` of ${state.total.toLocaleString()}${state.totalCapped ? "+" : ""}`
      : "";
  return (
    <div role="status" className="flex items-center gap-3 text-sm">
      <span>
        {state.label}: {state.count.toLocaleString()}
        {total} done
      </span>
      <Button intent="outline" size="xs" onPress={onStop} isDisabled={state.stopping}>
        {state.stopping ? "Stopping" : "Stop"}
      </Button>
    </div>
  );
}
