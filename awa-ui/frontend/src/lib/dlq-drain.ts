/** Rows the DLQ page retries per request, matching `awa dlq retry-bulk`. */
export const DLQ_RETRY_BATCH_SIZE = 1000;

/** How long the loop waits for rows another transaction holds. */
export const DLQ_RETRY_BUSY_TIMEOUT_MS = 60_000;

export type DlqDrainOutcome =
  | { kind: "done"; revived: number }
  | { kind: "stopped"; revived: number }
  | { kind: "failed"; revived: number; error: string };

export interface DlqBatchResult {
  count: number;
  busy?: boolean;
}

/**
 * Retry DLQ rows one bounded batch at a time until a batch revives nothing,
 * `shouldStop` returns true, or a batch fails. A failed batch (such as a
 * unique conflict) revived nothing, and its error message is returned. A
 * busy batch, where every matching row is held by another transaction, is
 * retried with backoff until `busyTimeoutMs` passes without progress.
 */
export async function drainDlqRetry(
  retryBatch: (batchSize: number) => Promise<DlqBatchResult>,
  options: {
    batchSize?: number;
    busyTimeoutMs?: number;
    shouldStop: () => boolean;
    onProgress: (revived: number) => void;
    sleep?: (ms: number) => Promise<void>;
    now?: () => number;
  }
): Promise<DlqDrainOutcome> {
  const batchSize = options.batchSize ?? DLQ_RETRY_BATCH_SIZE;
  const busyTimeoutMs = options.busyTimeoutMs ?? DLQ_RETRY_BUSY_TIMEOUT_MS;
  const sleep =
    options.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
  const now = options.now ?? Date.now;
  let revived = 0;
  let busySince: number | undefined;
  let busyWaitMs = 100;
  while (!options.shouldStop()) {
    let result: DlqBatchResult;
    try {
      result = await retryBatch(batchSize);
    } catch (error) {
      return {
        kind: "failed",
        revived,
        error: error instanceof Error ? error.message : String(error),
      };
    }
    if (result.busy) {
      busySince ??= now();
      if (now() - busySince >= busyTimeoutMs) {
        return {
          kind: "failed",
          revived,
          error: `every remaining matching row has been held by another transaction for ${Math.round(busyTimeoutMs / 1000)}s`,
        };
      }
      await sleep(busyWaitMs);
      busyWaitMs = Math.min(busyWaitMs * 2, 2000);
      continue;
    }
    if (result.count === 0) return { kind: "done", revived };
    busySince = undefined;
    busyWaitMs = 100;
    revived += result.count;
    options.onProgress(revived);
  }
  return { kind: "stopped", revived };
}
