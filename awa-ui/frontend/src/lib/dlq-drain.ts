/** Rows the DLQ page retries per request, matching `awa dlq retry-bulk`. */
export const DLQ_RETRY_BATCH_SIZE = 1000;

export type DlqDrainOutcome =
  | { kind: "done"; revived: number }
  | { kind: "stopped"; revived: number }
  | { kind: "failed"; revived: number; error: string };

/**
 * Retry DLQ rows one bounded batch at a time until a batch revives nothing,
 * `shouldStop` returns true, or a batch fails. A failed batch (such as a
 * unique conflict) revived nothing, and its error message is returned.
 */
export async function drainDlqRetry(
  retryBatch: (batchSize: number) => Promise<{ count: number }>,
  options: {
    batchSize?: number;
    shouldStop: () => boolean;
    onProgress: (revived: number) => void;
  }
): Promise<DlqDrainOutcome> {
  const batchSize = options.batchSize ?? DLQ_RETRY_BATCH_SIZE;
  let revived = 0;
  while (!options.shouldStop()) {
    let count: number;
    try {
      ({ count } = await retryBatch(batchSize));
    } catch (error) {
      return {
        kind: "failed",
        revived,
        error: error instanceof Error ? error.message : String(error),
      };
    }
    if (count === 0) return { kind: "done", revived };
    revived += count;
    options.onProgress(revived);
  }
  return { kind: "stopped", revived };
}
