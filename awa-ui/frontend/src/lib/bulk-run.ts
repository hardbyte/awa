/** Rows a bulk action changes per request, matching the CLI's `--batch-size`. */
export const BULK_BATCH_SIZE = 1000;

/** How long a run waits for rows another transaction holds. */
export const BULK_BUSY_TIMEOUT_MS = 60_000;

export interface BulkCursor {
  stage: number;
  after_job_id: number;
  /** The run covers job IDs below this and below `sequence_bound`. */
  before_job_id?: number;
  /** A second bound on the run's job IDs. */
  sequence_bound?: number;
  /** Ignored by the server; an older server sets it. */
  started_at?: string | null;
}

export interface BulkBatchResult {
  count: number;
  skipped: number;
  conflicts: number[];
  busy?: boolean;
  done: boolean;
  cursor?: BulkCursor;
  /** Matching rows with an explicit job ID above the job ID sequence that the run leaves alone, because the database role may not advance the sequence. An older server omits it. */
  left_above_sequence?: number;
}

export interface BulkRunProgress {
  count: number;
  skipped: number;
  conflicts: number[];
  leftAboveSequence: number;
}

export type BulkRunOutcome = BulkRunProgress &
  ({ kind: "done" } | { kind: "stopped" } | { kind: "failed"; error: string });

const MAX_CONFLICTS = 10;

/**
 * Send bounded batches, each resuming at the previous batch's cursor, until a
 * batch reports no matching row left, `shouldStop` returns true, or a request
 * fails. Rows skipped over a unique conflict are counted and their job IDs
 * kept. A busy batch, where every remaining matching row is held by another
 * transaction, is retried with backoff until `busyTimeoutMs` passes without
 * progress.
 */
export async function runBulk(
  runBatch: (batchSize: number, cursor?: BulkCursor) => Promise<BulkBatchResult>,
  options: {
    batchSize?: number;
    busyTimeoutMs?: number;
    shouldStop: () => boolean;
    onProgress: (progress: BulkRunProgress) => void;
    sleep?: (ms: number) => Promise<void>;
    now?: () => number;
  }
): Promise<BulkRunOutcome> {
  const batchSize = options.batchSize ?? BULK_BATCH_SIZE;
  const busyTimeoutMs = options.busyTimeoutMs ?? BULK_BUSY_TIMEOUT_MS;
  const sleep =
    options.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
  const now = options.now ?? Date.now;
  const progress: BulkRunProgress = { count: 0, skipped: 0, conflicts: [], leftAboveSequence: 0 };
  let cursor: BulkCursor | undefined;
  let busySince: number | undefined;
  let busyWaitMs = 100;
  while (!options.shouldStop()) {
    let result: BulkBatchResult;
    try {
      result = await runBatch(batchSize, cursor);
    } catch (error) {
      return {
        ...progress,
        kind: "failed",
        error: error instanceof Error ? error.message : String(error),
      };
    }
    cursor = result.cursor ?? cursor;
    progress.count += result.count;
    progress.skipped += result.skipped;
    progress.leftAboveSequence += result.left_above_sequence ?? 0;
    for (const id of result.conflicts) {
      if (progress.conflicts.length < MAX_CONFLICTS) progress.conflicts.push(id);
    }
    if (result.count > 0 || result.skipped > 0) {
      options.onProgress({ ...progress, conflicts: [...progress.conflicts] });
    }
    if (result.done) return { ...progress, kind: "done" };
    if (result.busy) {
      busySince ??= now();
      if (now() - busySince >= busyTimeoutMs) {
        return {
          ...progress,
          kind: "failed",
          error: `every remaining matching row has been held by another transaction for ${Math.round(busyTimeoutMs / 1000)}s`,
        };
      }
      await sleep(busyWaitMs);
      busyWaitMs = Math.min(busyWaitMs * 2, 2000);
    } else {
      busySince = undefined;
      busyWaitMs = 100;
    }
  }
  return { ...progress, kind: "stopped" };
}

/** A toast line for a finished run, e.g. "Retried 12 DLQ row(s)". */
export function describeBulkRun(verb: string, noun: string, outcome: BulkRunOutcome): string {
  const done = `${verb} ${outcome.count.toLocaleString()} ${noun}`;
  const skipped =
    outcome.skipped > 0
      ? `; skipped ${outcome.skipped.toLocaleString()} whose unique key a live job holds (${outcome.conflicts.join(", ")}${outcome.skipped > outcome.conflicts.length ? ", ..." : ""})`
      : "";
  const left =
    outcome.leftAboveSequence > 0
      ? `; left ${outcome.leftAboveSequence.toLocaleString()} with an explicit ID above the job ID sequence, which the database role may not advance`
      : "";
  if (outcome.kind === "failed") return `${done}${skipped}${left}, then stopped: ${outcome.error}`;
  if (outcome.kind === "stopped") return `${done} before stopping${skipped}${left}`;
  return `${done}${skipped}${left}`;
}
