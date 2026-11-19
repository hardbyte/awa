import { describe, it, expect } from "vitest";
import { runBulk, describeBulkRun, BULK_BATCH_SIZE } from "@/lib/bulk-run";
import type { BulkBatchResult, BulkCursor } from "@/lib/bulk-run";

function batch(
  count: number,
  extra: Partial<BulkBatchResult> = {}
): BulkBatchResult {
  return { count, skipped: 0, conflicts: [], done: false, ...extra };
}

function scripted(results: BulkBatchResult[]) {
  const calls: Array<{ size: number; cursor?: BulkCursor }> = [];
  const runBatch = async (size: number, cursor?: BulkCursor) => {
    calls.push({ size, cursor });
    return results.shift() ?? batch(0, { done: true });
  };
  return { runBatch, calls };
}

describe("runBulk", () => {
  it("sends batches, passing each cursor on, until one reports done", async () => {
    const { runBatch, calls } = scripted([
      batch(1000, { cursor: { stage: 0, after_job_id: 1000 } }),
      batch(250, { done: true, cursor: { stage: 0, after_job_id: 1250 } }),
    ]);
    const progress: number[] = [];
    const outcome = await runBulk(runBatch, {
      shouldStop: () => false,
      onProgress: (p) => progress.push(p.count),
    });
    expect(outcome).toMatchObject({ kind: "done", count: 1250, skipped: 0 });
    expect(progress).toEqual([1000, 1250]);
    expect(calls).toEqual([
      { size: BULK_BATCH_SIZE, cursor: undefined },
      { size: BULK_BATCH_SIZE, cursor: { stage: 0, after_job_id: 1000 } },
    ]);
  });

  it("collects skipped rows and carries on", async () => {
    const { runBatch } = scripted([
      batch(2, { skipped: 1, conflicts: [7] }),
      batch(3, { skipped: 1, conflicts: [11], done: true }),
    ]);
    const outcome = await runBulk(runBatch, {
      shouldStop: () => false,
      onProgress: () => {},
    });
    expect(outcome).toMatchObject({
      kind: "done",
      count: 5,
      skipped: 2,
      conflicts: [7, 11],
    });
    expect(describeBulkRun("Retried", "DLQ row(s)", outcome)).toBe(
      "Retried 5 DLQ row(s); skipped 2 whose unique key a live job holds (7, 11)"
    );
  });

  it("stops between batches when asked", async () => {
    const { runBatch, calls } = scripted([batch(5), batch(5), batch(5)]);
    let stop = false;
    const outcome = await runBulk(runBatch, {
      batchSize: 5,
      shouldStop: () => stop,
      onProgress: (p) => {
        if (p.count >= 10) stop = true;
      },
    });
    expect(outcome).toMatchObject({ kind: "stopped", count: 10 });
    expect(calls).toHaveLength(2);
  });

  it("waits while every matching row is held, then carries on", async () => {
    const { runBatch } = scripted([
      batch(0, { busy: true }),
      batch(0, { busy: true }),
      batch(4, { done: true }),
    ]);
    const slept: number[] = [];
    const outcome = await runBulk(runBatch, {
      shouldStop: () => false,
      onProgress: () => {},
      sleep: async (ms) => {
        slept.push(ms);
      },
      now: () => 0,
    });
    expect(outcome).toMatchObject({ kind: "done", count: 4 });
    expect(slept).toEqual([100, 200]);
  });

  it("gives up when rows stay held past the timeout", async () => {
    let clock = 0;
    const outcome = await runBulk(async () => batch(0, { busy: true }), {
      busyTimeoutMs: 1000,
      shouldStop: () => false,
      onProgress: () => {},
      sleep: async (ms) => {
        clock += ms;
      },
      now: () => clock,
    });
    expect(outcome.kind).toBe("failed");
  });

  it("stops on a failed request and keeps its message", async () => {
    const outcome = await runBulk(
      async () => {
        throw new Error("boom");
      },
      { shouldStop: () => false, onProgress: () => {} }
    );
    expect(outcome).toMatchObject({ kind: "failed", count: 0, error: "boom" });
  });
});
