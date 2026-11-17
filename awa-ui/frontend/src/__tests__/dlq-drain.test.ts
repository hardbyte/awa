import { describe, it, expect } from "vitest";
import { drainDlqRetry, DLQ_RETRY_BATCH_SIZE } from "@/lib/dlq-drain";

function batches(...results: Array<number | Error>) {
  const sizes: number[] = [];
  const retryBatch = async (batchSize: number) => {
    sizes.push(batchSize);
    const next = results.shift();
    if (next instanceof Error) throw next;
    return { count: next ?? 0 };
  };
  return { retryBatch, sizes };
}

describe("drainDlqRetry", () => {
  it("sends batches until one revives nothing", async () => {
    const { retryBatch, sizes } = batches(1000, 1000, 250, 0);
    const progress: number[] = [];
    const outcome = await drainDlqRetry(retryBatch, {
      shouldStop: () => false,
      onProgress: (revived) => progress.push(revived),
    });
    expect(outcome).toEqual({ kind: "done", revived: 2250 });
    expect(progress).toEqual([1000, 2000, 2250]);
    expect(sizes).toEqual(Array(4).fill(DLQ_RETRY_BATCH_SIZE));
  });

  it("stops between batches when asked", async () => {
    const { retryBatch, sizes } = batches(5, 5, 5);
    let stop = false;
    const outcome = await drainDlqRetry(retryBatch, {
      batchSize: 5,
      shouldStop: () => stop,
      onProgress: (revived) => {
        if (revived >= 10) stop = true;
      },
    });
    expect(outcome).toEqual({ kind: "stopped", revived: 10 });
    expect(sizes).toEqual([5, 5]);
  });

  it("stops on a failed batch and keeps its message", async () => {
    const conflict = new Error(
      "DLQ job 42 conflicts with a live unique claim; its batch was not retried"
    );
    const { retryBatch } = batches(3, conflict, 3);
    const outcome = await drainDlqRetry(retryBatch, {
      batchSize: 3,
      shouldStop: () => false,
      onProgress: () => {},
    });
    expect(outcome).toEqual({
      kind: "failed",
      revived: 3,
      error: conflict.message,
    });
  });

  it("waits while every matching row is held, then carries on", async () => {
    const results: Array<{ count: number; busy?: boolean }> = [
      { count: 0, busy: true },
      { count: 0, busy: true },
      { count: 4 },
      { count: 0 },
    ];
    const slept: number[] = [];
    const outcome = await drainDlqRetry(async () => results.shift()!, {
      shouldStop: () => false,
      onProgress: () => {},
      sleep: async (ms) => {
        slept.push(ms);
      },
      now: () => 0,
    });
    expect(outcome).toEqual({ kind: "done", revived: 4 });
    expect(slept).toEqual([100, 200]);
  });

  it("gives up when rows stay held past the timeout", async () => {
    let clock = 0;
    const outcome = await drainDlqRetry(async () => ({ count: 0, busy: true }), {
      busyTimeoutMs: 1000,
      shouldStop: () => false,
      onProgress: () => {},
      sleep: async (ms) => {
        clock += ms;
      },
      now: () => clock,
    });
    expect(outcome.kind).toBe("failed");
    expect(outcome.revived).toBe(0);
  });
});
