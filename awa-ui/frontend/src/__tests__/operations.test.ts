import { describe, it, expect } from "vitest";
import type { JobRow, QueueHistory, QueueSeries, QueueStats } from "@/lib/api";
import {
  assessQueue,
  assessQueues,
  formatDuration,
  formatRate,
  groupFailures,
  overallVerdict,
  totalBacklog,
  trailingTrend,
} from "@/lib/operations";

function queue(overrides: Partial<QueueStats> = {}): QueueStats {
  return {
    queue: "email",
    display_name: null,
    description: null,
    owner: null,
    docs_url: null,
    tags: [],
    extra: {},
    descriptor_last_seen_at: null,
    descriptor_stale: false,
    descriptor_mismatch: false,
    total_queued: 0,
    scheduled: 0,
    available: 1000,
    retryable: 0,
    running: 10,
    failed: 0,
    waiting_external: 0,
    completed_last_hour: 600,
    lag_seconds: 5,
    paused: false,
    ...overrides,
  };
}

function series(available: (number | null)[]): QueueSeries {
  return {
    available,
    running: available.map(() => 10),
    lag_seconds: available.map(() => 5),
  };
}

function history(
  queues: Record<string, (number | null)[]>,
  enabled = true,
): QueueHistory {
  const length = Object.values(queues)[0]?.length ?? 0;
  return {
    enabled,
    window_secs: 3600,
    sample_interval_secs: 15,
    bucket_secs: 60,
    observed_since: "2026-10-25T00:00:00Z",
    buckets: Array.from(
      { length },
      (_, index) => `2026-10-25T00:${String(index).padStart(2, "0")}:00Z`,
    ),
    queues: Object.fromEntries(
      Object.entries(queues).map(([name, values]) => [name, series(values)]),
    ),
  };
}

describe("trailingTrend", () => {
  it("needs three samples spanning two minutes", () => {
    expect(trailingTrend([null, null, 10, 20], 60)).toBeNull();
    expect(trailingTrend([10, 20, 30], 30)).toBeNull();
    expect(trailingTrend([10, 20, 30], 60)).not.toBeNull();
  });

  it("fits the slope per minute over the trailing window only", () => {
    // An old spike outside the 5 minute window must not affect the fit.
    const trend = trailingTrend([9000, 0, 100, 200, 300, 400, 500], 60)!;
    expect(trend.perMinute).toBeCloseTo(100);
    expect(trend.minutes).toBe(5);
    expect(trend.change).toBeCloseTo(500);
  });

  it("skips gaps instead of treating them as zero", () => {
    const trend = trailingTrend([100, null, 300, null, 500, 600], 60)!;
    expect(trend.perMinute).toBeCloseTo(100);
  });
});

describe("assessQueue", () => {
  it("flags waiting work with nothing running as stalled before anything else", () => {
    const result = assessQueue(
      queue({ running: 0, lag_seconds: 90 }),
      series([100, 200, 300, 400, 500, 600]),
      60,
    );
    expect(result.health).toBe("stalled");
    expect(result.needsAttention).toBe(true);
  });

  it("does not call a fresh queue stalled before the wait threshold", () => {
    const result = assessQueue(
      queue({ running: 0, lag_seconds: 10 }),
      undefined,
      60,
    );
    expect(result.health).toBe("unknown");
  });

  it("reports a materially growing backlog as falling behind", () => {
    const result = assessQueue(
      queue({ available: 1500 }),
      series([1000, 1100, 1200, 1300, 1400, 1500]),
      60,
    );
    expect(result.health).toBe("behind");
    expect(result.detail).toContain("+100/min");
  });

  it("treats small wobble on a large backlog as steady", () => {
    const result = assessQueue(
      queue({ available: 184_000 }),
      series([184_000, 184_400, 183_900, 184_300, 184_100, 184_000]),
      60,
    );
    expect(result.health).toBe("steady");
    expect(result.needsAttention).toBe(false);
  });

  it("estimates time to clear a draining backlog", () => {
    const result = assessQueue(
      queue({ available: 600 }),
      series([1600, 1400, 1200, 1000, 800, 600]),
      60,
    );
    expect(result.health).toBe("draining");
    expect(result.clearSeconds).toBeCloseTo(180);
    expect(result.detail).toBe("Clears in about 3m 0s at this pace");
  });

  it("flags a long wait even when depth holds", () => {
    const result = assessQueue(
      queue({ lag_seconds: 900 }),
      series([1000, 1000, 1000, 1000]),
      60,
    );
    expect(result.health).toBe("slow");
  });

  it("only asks for attention on paused queues holding work", () => {
    expect(
      assessQueue(queue({ paused: true }), undefined, 60).needsAttention,
    ).toBe(true);
    const empty = assessQueue(
      queue({ paused: true, available: 0 }),
      undefined,
      60,
    );
    expect(empty.needsAttention).toBe(false);
    expect(empty.rank).toBeGreaterThan(
      assessQueue(queue(), undefined, 60).rank,
    );
  });

  it("says why there is no trend when history is disabled", () => {
    expect(assessQueue(queue(), undefined, 60, false).detail).toMatch(
      /disabled/,
    );
  });
});

describe("assessQueues and overallVerdict", () => {
  it("orders by risk, then backlog", () => {
    const rows = assessQueues(
      [
        queue({ queue: "steady", available: 9000 }),
        queue({ queue: "stalled", running: 0, lag_seconds: 120 }),
        queue({ queue: "idle", available: 0 }),
        queue({ queue: "behind", available: 500 }),
      ],
      history({
        steady: [9000, 9000, 9000, 9000],
        stalled: [1000, 1000, 1000, 1000],
        idle: [0, 0, 0, 0],
        behind: [100, 200, 300, 400, 500],
      }),
    );
    expect(rows.map((row) => row.queue.queue)).toEqual([
      "stalled",
      "behind",
      "steady",
      "idle",
    ]);
    const verdict = overallVerdict(rows, true);
    expect(verdict.tone).toBe("danger");
    expect(verdict.title).toBe("1 queue not being worked");
    expect(verdict.detail).toContain("1 queue falling behind");
  });

  it("is honest when there is not enough history to judge", () => {
    const rows = assessQueues([queue()], null);
    expect(overallVerdict(rows, false).title).toBe("Not enough history yet");
  });

  it("reports keeping up when nothing is wrong", () => {
    const rows = assessQueues(
      [queue({ available: 0 })],
      history({ email: [0, 0, 0] }),
    );
    expect(overallVerdict(rows, true)).toMatchObject({
      tone: "success",
      title: "Keeping up",
    });
  });
});

describe("totalBacklog", () => {
  it("sums per bucket and keeps buckets with no samples null", () => {
    expect(
      totalBacklog(history({ a: [null, 1, 2], b: [null, null, 3] })),
    ).toEqual([null, 1, 5]);
  });
});

describe("groupFailures", () => {
  function failed(
    id: number,
    message: string,
    finalized: string,
    kind = "SendEmail",
  ): JobRow {
    return {
      id,
      kind,
      queue: "email",
      errors: [{ error: message }],
      finalized_at: finalized,
    } as unknown as JobRow;
  }

  it("groups by kind, queue and error, masking per-job identifiers", () => {
    const groups = groupFailures([
      failed(1, "timed out calling order 18273645", "2026-10-25T00:00:01Z"),
      failed(2, "timed out calling order 99887766", "2026-10-25T00:00:03Z"),
      failed(
        3,
        "timed out calling order 12345678",
        "2026-10-25T00:00:02Z",
        "Other",
      ),
    ]);
    expect(groups).toHaveLength(2);
    expect(groups[0]).toMatchObject({ kind: "SendEmail", count: 2 });
    expect(groups[0]!.latest.id).toBe(2);
    expect(groups[0]!.message).toBe("timed out calling order 99887766");
  });
});

describe("formatting", () => {
  it("formats durations consistently across units", () => {
    expect(formatDuration(2.4)).toBe("2s");
    expect(formatDuration(458)).toBe("7m 38s");
    expect(formatDuration(3720)).toBe("1h 2m");
    expect(formatDuration(null)).toBe("—");
  });

  it("formats signed rates", () => {
    expect(formatRate(1234, true)).toBe("+1,234/min");
    expect(formatRate(-0.5, true)).toBe("−0.5/min");
    expect(formatRate(52_000)).toBe("52K/min");
  });
});
