import { describe, it, expect } from "vitest";
import type {
  HealthReport,
  JobRow,
  QueueAssessment,
  QueueHistory,
  QueueStats,
} from "@/lib/api";
import {
  eventIndexes,
  formatDuration,
  formatRate,
  groupFailures,
  joinQueues,
  totalBacklog,
  totalRate,
} from "@/lib/operations";

function queue(name: string, available = 10): QueueStats {
  return {
    queue: name,
    display_name: null,
    description: null,
    owner: null,
    docs_url: null,
    tags: [],
    extra: {},
    descriptor_last_seen_at: null,
    descriptor_stale: false,
    descriptor_mismatch: false,
    total_queued: available,
    scheduled: 0,
    available,
    retryable: 0,
    running: 1,
    failed: 0,
    waiting_external: 0,
    completed_last_hour: 600,
    lag_seconds: 5,
    paused: false,
  };
}

function assessment(
  name: string,
  rank: number,
  flow: Partial<QueueAssessment["flow"]> = {},
  liveWorkers = 1,
): QueueAssessment {
  return {
    queue: name,
    health: "steady",
    label: "Steady",
    reason: "Backlog holding level",
    needs_attention: false,
    rank,
    limited_by: null,
    trend: null,
    clear_seconds: null,
    flow: {
      claimed_per_minute: null,
      completed_per_minute: null,
      failed_per_minute: null,
      retried_per_minute: null,
      arrivals_per_minute: null,
      mean_claim_wait_seconds: null,
      rate_limited: false,
      ...flow,
    },
    live_workers: liveWorkers,
    in_flight: 0,
    capacity: null,
  };
}

function report(queues: QueueAssessment[]): HealthReport {
  return {
    status: "ok",
    title: "Keeping up",
    detail: "",
    generated_at: "2026-10-25T00:00:00Z",
    history_available: true,
    history_since: null,
    queues,
  };
}

function history(
  queues: Record<string, (number | null)[]>,
  events: QueueHistory["events"] = [],
): QueueHistory {
  const length = Object.values(queues)[0]?.length ?? 3;
  const empty = () => Array.from({ length }, () => null);
  return {
    installed: true,
    bucket_secs: 60,
    minutes: length,
    observed_since: null,
    buckets: Array.from(
      { length },
      (_, index) => `2026-10-25T00:0${index}:00Z`,
    ),
    queues: Object.fromEntries(
      Object.entries(queues).map(([name, available]) => [
        name,
        {
          available,
          running: empty(),
          lag_seconds: empty(),
          claimed: empty(),
          completed: empty(),
          failed_attempts: empty(),
          rate_limited: empty(),
        },
      ]),
    ),
    fleet: { live_instances: empty(), healthy_instances: empty() },
    events,
  };
}

describe("joinQueues", () => {
  it("follows the report's risk order and appends queues it does not know", () => {
    const rows = joinQueues(
      [queue("a"), queue("new"), queue("b")],
      report([assessment("b", 0), assessment("a", 6)]),
      history({ a: [1, 2, 3] }),
    );
    expect(rows.map((row) => row.queue.queue)).toEqual(["b", "a", "new"]);
    expect(rows[1]?.series?.available).toEqual([1, 2, 3]);
    expect(rows[2]?.assessment).toBeUndefined();
  });
});

describe("totalBacklog", () => {
  it("sums per bucket and keeps buckets with no samples null", () => {
    expect(
      totalBacklog(history({ a: [null, 1, 2], b: [null, null, 3] })),
    ).toEqual([null, 1, 5]);
  });
});

describe("totalRate", () => {
  it("sums known rates", () => {
    const total = totalRate(
      report([
        assessment("a", 0, { completed_per_minute: 10 }),
        assessment("b", 0, { completed_per_minute: 5 }),
      ]),
      (flow) => flow.completed_per_minute,
    );
    expect(total).toBe(15);
  });

  it("refuses a partial total while a served queue is unknown", () => {
    const total = totalRate(
      report([
        assessment("a", 0, { completed_per_minute: 10 }),
        assessment("b", 0, {}, 2),
      ]),
      (flow) => flow.completed_per_minute,
    );
    expect(total).toBeNull();
  });

  it("ignores unknown rates on queues nobody serves", () => {
    const total = totalRate(
      report([
        assessment("a", 0, { completed_per_minute: 10 }),
        assessment("orphan", 0, {}, 0),
      ]),
      (flow) => flow.completed_per_minute,
    );
    expect(total).toBe(10);
  });
});

describe("eventIndexes", () => {
  it("maps fleet events onto bucket positions", () => {
    const indexes = eventIndexes(
      history({ a: [1, 2, 3] }, [
        { at: "2026-10-25T00:02:00Z", kind: "version_change", detail: "" },
        { at: "2026-10-24T23:00:00Z", kind: "leader_change", detail: "" },
      ]),
    );
    expect(indexes).toEqual([2]);
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
