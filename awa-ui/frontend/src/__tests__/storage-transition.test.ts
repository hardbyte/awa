import { describe, it, expect } from "vitest";
import { durationSince } from "@/lib/time";
import {
  appendBacklogSample,
  rollbackBoundary,
  shouldShowStorageTransitionCard,
} from "@/components/StorageTransition";
import type { StorageStatusReport } from "@/lib/api";

function makeReport(overrides: Partial<StorageStatusReport> = {}): StorageStatusReport {
  return {
    current_engine: "canonical",
    active_engine: "canonical",
    prepared_engine: null,
    state: "canonical",
    transition_epoch: 1,
    details: {},
    entered_at: "2026-06-02T00:00:00Z",
    updated_at: "2026-06-02T00:00:00Z",
    finalized_at: null,
    canonical_live_backlog: 0,
    prepared_queue_storage_schema: null,
    prepared_schema_ready: false,
    live_runtime_capability_counts: {},
    can_enter_mixed_transition: false,
    enter_mixed_transition_blockers: ["nothing prepared"],
    can_finalize: false,
    finalize_blockers: ["not in mixed_transition"],
    ...overrides,
  };
}

describe("durationSince", () => {
  // The 1Hz tick in StorageTransitionCard refreshes this value once per
  // second so the operator sees the in-state clock advance during a
  // multi-hour drain. The tests pin `nowMs` so they don't drift.
  const start = "2026-06-02T00:00:00Z";
  const startMs = new Date(start).getTime();

  it("renders bare seconds under one minute", () => {
    expect(durationSince(start, startMs + 5_000)).toBe("5s");
    expect(durationSince(start, startMs + 59_000)).toBe("59s");
  });

  it("renders minutes with seconds when both are non-zero", () => {
    expect(durationSince(start, startMs + 60_000)).toBe("1m");
    expect(durationSince(start, startMs + 65_000)).toBe("1m 5s");
    expect(durationSince(start, startMs + 59 * 60_000 + 30_000)).toBe("59m 30s");
  });

  it("renders hours with minutes — the readout the issue calls out", () => {
    // 2h 14m — the exact example from the issue description.
    const twoHoursFourteen = 2 * 3600 + 14 * 60;
    expect(durationSince(start, startMs + twoHoursFourteen * 1000)).toBe("2h 14m");
    expect(durationSince(start, startMs + 3600_000)).toBe("1h");
  });

  it("renders days with hours past 24h", () => {
    expect(durationSince(start, startMs + 25 * 3600_000)).toBe("1d 1h");
    expect(durationSince(start, startMs + 72 * 3600_000)).toBe("3d");
  });

  it("clamps negative durations to 0s", () => {
    // If clocks skew, we should not render "-3s" as the readout.
    expect(durationSince(start, startMs - 3_000)).toBe("0s");
  });

  it("returns em-dash for invalid dates", () => {
    expect(durationSince("not-a-date")).toBe("—");
  });
});

describe("appendBacklogSample", () => {
  it("appends the first sample on a fresh epoch", () => {
    const report = makeReport({ transition_epoch: 7, canonical_live_backlog: 42 });
    const result = appendBacklogSample([], 7, report, 1000);
    expect(result.epoch).toBe(7);
    expect(result.samples).toEqual([{ at: 1000, value: 42 }]);
  });

  it("resets the series when transition_epoch changes", () => {
    // Operator aborts a transition and restarts it — the new epoch must
    // not show data from the previous attempt.
    const previousSeries = [
      { at: 1000, value: 100 },
      { at: 2000, value: 80 },
      { at: 3000, value: 60 },
    ];
    const report = makeReport({ transition_epoch: 8, canonical_live_backlog: 12 });
    const result = appendBacklogSample(previousSeries, 7, report, 4000);
    expect(result.epoch).toBe(8);
    expect(result.samples).toEqual([{ at: 4000, value: 12 }]);
  });

  it("dedupes consecutive identical backlog values", () => {
    // The dashboard cache replays the same body to consecutive polls;
    // we must not pile up "the same point" in the sparkline.
    const report = makeReport({ transition_epoch: 5, canonical_live_backlog: 9 });
    let state = appendBacklogSample([], 5, report, 1000);
    state = appendBacklogSample(state.samples, state.epoch, report, 2000);
    expect(state.samples).toEqual([{ at: 1000, value: 9 }]);
  });

  it("retains new samples when the value actually moved", () => {
    const reportA = makeReport({ transition_epoch: 5, canonical_live_backlog: 9 });
    const reportB = makeReport({ transition_epoch: 5, canonical_live_backlog: 7 });
    let state = appendBacklogSample([], 5, reportA, 1000);
    state = appendBacklogSample(state.samples, state.epoch, reportB, 2000);
    expect(state.samples).toEqual([
      { at: 1000, value: 9 },
      { at: 2000, value: 7 },
    ]);
  });
});

describe("rollbackBoundary", () => {
  it("classifies canonical / prepared / aborted as reversible-via-abort", () => {
    expect(rollbackBoundary(makeReport({ state: "canonical" }))).toBe(
      "reversible-via-abort"
    );
    expect(rollbackBoundary(makeReport({ state: "prepared" }))).toBe(
      "reversible-via-abort"
    );
    expect(rollbackBoundary(makeReport({ state: "aborted" }))).toBe(
      "reversible-via-abort"
    );
  });

  it("classifies mixed_transition as restore-only (the one-way door)", () => {
    expect(rollbackBoundary(makeReport({ state: "mixed_transition" }))).toBe(
      "restore-only"
    );
  });

  it("classifies active as finalized", () => {
    expect(
      rollbackBoundary(
        makeReport({ state: "active", active_engine: "queue_storage" })
      )
    ).toBe("finalized");
  });
});

describe("shouldShowStorageTransitionCard", () => {
  it("hides the card on a canonical-only cluster", () => {
    // Pre-flight — nothing prepared, canonical everywhere. The card
    // would only be noise here so the runtime page hides it entirely.
    expect(
      shouldShowStorageTransitionCard(
        makeReport({
          state: "canonical",
          active_engine: "canonical",
          prepared_engine: null,
        })
      )
    ).toBe(false);
  });

  it("shows the card when a prepared engine is recorded", () => {
    expect(
      shouldShowStorageTransitionCard(
        makeReport({ state: "prepared", prepared_engine: "queue_storage" })
      )
    ).toBe(true);
  });

  it("shows the card when the cluster has finalized onto queue_storage", () => {
    expect(
      shouldShowStorageTransitionCard(
        makeReport({
          state: "active",
          active_engine: "queue_storage",
          prepared_engine: null,
        })
      )
    ).toBe(true);
  });
});
