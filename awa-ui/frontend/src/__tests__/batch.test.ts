import { describe, it, expect } from "vitest";
import {
  batchOutcome,
  bulkActionReason,
  bulkConfirmLabel,
  bulkConfirmText,
  describeFilter,
  describeSpec,
  filterToSearch,
  formatCount,
  formFromSearch,
  isBatchOperationId,
  isNarrowing,
  previewCount,
  sameFilter,
  searchFromForm,
} from "@/lib/batch";
import { toWireFilter } from "@/lib/api";
import { bulkTier } from "@/lib/confirm";
import { TYPE_TO_CONFIRM_MIN_JOBS } from "@/lib/constants";

describe("isNarrowing", () => {
  it("is false for an empty filter", () => {
    expect(isNarrowing({})).toBe(false);
  });

  it("is false for state and dead_lettered alone", () => {
    expect(isNarrowing({ state: "available" })).toBe(false);
    expect(isNarrowing({ state: ["available", "scheduled"] })).toBe(false);
    expect(isNarrowing({ dead_lettered: false })).toBe(false);
    expect(isNarrowing({ state: "scheduled", dead_lettered: true })).toBe(false);
  });

  it("is true for kind, queue, tag, ids or a created bound", () => {
    expect(isNarrowing({ kind: "k" })).toBe(true);
    expect(isNarrowing({ queue: "q" })).toBe(true);
    expect(isNarrowing({ tag: "t" })).toBe(true);
    expect(isNarrowing({ tag: ["a", "b"] })).toBe(true);
    expect(isNarrowing({ ids: [1] })).toBe(true);
    expect(isNarrowing({ created_at_gte: "2026-10-01T00:00:00.000Z" })).toBe(true);
    expect(isNarrowing({ created_at_lt: "2026-10-01T00:00:00.000Z" })).toBe(true);
  });

  it("ignores empty values", () => {
    expect(isNarrowing({ kind: "", queue: "", ids: [], tag: [] })).toBe(false);
  });

  it("reads the wire form of a jobs-page filter", () => {
    expect(isNarrowing(toWireFilter({ states: ["available"], tags: [], ids: [] }))).toBe(false);
    expect(isNarrowing(toWireFilter({ states: ["available"], tags: ["e2e"] }))).toBe(true);
  });
});

describe("sameFilter", () => {
  it("ignores key order and undefined fields", () => {
    expect(sameFilter({ queue: "q", kind: "k" }, { kind: "k", queue: "q", tag: undefined })).toBe(
      true
    );
  });

  it("sees any changed field", () => {
    expect(sameFilter({ queue: "q" }, { queue: "r" })).toBe(false);
    expect(sameFilter({ queue: "q" }, { queue: "q", state: "available" })).toBe(false);
    expect(sameFilter({ ids: [1, 2] }, { ids: [1] })).toBe(false);
    expect(sameFilter({ tag: "a" }, { tag: ["a", "b"] })).toBe(false);
    expect(
      sameFilter(
        { queue: "q", created_at_gte: "2026-10-01T00:00:00.000Z" },
        { queue: "q", created_at_gte: "2026-10-02T00:00:00.000Z" }
      )
    ).toBe(false);
  });
});

describe("previewCount", () => {
  it("reads a plain number as exact", () => {
    expect(previewCount({ total_matched: 3, sample: [] })).toEqual({ kind: "exact", value: 3 });
  });

  it("keeps an at-least count", () => {
    expect(
      previewCount({ total_matched: { kind: "at_least", value: 10000 }, sample: [] })
    ).toEqual({ kind: "at_least", value: 10000 });
  });
});

describe("formatCount", () => {
  it("shows an exact count as is", () => {
    expect(formatCount({ kind: "exact", value: 2 })).toBe("2");
  });

  it("marks an at-least count", () => {
    expect(formatCount({ kind: "at_least", value: 10000 })).toBe(
      `at least ${(10000).toLocaleString()}`
    );
  });
});

describe("describeFilter", () => {
  it("names each field and the default states", () => {
    expect(
      describeFilter({
        queue: "e2e_test",
        kind: "e2e_job",
        tag: ["e2e", "priority"],
        ids: [1, 2],
        created_at_gte: "2026-10-01T00:00:00.000Z",
      })
    ).toBe(
      "available or scheduled jobs with queue e2e_test, kind e2e_job, tagged e2e, priority, id 1, 2, created from 2026-10-01T00:00:00.000Z"
    );
  });

  it("names a chosen state", () => {
    expect(describeFilter({ queue: "q", state: "scheduled" })).toBe("scheduled jobs with queue q");
  });

  it("names the operation kind's states when no state is chosen", () => {
    expect(describeFilter({ queue: "q" }, "dead-lettered")).toBe("dead-lettered jobs with queue q");
    expect(describeFilter({ queue: "q", state: "cancelled" }, "failed or cancelled")).toBe(
      "cancelled jobs with queue q"
    );
  });
});

describe("the new-operation form in the URL", () => {
  it("round-trips through the search", () => {
    const form = formFromSearch({ op: "move_queue", queue: "q", tags: "a, b", destination: "d", priority: 3 });
    expect(form.opKind).toBe("move_queue");
    expect(form.priority).toBe("3");
    expect(formFromSearch(searchFromForm(form))).toEqual(form);
  });

  it("leaves defaults and blanks out of the URL", () => {
    expect(searchFromForm(formFromSearch({ queue: "q" }))).toEqual({ queue: "q" });
  });

  it("drops a state the operation does not act on", () => {
    expect(formFromSearch({ op: "retry", state: "available" }).state).toBe("");
    expect(formFromSearch({ op: "retry", state: "failed" }).state).toBe("failed");
    expect(formFromSearch({ op: "bogus" }).opKind).toBe("set_priority");
  });
});

describe("operation details", () => {
  it("turns a wire filter into a jobs search", () => {
    expect(filterToSearch({ kind: "k", tag: ["a", "b"], state: "failed" })).toEqual({
      filter: { kind: "k", tags: ["a", "b"] },
      state: "failed",
    });
    expect(filterToSearch({ queue: "q", state: ["available", "scheduled"] }).state).toBeUndefined();
  });

  it("reports changed and skipped separately", () => {
    expect(batchOutcome({ processed: 0, skipped: 3000, errored: 0 })).toBe(
      `0 changed, ${(3000).toLocaleString()} skipped`
    );
    expect(batchOutcome({ processed: 1, skipped: 0, errored: 2 })).toBe("1 changed, 0 skipped, 2 errored");
  });

  it("describes what each kind sets", () => {
    expect(describeSpec("set_priority", { priority: 1 })).toBe("Priority 1");
    expect(describeSpec("move_queue", { queue: "x" })).toBe("To queue x");
    expect(describeSpec("retry", {})).toBeNull();
  });

  it("accepts only UUID ids", () => {
    expect(isBatchOperationId("44444444-4444-4444-8444-444444444444")).toBe(true);
    expect(isBatchOperationId("new")).toBe(false);
  });
});

describe("bulkActionReason", () => {
  const filter = { kind: "send_email" };

  it("allows an action whose states overlap the list's", () => {
    expect(bulkActionReason("retry", { filter, states: ["failed"] })).toBeNull();
    expect(bulkActionReason("cancel", { filter, states: ["running"] })).toBeNull();
    expect(bulkActionReason("retry", { filter })).toBeNull();
  });

  it("says why an action can't act on the list's state", () => {
    expect(bulkActionReason("retry", { filter, states: ["running"] })).toBe(
      "Retry acts on failed or cancelled jobs, and this list shows running jobs"
    );
    expect(bulkActionReason("discard", { filter, states: ["waiting_external"] })).toMatch(/shows waiting external jobs/);
  });

  it("needs a filter that names jobs, not just a state", () => {
    expect(bulkActionReason("retry", { filter: { state: "failed" }, states: ["failed"] })).toMatch(/a state alone/);
  });

  it("is off when nothing matches", () => {
    expect(bulkActionReason("retry", { filter, count: { kind: "exact", value: 0 } })).toBe("Nothing matches this filter");
    expect(bulkActionReason("retry", { filter, count: { kind: "at_least", value: 0 } })).toBeNull();
  });

  it("ignores states for dead-letter kinds", () => {
    expect(bulkActionReason("purge_dlq", { filter: { queue: "q" } })).toBeNull();
  });
});

describe("bulkTier", () => {
  it("asks to type for discards and purges at any size", () => {
    expect(bulkTier("discard", { kind: "exact", value: 1 })).toBe(3);
    expect(bulkTier("purge_dlq", { kind: "exact", value: 2 })).toBe(3);
  });

  it("asks to type for a cancel at the threshold or of an unbounded count", () => {
    expect(bulkTier("cancel", { kind: "exact", value: TYPE_TO_CONFIRM_MIN_JOBS - 1 })).toBe(2);
    expect(bulkTier("cancel", { kind: "exact", value: TYPE_TO_CONFIRM_MIN_JOBS })).toBe(3);
    expect(bulkTier("cancel", { kind: "at_least", value: 10 })).toBe(3);
    expect(bulkTier("cancel", undefined)).toBe(3);
  });

  it("keeps reversible actions at tier 2", () => {
    for (const kind of ["retry", "set_priority", "move_queue", "move_to_dlq", "retry_from_dlq"] as const) {
      expect(bulkTier(kind, { kind: "at_least", value: 100_000 })).toBe(2);
    }
  });
});

describe("bulkConfirmLabel", () => {
  it("names the action, the count and what it counts", () => {
    expect(bulkConfirmLabel("retry", { kind: "exact", value: 1234 })).toBe(`Retry ${(1234).toLocaleString()} jobs`);
    expect(bulkConfirmLabel("cancel", { kind: "at_least", value: 10 })).toBe("Cancel at least 10 jobs");
    expect(bulkConfirmLabel("purge_dlq", { kind: "exact", value: 1 })).toBe("Purge 1 dead letter");
    expect(bulkConfirmLabel("move_to_dlq", { kind: "exact", value: 3 })).toBe("Move 3 jobs to the DLQ");
    expect(bulkConfirmText({ kind: "exact", value: 1234 })).toBe("1234");
  });
});
