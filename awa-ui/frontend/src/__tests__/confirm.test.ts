import { describe, expect, it } from "vitest";
import { selectionTier } from "@/lib/confirm";
import { jobActionCopy, selectionCopy, selectionOutcome } from "@/lib/job-actions";

describe("selectionTier", () => {
  it("confirms one job by name and several with a list", () => {
    expect(selectionTier(1)).toBe(1);
    expect(selectionTier(2)).toBe(2);
    expect(selectionTier(100)).toBe(2);
  });
});

describe("jobActionCopy", () => {
  const job = { id: 918, kind: "send_email" };

  it("names the job in the title and the confirm button", () => {
    const copy = jobActionCopy({ ...job, state: "failed" }, "retry");
    expect(copy.title).toBe("Retry job #918?");
    expect(copy.confirmLabel).toBe("Retry job #918");
    expect(copy.description).toMatch(/happens again/);
  });

  it("warns that restarting a waiting job discards its callback", () => {
    const copy = jobActionCopy({ ...job, state: "waiting_external" }, "retry");
    expect(copy.confirmLabel).toBe("Restart job #918");
    expect(copy.description).toMatch(/discards the pending callback/);
  });

  it("says a running job's handler is signalled to stop", () => {
    const copy = jobActionCopy({ ...job, state: "running" }, "cancel");
    expect(copy).toMatchObject({ confirmLabel: "Cancel job #918", intent: "danger" });
    expect(copy.description).toMatch(/signalled to stop/);
    expect(copy.description).toMatch(/can be retried later/);
  });

  it("never claims a cancel can't be undone", () => {
    for (const state of ["available", "scheduled", "retryable", "running", "waiting_external"]) {
      expect(jobActionCopy({ ...job, state }, "cancel").description).not.toMatch(/undone/);
    }
  });
});

describe("selectionCopy", () => {
  it("counts the jobs and mentions waiting ones only when there are some", () => {
    expect(selectionCopy("retry", 4, 0)).toMatchObject({ title: "Retry 4 jobs?", confirmLabel: "Retry 4 jobs" });
    expect(selectionCopy("retry", 4, 0).description).not.toMatch(/callback/);
    expect(selectionCopy("retry", 4, 2).description).toMatch(/2 of them are waiting/);
    expect(selectionCopy("cancel", 4, 0)).toMatchObject({ confirmLabel: "Cancel 4 jobs", intent: "danger" });
  });
});

describe("selectionOutcome", () => {
  const states = (...list: string[]) => list.map((state) => ({ state }));

  it("reports what the server changed, not what was asked", () => {
    expect(selectionOutcome("retry", states("failed", "failed", "cancelled"), 3, 2)).toEqual({
      message: "Retried 2 of 3 selected job(s)",
      detail: "Skipped: 1 changed state before the retry reached them.",
    });
  });

  it("says why selected jobs weren't sent", () => {
    const outcome = selectionOutcome("retry", states("failed", "retryable", "retryable", "completed"), 1, 1);
    expect(outcome.message).toBe("Retried 1 of 4 selected job(s)");
    expect(outcome.detail).toBe(
      "Skipped: 2 retryable job(s) retry automatically; 1 weren't failed, cancelled or waiting."
    );
  });

  it("has no detail when every selected job changed", () => {
    expect(selectionOutcome("cancel", states("available", "running"), 2, 2)).toEqual({
      message: "Cancelled 2 of 2 selected job(s)",
    });
  });

  it("counts finished jobs as skipped by a cancel", () => {
    expect(selectionOutcome("cancel", states("available", "completed"), 1, 1).detail).toBe(
      "Skipped: 1 had already finished."
    );
  });
});
