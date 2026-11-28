import { describe, expect, it } from "vitest";
import { jobSummary, type SummaryJob } from "@/lib/job-summary";

const NOW = Date.parse("2026-11-28T12:00:00Z");
const ago = (seconds: number) => new Date(NOW - seconds * 1000).toISOString();
const later = (seconds: number) => new Date(NOW + seconds * 1000).toISOString();

function job(overrides: Partial<SummaryJob>): SummaryJob {
  return {
    state: "available",
    attempt: 0,
    max_attempts: 5,
    run_at: ago(0),
    heartbeat_at: null,
    deadline_at: null,
    attempted_at: null,
    finalized_at: null,
    created_at: ago(600),
    callback_timeout_at: null,
    progress: null,
    ...overrides,
  };
}

const texts = (summary: ReturnType<typeof jobSummary>) => summary.map((fact) => fact.text);

describe("jobSummary", () => {
  it("running: elapsed, heartbeat age, deadline, attempt and progress", () => {
    const facts = jobSummary(
      job({
        state: "running",
        attempt: 2,
        attempted_at: ago(125),
        heartbeat_at: ago(4),
        deadline_at: later(1800),
        progress: { percent: 49.6, message: "halfway" },
      }),
      NOW
    );
    expect(texts(facts)).toEqual([
      "Running for 2m 5s",
      "heartbeat 4s ago",
      "deadline in 30m",
      "attempt 2 of 5",
      "50% done",
    ]);
    expect(facts[1]?.at).toBe(ago(4));
  });

  it("running without a heartbeat says it's a short-job claim, and flags a passed deadline", () => {
    const facts = jobSummary(job({ state: "running", attempt: 1, attempted_at: ago(90), deadline_at: ago(30) }), NOW);
    expect(texts(facts)).toContain("no heartbeat yet (short-job claim)");
    expect(facts.find((fact) => fact.key === "deadline")).toMatchObject({ text: "deadline passed 30s ago", warning: true });
  });

  it("retryable and scheduled: when it runs next", () => {
    expect(texts(jobSummary(job({ state: "retryable", attempt: 2, run_at: later(300) }), NOW))).toEqual([
      "Next attempt in 5m",
      "after attempt 2 of 5",
    ]);
    expect(texts(jobSummary(job({ state: "scheduled", run_at: later(7200) }), NOW))).toEqual(["Runs in 2h"]);
    expect(texts(jobSummary(job({ state: "scheduled", run_at: ago(20) }), NOW))).toEqual(["Runs due 20s ago"]);
  });

  it("waiting: how long, and when the callback times out", () => {
    const facts = jobSummary(
      job({ state: "waiting_external", attempted_at: ago(3600), callback_timeout_at: later(600) }),
      NOW
    );
    expect(texts(facts)).toEqual(["Waiting for a callback for 1h", "callback times out in 10m"]);
    const expired = jobSummary(job({ state: "waiting_external", attempted_at: ago(60), callback_timeout_at: ago(5) }), NOW);
    expect(expired[1]).toMatchObject({ text: "callback timed out 5s ago", warning: true });
  });

  it("available: how long it has been ready", () => {
    expect(texts(jobSummary(job({ run_at: ago(45) }), NOW))).toEqual(["Ready for 45s"]);
    expect(texts(jobSummary(job({ run_at: ago(45), attempt: 2 }), NOW))).toEqual(["Ready for 45s", "2 attempts so far"]);
  });

  it("completed: how long it took and waited", () => {
    expect(
      texts(jobSummary(job({ state: "completed", attempt: 1, attempted_at: ago(130), finalized_at: ago(10) }), NOW))
    ).toEqual(["Took 2m", "waited 7m 50s to start", "finished 10s ago"]);
  });

  it("failed and cancelled: when, after how many attempts", () => {
    expect(texts(jobSummary(job({ state: "failed", attempt: 3, finalized_at: ago(60) }), NOW))).toEqual([
      "Failed 1m ago after 3 attempts",
    ]);
    expect(texts(jobSummary(job({ state: "cancelled", attempt: 0, finalized_at: ago(5) }), NOW))).toEqual([
      "Cancelled 5s ago",
    ]);
  });

  it("dead-lettered rows", () => {
    expect(texts(jobSummary(job({ state: "dead_lettered", attempt: 1, dlq_at: ago(180) }), NOW))).toEqual([
      "Dead-lettered 3m ago after 1 attempt",
    ]);
  });
});
