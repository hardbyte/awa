import { describe, it, expect } from "vitest";
import { batchStateMeta, JOB_STATES, jobStateMeta } from "@/lib/states";

describe("jobStateMeta", () => {
  it("gives every job state its own tone", () => {
    const tones = JOB_STATES.map((state) => jobStateMeta(state).tone);
    expect(new Set(tones).size).toBe(JOB_STATES.length);
  });

  it("labels states in sentence case", () => {
    expect(jobStateMeta("waiting_external").label).toBe("Waiting for callback");
    expect(jobStateMeta("waiting_external").shortLabel).toBe("Waiting");
    expect(jobStateMeta("some_new_state").label).toBe("Some new state");
  });
});

describe("batchStateMeta", () => {
  it("maps operation states onto the job palette", () => {
    expect(batchStateMeta("pending").tone).toBe("scheduled");
    expect(batchStateMeta("scanning").tone).toBe("running");
    expect(batchStateMeta("failed").tone).toBe("failed");
  });
});
