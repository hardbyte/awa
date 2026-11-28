import { describe, it, expect } from "vitest";
import { formatDuration, timeAgo } from "@/lib/time";
import { parseSearch } from "@/components/SearchBar";
import { toBulkRetryResult } from "@/lib/api";
import type { JobRow } from "@/lib/api";

describe("formatDuration", () => {
  it("formats seconds as a compact duration", () => {
    expect(formatDuration(0.4)).toBe("0s");
    expect(formatDuration(59.9)).toBe("59s");
    expect(formatDuration(1524.7)).toBe("25m 24s");
    expect(formatDuration(7200)).toBe("2h");
    expect(formatDuration(90061)).toBe("1d 1h");
  });
});

describe("timeAgo", () => {
  it("shows seconds for recent times", () => {
    const now = new Date().toISOString();
    expect(timeAgo(now)).toMatch(/^(0s ago|just now)$/);
  });

  it("shows minutes", () => {
    const fiveMinAgo = new Date(Date.now() - 5 * 60_000).toISOString();
    expect(timeAgo(fiveMinAgo)).toBe("5m ago");
  });

  it("shows hours", () => {
    const twoHoursAgo = new Date(Date.now() - 2 * 3600_000).toISOString();
    expect(timeAgo(twoHoursAgo)).toBe("2h ago");
  });

  it("shows days", () => {
    const threeDaysAgo = new Date(Date.now() - 3 * 86400_000).toISOString();
    expect(timeAgo(threeDaysAgo)).toBe("3d ago");
  });

  it("handles future dates", () => {
    const future = new Date(Date.now() + 60_000).toISOString();
    expect(timeAgo(future)).toBe("just now");
  });
});

describe("parseSearch", () => {
  it("parses a single kind filter", () => {
    expect(parseSearch("kind:send_email")).toEqual({ kind: "send_email" });
  });

  it("parses a single queue filter", () => {
    expect(parseSearch("queue:default")).toEqual({ queue: "default" });
  });

  it("parses a single tag filter", () => {
    expect(parseSearch("tag:urgent")).toEqual({ tag: "urgent" });
  });

  it("parses multiple filters", () => {
    expect(parseSearch("kind:send_email queue:default tag:urgent")).toEqual({
      kind: "send_email",
      queue: "default",
      tag: "urgent",
    });
  });

  it("ignores empty prefixes", () => {
    expect(parseSearch("kind: queue:")).toEqual({});
  });

  it("returns empty object for empty string", () => {
    expect(parseSearch("")).toEqual({});
  });

  it("handles extra whitespace", () => {
    expect(parseSearch("  kind:test   queue:q1  ")).toEqual({
      kind: "test",
      queue: "q1",
    });
  });

  it("last value wins for duplicate prefixes", () => {
    expect(parseSearch("kind:first kind:second")).toEqual({
      kind: "second",
    });
  });
});

describe("toBulkRetryResult", () => {
  const job = { id: 7 } as JobRow;

  it("reads the report a current server returns", () => {
    expect(toBulkRetryResult({ jobs: [job], skipped: 1, conflicts: [8] })).toEqual({
      jobs: [job],
      skipped: 1,
      conflicts: [8],
    });
  });

  it("reads the array a server from before report_skipped returns", () => {
    expect(toBulkRetryResult([job])).toEqual({ jobs: [job], skipped: 0, conflicts: [] });
  });
});
