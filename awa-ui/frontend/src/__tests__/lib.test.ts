import { describe, it, expect } from "vitest";
import { timeAgo } from "@/lib/time";
import { parseSearch } from "@/components/SearchBar";

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
