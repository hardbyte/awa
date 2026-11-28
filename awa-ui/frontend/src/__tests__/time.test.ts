import { describe, it, expect } from "vitest";
import { durationBetween, formatAbsolute, formatRelative } from "@/lib/time";
import { formatNumber, plural } from "@/lib/number";
import { pausedLabel } from "@/lib/pause";

const NOW = Date.parse("2026-11-28T12:00:00Z");

describe("durationBetween", () => {
  it("formats the gap between two timestamps", () => {
    expect(durationBetween("2026-11-28T12:00:00Z", "2026-11-28T12:25:24Z")).toBe("25m 24s");
    expect(durationBetween("2026-11-28T10:00:00Z", "2026-11-28T12:00:00Z")).toBe("2h");
  });

  it("is a dash when either end is missing or invalid", () => {
    expect(durationBetween(null, "2026-11-28T12:00:00Z")).toBe("—");
    expect(durationBetween("2026-11-28T12:00:00Z", undefined)).toBe("—");
    expect(durationBetween("nope", "2026-11-28T12:00:00Z")).toBe("—");
  });
});

describe("formatRelative", () => {
  it("reads the past as ago and the future as in", () => {
    expect(formatRelative("2026-11-28T11:55:00Z", NOW)).toBe("5m ago");
    expect(formatRelative("2026-11-28T14:30:00Z", NOW)).toBe("in 2h");
    expect(formatRelative("2026-11-25T12:00:00Z", NOW)).toBe("3d ago");
  });

  it("says just now within a second", () => {
    expect(formatRelative("2026-11-28T12:00:00.400Z", NOW)).toBe("just now");
  });
});

describe("formatAbsolute", () => {
  it("names the zone it formats in", () => {
    const text = formatAbsolute("2026-11-28T14:03:05Z", "UTC");
    expect(text).toContain("2026");
    expect(text).toContain("14:03:05");
    expect(text).toContain("UTC");
  });

  it("is a dash for an invalid timestamp", () => {
    expect(formatAbsolute("not a date", "UTC")).toBe("—");
  });
});

describe("numbers", () => {
  it("groups digits and pluralises", () => {
    expect(formatNumber(3000)).toBe((3000).toLocaleString());
    expect(plural(1, "job")).toBe("1 job");
    expect(plural(3000, "job")).toBe(`${(3000).toLocaleString()} jobs`);
  });
});

describe("pausedLabel", () => {
  const now = Date.parse("2026-11-28T12:00:00Z");
  const at = new Date(now - 25 * 60_000).toISOString();
  it("says when and by whom, as far as the server reports them", () => {
    expect(pausedLabel({ paused_at: at, paused_by: "ops" }, now)).toBe("Paused 25m ago by ops");
    expect(pausedLabel({ paused_at: at, paused_by: null }, now)).toBe("Paused 25m ago");
    expect(pausedLabel({ paused_at: null, paused_by: "ops" }, now)).toBe("Paused by ops");
    expect(pausedLabel({ paused_at: null, paused_by: "  " }, now)).toBe("Paused");
  });
  it("falls back to Paused for a server that sends neither field", () => {
    expect(pausedLabel({}, now)).toBe("Paused");
    expect(pausedLabel({ paused_at: "not a time" }, now)).toBe("Paused");
  });
});
