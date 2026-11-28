import { describe, expect, it } from "vitest";
import { freshnessStatus } from "@/lib/freshness";
import type { Freshness } from "@/lib/observation/scheduler";

const base: Freshness = { views: 3, failed: 0, unloaded: 0, pending: 0, ageMs: 3_000, paused: false, rejected: 0, halted: false };
const status = (overrides: Partial<Freshness>) => freshnessStatus({ ...base, ...overrides });

describe("freshnessStatus", () => {
  it("is live with the age of the least recent view", () => {
    expect(status({})).toEqual({ tone: "live", short: "Live", label: "Live · updated 3s ago" });
    expect(status({ ageMs: 400 }).label).toBe("Live · updated just now");
    expect(status({ ageMs: 125_000 }).label).toBe("Live · updated 2m 5s ago");
  });

  it("is loading until every view has loaded once", () => {
    expect(status({ ageMs: null })).toMatchObject({ tone: "live", label: "Loading…" });
  });

  it("is stale while some reads fail, saying how old the data is", () => {
    expect(status({ failed: 1, ageMs: 120_000 })).toEqual({
      tone: "stale",
      short: "Reconnecting",
      label: "Last updated 2m ago — reconnecting",
    });
    expect(status({ failed: 1, unloaded: 1, ageMs: null }).label).toBe("Some views failed to load — reconnecting");
  });

  it("is down when every read fails or access is refused", () => {
    expect(status({ failed: 3 })).toMatchObject({ tone: "down", label: "Server unreachable" });
    expect(status({ halted: true })).toMatchObject({ tone: "down", short: "Refused" });
  });

  it("says when a list paused itself or views were turned away", () => {
    expect(status({ paused: true })).toMatchObject({ tone: "paused", label: "Paused while rows are selected" });
    expect(status({ rejected: 1 })).toMatchObject({ tone: "stale", short: "Too many views" });
  });

  it("is live when nothing on the page refreshes", () => {
    expect(status({ views: 0, ageMs: null })).toMatchObject({ tone: "live", label: "Live" });
  });
});
