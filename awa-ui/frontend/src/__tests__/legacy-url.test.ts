import { describe, it, expect } from "vitest";
import { foldLegacyFilter } from "@/lib/legacy-url";

describe("foldLegacyFilter", () => {
  it("leaves a URL without legacy keys alone", () => {
    expect(foldLegacyFilter({ state: "failed", q: "kind:a" })).toBeNull();
  });

  it("folds kind, queue and tags into q and keeps other keys", () => {
    expect(foldLegacyFilter({ state: "failed", kind: "send_email" })).toEqual({
      state: "failed",
      q: "kind:send_email",
    });
    expect(foldLegacyFilter({ queue: "default", tag: ["a", "b"], limit: 20 })).toEqual({
      limit: 20,
      q: "queue:default tag:a tag:b",
    });
  });

  it("keeps an existing search term over a legacy one", () => {
    expect(foldLegacyFilter({ q: "kind:a tag:x", kind: "b", tag: "y" })).toEqual({ q: "kind:a tag:x tag:y" });
  });

  it("reads numbers the router parsed from the query", () => {
    expect(foldLegacyFilter({ queue: 123 })).toEqual({ q: "queue:123" });
  });
});
