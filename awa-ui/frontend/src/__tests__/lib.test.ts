import { describe, it, expect } from "vitest";
import { timeAgo } from "@/lib/time";
import {
  buildSearch,
  invalidTerms,
  isJobId,
  parseSearch,
  termError,
  toRfc3339,
} from "@/components/SearchBar";
import { toWireFilter } from "@/lib/api";

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
    expect(parseSearch("tag:urgent")).toEqual({ tags: ["urgent"] });
  });

  it("parses multiple filters", () => {
    expect(parseSearch("kind:send_email queue:default tag:urgent")).toEqual({
      kind: "send_email",
      queue: "default",
      tags: ["urgent"],
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

  it("collects repeated tags and ids", () => {
    expect(parseSearch("tag:a tag:b tag:a id:3,4 id:5")).toEqual({
      tags: ["b", "a"],
      ids: [3, 4, 5],
    });
  });

  it("parses created bounds and drops malformed ids", () => {
    expect(parseSearch("created>=2026-10-01 created<2026-10-02 id:x")).toEqual({
      createdGte: "2026-10-01",
      createdLt: "2026-10-02",
    });
  });

  it("round-trips through buildSearch", () => {
    const search = "kind:k queue:q tag:a tag:b id:7 created>=2026-10-01";
    expect(buildSearch(parseSearch(search))).toBe(search);
  });

  it("converts typed dates to RFC 3339", () => {
    expect(toRfc3339("2026-10-01T00:00:00Z")).toBe("2026-10-01T00:00:00.000Z");
    expect(toRfc3339("not a date")).toBeUndefined();
  });

  it("refuses terms it would otherwise drop, typed or from the URL", () => {
    expect(termError("id:123,456")).toBeUndefined();
    expect(termError("queue:billing id:abc")).toMatch(/whole job ids/);
    expect(termError("id:0")).toMatch(/whole job ids/);
    expect(termError("created>=2026-10-30")).toBeUndefined();
    expect(termError("created>=not-a-date")).toMatch(/created>=/);
    expect(termError("created<soon")).toMatch(/created</);
    expect(termError("tag:a,b kind:k")).toBeUndefined();
    expect(termError("queue:q kind:")).toMatch(/kind: needs a value/);
    expect(termError("queue:q billing")).toMatch(/billing isn't a filter/);
    expect(termError("")).toBeUndefined();
  });

  it("lists every malformed term so none is lost", () => {
    expect(invalidTerms("queue:q id:abc tag:t created<nope").map((t) => t.term)).toEqual([
      "id:abc",
      "created<nope",
    ]);
  });

  it("only accepts ids the UI can carry exactly", () => {
    expect(isJobId("9007199254740991")).toBe(true);
    expect(isJobId("9007199254740993")).toBe(false);
    expect(isJobId("12a")).toBe(false);
    expect(termError("id:9007199254740993")).toMatch(/whole job ids/);
    expect(parseSearch("id:9007199254740993")).toEqual({});
  });
});

describe("toWireFilter", () => {
  it("sends one value bare and several as a list under the singular key", () => {
    expect(toWireFilter({ queue: "q", tags: ["a"], states: ["failed"] })).toEqual({
      queue: "q",
      tag: "a",
      state: "failed",
    });
    expect(toWireFilter({ tags: ["a", "b"], states: ["failed", "cancelled"], ids: [] })).toEqual({
      tag: ["a", "b"],
      state: ["failed", "cancelled"],
    });
  });
});
