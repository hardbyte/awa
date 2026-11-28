import { describe, expect, it } from "vitest";
import {
  errorSummary,
  groupAttempts,
  groupFailures,
  isLibraryFrame,
  latestError,
  parseErrorEntry,
} from "@/lib/errors";

const PYTHON = [
  "TimeoutError: read timed out after 30s",
  "Traceback (most recent call last):",
  '  File "/app/jobs/sync.py", line 42, in handle',
  "    rows = client.fetch()",
  '  File "/usr/lib/python3.12/site-packages/httpx/_client.py", line 900, in send',
  "    raise TimeoutError()",
].join("\n");

const at = (minute: number) => `2026-11-28T10:${String(minute).padStart(2, "0")}:00Z`;
const entry = (attempt: number, error: string, minute: number, extra: Record<string, unknown> = {}) => ({
  error,
  attempt,
  at: at(minute),
  ...extra,
});

describe("parseErrorEntry", () => {
  it("splits a Python error into class, message and traceback", () => {
    const parsed = parseErrorEntry(entry(3, PYTHON, 5, { terminal: true, output: { rows: 2 } }));
    expect(parsed).toMatchObject({
      exceptionClass: "TimeoutError",
      message: "read timed out after 30s",
      attempt: 3,
      at: at(5),
      terminal: true,
      output: { rows: 2 },
    });
    expect(parsed.traceback?.split("\n")[0]).toBe("Traceback (most recent call last):");
    expect(errorSummary(parsed)).toBe("TimeoutError: read timed out after 30s");
  });

  it("keeps a plain message whole and finds no class in lowercase prefixes", () => {
    expect(parseErrorEntry(entry(1, "connection refused", 1))).toMatchObject({
      exceptionClass: null,
      message: "connection refused",
      traceback: null,
    });
    expect(parseErrorEntry(entry(1, "cancelled: operator request", 1)).exceptionClass).toBeNull();
  });

  it("takes dotted class names", () => {
    expect(parseErrorEntry(entry(1, "httpx.ReadTimeout: slow", 1)).exceptionClass).toBe("httpx.ReadTimeout");
  });

  it("shows an object error as JSON and tolerates a bare string entry", () => {
    expect(parseErrorEntry({ error: { code: 503 }, attempt: 2, at: at(1) }).message).toBe('{"code":503}');
    expect(parseErrorEntry("boom")).toMatchObject({ message: "boom", attempt: null, at: null, terminal: false });
  });
});

describe("groupAttempts", () => {
  it("starts a new run where the attempt number goes down", () => {
    const runs = groupAttempts([entry(1, "a", 1), entry(2, "b", 3), entry(1, "c", 30), entry(2, "d", 31)]);
    expect(runs.map((run) => run.rows.map((row) => row.error.message))).toEqual([["a", "b"], ["c", "d"]]);
    expect(runs[1]?.rows[0]?.gapMs).toBeNull();
    expect(runs[0]?.rows[1]?.gapMs).toBe(2 * 60_000);
  });

  it("keeps a terminal entry for the same attempt in its run", () => {
    const runs = groupAttempts([
      entry(1, "a", 1),
      entry(2, "timeout", 2),
      entry(2, "unique claim held by a newer job", 2, { terminal: true }),
    ]);
    expect(runs).toHaveLength(1);
    expect(runs[0]?.rows.map((row) => [row.error.attempt, row.error.terminal])).toEqual([
      [1, false],
      [2, false],
      [2, true],
    ]);
  });

  it("collapses identical consecutive errors and measures the next gap from the last of them", () => {
    const runs = groupAttempts([
      entry(1, "timeout", 1),
      entry(2, "timeout", 2),
      entry(3, "timeout", 4),
      entry(4, "boom", 10),
    ]);
    const rows = runs[0]!.rows;
    expect(rows).toHaveLength(2);
    expect(rows[0]?.repeats.map((r) => r.attempt)).toEqual([2, 3]);
    expect(rows[1]?.gapMs).toBe(6 * 60_000);
  });

  it("doesn't collapse identical errors across runs", () => {
    const runs = groupAttempts([entry(2, "timeout", 1), entry(1, "timeout", 9)]);
    expect(runs).toHaveLength(2);
  });

  it("is empty without entries", () => {
    expect(groupAttempts(null)).toEqual([]);
    expect(latestError([])).toBeNull();
  });
});

describe("isLibraryFrame", () => {
  it("dims installed packages and the standard library, not the job's own code", () => {
    const lines = PYTHON.split("\n");
    expect(isLibraryFrame(lines[2]!)).toBe(false);
    expect(isLibraryFrame(lines[4]!)).toBe(true);
    expect(isLibraryFrame('  File "<frozen importlib._bootstrap>", line 1')).toBe(true);
  });
});

describe("groupFailures", () => {
  it("groups by kind and error class, biggest first, keeping job order", () => {
    const jobs = [
      { id: 5, kind: "sync", errors: [entry(3, PYTHON, 5)] },
      { id: 4, kind: "sync", errors: [entry(1, "ValueError: bad row", 4)] },
      { id: 3, kind: "sync", errors: [entry(2, "TimeoutError: other message", 3)] },
      { id: 2, kind: "email", errors: [entry(1, "TimeoutError: smtp", 2)] },
      { id: 1, kind: "email", errors: null },
    ];
    const groups = groupFailures(jobs);
    expect(groups[0]).toMatchObject({
      kind: "sync",
      key: "TimeoutError",
      summary: "TimeoutError: read timed out after 30s",
      count: 2,
      jobIds: [5, 3],
    });
    expect(groups.map((g) => `${g.kind}/${g.key}`)).toEqual([
      "sync/TimeoutError",
      "sync/ValueError",
      "email/TimeoutError",
      "email/No error recorded",
    ]);
  });
});
