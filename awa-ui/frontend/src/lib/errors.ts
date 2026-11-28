/**
 * Reading a job's `errors` entries, which workers write as
 * `{error, attempt, at, terminal?, output?}`. An entry records when an attempt
 * failed, not when it started, and nothing marks a manual retry, so a new run
 * is inferred where the attempt number goes down.
 */

export interface ParsedError {
  /** The exception class when the message starts `Class: message`, e.g. a Python error. */
  exceptionClass: string | null;
  /** The first line, without the class. */
  message: string;
  /** Every line after the first, e.g. a Python traceback; null when there are none. */
  traceback: string | null;
  attempt: number | null;
  at: string | null;
  terminal: boolean;
  output: unknown;
  /** The entry as stored. */
  raw: unknown;
}

const CLASS_PREFIX = /^((?:[A-Za-z_]\w*\.)*[A-Z]\w*):\s+(.*)$/;

function errorText(value: unknown): string {
  if (typeof value === "string") return value;
  if (value === undefined || value === null) return "";
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

export function parseErrorEntry(entry: unknown): ParsedError {
  const fields = entry && typeof entry === "object" ? (entry as Record<string, unknown>) : { error: entry };
  const text = errorText(fields.error).replace(/\r\n/g, "\n");
  const newline = text.indexOf("\n");
  const first = (newline === -1 ? text : text.slice(0, newline)).trim();
  const rest = newline === -1 ? "" : text.slice(newline + 1).replace(/\s+$/, "");
  const match = CLASS_PREFIX.exec(first);
  return {
    exceptionClass: match ? match[1]! : null,
    message: match ? match[2]! : first,
    traceback: rest ? rest : null,
    attempt: typeof fields.attempt === "number" ? fields.attempt : null,
    at: typeof fields.at === "string" ? fields.at : null,
    terminal: fields.terminal === true,
    output: fields.output,
    raw: entry,
  };
}

/** The error's first line, with its class, e.g. "TimeoutError: read timed out". */
export function errorSummary(error: Pick<ParsedError, "exceptionClass" | "message">): string {
  return error.exceptionClass ? `${error.exceptionClass}: ${error.message}` : error.message;
}

function sameError(a: ParsedError, b: ParsedError): boolean {
  return errorText((a.raw as Record<string, unknown> | null)?.error) === errorText((b.raw as Record<string, unknown> | null)?.error);
}

/** One row of an attempt table: an entry and the identical ones right after it. */
export interface AttemptRow {
  error: ParsedError;
  /** Later consecutive entries with exactly the same error text. */
  repeats: ParsedError[];
  /** Milliseconds since the previous entry failed, in the same run. */
  gapMs: number | null;
}

export interface Run {
  /** 1 for the first run, counting up. */
  run: number;
  rows: AttemptRow[];
}

/**
 * Entries grouped into runs, oldest first. A run ends where the attempt
 * number goes down; within a run, identical consecutive errors collapse into
 * one row.
 */
export function groupAttempts(errors: unknown[] | null | undefined): Run[] {
  const runs: Run[] = [];
  let previous: ParsedError | null = null;
  for (const entry of errors ?? []) {
    const error = parseErrorEntry(entry);
    const startsRun =
      !previous || (error.attempt !== null && previous.attempt !== null && error.attempt < previous.attempt);
    if (startsRun) runs.push({ run: runs.length + 1, rows: [] });
    const run = runs[runs.length - 1]!;
    const last = run.rows[run.rows.length - 1];
    if (last && sameError(last.repeats[last.repeats.length - 1] ?? last.error, error)) {
      last.repeats.push(error);
    } else {
      const gapMs =
        !startsRun && previous?.at && error.at ? new Date(error.at).getTime() - new Date(previous.at).getTime() : null;
      run.rows.push({ error, repeats: [], gapMs: gapMs !== null && Number.isFinite(gapMs) ? gapMs : null });
    }
    previous = error;
  }
  return runs;
}

/** The newest entry, parsed, or null without any. */
export function latestError(errors: unknown[] | null | undefined): ParsedError | null {
  const list = errors ?? [];
  return list.length > 0 ? parseErrorEntry(list[list.length - 1]) : null;
}

/** Whether a traceback line points into library code rather than the job's own. */
export function isLibraryFrame(line: string): boolean {
  return /^\s*File "[^"]*(site-packages|dist-packages|\/lib\/python\d|<frozen )/.test(line);
}

export interface FailureGroup {
  kind: string;
  /** The class, or the first line when there is none. */
  key: string;
  /** The newest job's error summary. */
  summary: string;
  count: number;
  /** Job ids in the order given, newest first when the jobs are. */
  jobIds: number[];
}

/** Failed jobs grouped by kind and error class (or first line), biggest group first. */
export function groupFailures(
  jobs: { id: number; kind: string; errors: unknown[] | null }[]
): FailureGroup[] {
  const groups = new Map<string, FailureGroup>();
  for (const job of jobs) {
    const error = latestError(job.errors);
    const key = error ? (error.exceptionClass ?? error.message) || "No error recorded" : "No error recorded";
    const id = `${job.kind}\u0000${key}`;
    const group = groups.get(id);
    if (group) {
      group.count += 1;
      group.jobIds.push(job.id);
    } else {
      groups.set(id, {
        kind: job.kind,
        key,
        summary: error ? errorSummary(error) || "No error recorded" : "No error recorded",
        count: 1,
        jobIds: [job.id],
      });
    }
  }
  return [...groups.values()].sort((a, b) => b.count - a.count);
}
