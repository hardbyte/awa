import { Fragment, useState } from "react";
import { errorSummary, groupAttempts, isLibraryFrame, latestError, type AttemptRow, type ParsedError } from "@/lib/errors";
import { formatDuration } from "@/lib/time";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { CopyButton } from "@/components/CopyButton";
import { JsonView } from "@/components/JsonView";
import { Time } from "@/components/Time";

function Traceback({ text }: { text: string }) {
  return (
    <pre
      className="max-h-96 overflow-auto whitespace-pre-wrap break-words rounded-md bg-muted p-3 font-mono text-xs leading-relaxed"
      data-testid="error-traceback"
    >
      {text.split("\n").map((line, index) => (
        <span key={index} className={isLibraryFrame(line) ? "text-muted-fg/70" : undefined}>
          {line}
          {"\n"}
        </span>
      ))}
    </pre>
  );
}

function fullText(error: ParsedError): string {
  return [errorSummary(error), error.traceback].filter(Boolean).join("\n");
}

type Outcome = "Terminal" | "Retried" | "Gave up" | "Retry scheduled" | "Cancelled" | "Dead-lettered";

function outcome(error: ParsedError, isLast: boolean, jobState: string): Outcome {
  if (error.terminal) return "Terminal";
  if (!isLast) return "Retried";
  if (jobState === "retryable") return "Retry scheduled";
  if (jobState === "cancelled") return "Cancelled";
  if (jobState === "dead_lettered") return "Dead-lettered";
  if (jobState === "failed") return "Gave up";
  return "Retried";
}

function AttemptRows({
  row,
  runLabel,
  isLastRow,
  jobState,
}: {
  row: AttemptRow;
  runLabel: string | null;
  isLastRow: boolean;
  jobState: string;
}) {
  const [expanded, setExpanded] = useState(false);
  const entries = expanded ? [row.error, ...row.repeats] : [row.error];
  const last = row.repeats[row.repeats.length - 1] ?? row.error;
  return (
    <>
      {entries.map((error, index) => {
        const isFirst = index === 0;
        const isFinalEntry = isLastRow && (expanded ? index === entries.length - 1 : true);
        const shownAttempts =
          !expanded && row.repeats.length > 0 ? `${row.error.attempt ?? "?"}–${last.attempt ?? "?"}` : String(error.attempt ?? "—");
        return (
          <tr key={index} className="border-t align-top">
            <td className="py-1.5 pr-3 text-muted-fg">{isFirst ? runLabel : null}</td>
            <td className="py-1.5 pr-3 tabular-nums">{shownAttempts}</td>
            <td className="py-1.5 pr-3 whitespace-nowrap">
              <Time value={(!expanded && row.repeats.length > 0 ? last : error).at} />
            </td>
            <td className="py-1.5 pr-3 whitespace-nowrap tabular-nums text-muted-fg">
              {isFirst && row.gapMs !== null ? `+${formatDuration(row.gapMs / 1000)}` : ""}
            </td>
            <td className="py-1.5 pr-3 whitespace-nowrap">
              {outcome(!expanded ? last : error, isFinalEntry, jobState)}
            </td>
            <td className="max-w-md py-1.5">
              <span className="block truncate" title={errorSummary(error)}>
                {errorSummary(error) || "—"}
              </span>
              {isFirst && row.repeats.length > 0 && (
                <Button
                  intent="plain"
                  size="xs"
                  className="mt-0.5 -ml-2 text-muted-fg"
                  aria-expanded={expanded}
                  onPress={() => setExpanded((value) => !value)}
                >
                  {expanded ? "Collapse" : `Same error ×${row.repeats.length + 1}; show each`}
                </Button>
              )}
            </td>
          </tr>
        );
      })}
    </>
  );
}

/**
 * A job's errors: the latest one formatted, then a table of every failed
 * attempt. Entries carry when an attempt failed but not when it started, so
 * the table shows the gap between failures, not how long each attempt ran.
 */
export function ErrorBlock({
  errors,
  jobState,
  lastAttempt,
}: {
  errors: unknown[] | null | undefined;
  /** "dead_lettered" for a DLQ row. */
  jobState: string;
  /** When the final attempt started and ended, if the job has finished. */
  lastAttempt?: { startedAt: string | null; finishedAt: string | null };
}) {
  const [showRaw, setShowRaw] = useState(false);
  const latest = latestError(errors);
  if (!latest || !errors) return null;
  const runs = groupAttempts(errors);
  const multipleRuns = runs.length > 1;
  const lastRun = runs[runs.length - 1];
  const finalTook =
    lastAttempt?.startedAt && lastAttempt.finishedAt
      ? (new Date(lastAttempt.finishedAt).getTime() - new Date(lastAttempt.startedAt).getTime()) / 1000
      : null;

  return (
    <Card data-testid="error-block">
      <CardHeader>
        <div className="flex flex-wrap items-center justify-between gap-2">
          <span className="font-semibold text-danger-subtle-fg">
            Errors ({errors.length.toLocaleString()} {errors.length === 1 ? "entry" : "entries"}
            {multipleRuns ? ` in ${runs.length} runs` : ""})
          </span>
          <Button intent="outline" size="xs" aria-pressed={showRaw} onPress={() => setShowRaw((value) => !value)}>
            {showRaw ? "Formatted" : "Raw JSON"}
          </Button>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {showRaw ? (
          <JsonView data={errors} />
        ) : (
          <>
            <section aria-label="Latest error" className="space-y-2" data-testid="latest-error">
              <div className="group flex flex-wrap items-start gap-x-2 gap-y-1">
                <p className="min-w-0 flex-1 break-words">
                  {latest.exceptionClass && <span className="font-semibold">{latest.exceptionClass}: </span>}
                  <span>{latest.message || "(no message)"}</span>
                </p>
                <span className="[&_button]:opacity-100">
                  <CopyButton value={fullText(latest)} label="Copy error" />
                </span>
              </div>
              <div className="flex flex-wrap items-center gap-2 text-xs text-muted-fg">
                {latest.terminal && <Badge intent="danger">Terminal</Badge>}
                {latest.attempt !== null && <span>Attempt {latest.attempt}</span>}
                {latest.at && (
                  <span>
                    failed <Time value={latest.at} />
                  </span>
                )}
                {finalTook !== null && finalTook >= 0 && <span>· the last attempt ran {formatDuration(finalTook)}</span>}
              </div>
              {latest.traceback && <Traceback text={latest.traceback} />}
            </section>

            {errors.length > 1 && (
              <div className="overflow-x-auto">
                <table className="w-full text-left text-sm" aria-label="Failed attempts">
                  <thead className="text-xs text-muted-fg">
                    <tr>
                      <th className="py-1 pr-3 font-medium">{multipleRuns ? "Run" : <span className="sr-only">Run</span>}</th>
                      <th className="py-1 pr-3 font-medium">Attempt</th>
                      <th className="py-1 pr-3 font-medium">Failed</th>
                      <th className="py-1 pr-3 font-medium">Gap</th>
                      <th className="py-1 pr-3 font-medium">Outcome</th>
                      <th className="py-1 font-medium">Error</th>
                    </tr>
                  </thead>
                  <tbody>
                    {runs.map((run) => (
                      <Fragment key={run.run}>
                        {run.rows.map((row, index) => (
                          <AttemptRows
                            key={index}
                            row={row}
                            runLabel={multipleRuns && index === 0 ? `Run ${run.run}` : null}
                            isLastRow={run === lastRun && index === run.rows.length - 1}
                            jobState={jobState}
                          />
                        ))}
                      </Fragment>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </>
        )}
      </CardContent>
    </Card>
  );
}
