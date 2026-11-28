import { Fragment } from "react";
import { useNow } from "@/hooks/use-now";
import { useTimeZone } from "@/hooks/use-time-zone";
import { jobSummary, type SummaryJob } from "@/lib/job-summary";
import { formatAbsolute } from "@/lib/time";

/** One line under a job's title saying where it stands right now. */
export function JobStateSummary({ job }: { job: SummaryJob }) {
  const now = useNow();
  const [tz] = useTimeZone();
  const facts = jobSummary(job, now);
  if (facts.length === 0) return null;
  return (
    <p className="text-sm text-muted-fg" data-testid="job-summary" data-state={job.state}>
      {facts.map((fact, index) => (
        <Fragment key={fact.key}>
          {index > 0 && <span aria-hidden="true"> · </span>}
          <span
            title={fact.at ? formatAbsolute(fact.at, tz) : undefined}
            className={fact.warning ? "font-medium text-warning-subtle-fg" : index === 0 ? "text-fg" : undefined}
            data-fact={fact.key}
          >
            {fact.text}
          </span>
          {index < facts.length - 1 && <span className="sr-only">, </span>}
        </Fragment>
      ))}
    </p>
  );
}
