import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useParams } from "@tanstack/react-router";
import { useManagedQuery } from "@/hooks/use-managed-query";
import { usePollInterval } from "@/hooks/use-poll-interval";
import {
  fetchCronJobs,
  fetchJobCount,
  toWireFilter,
  fetchJobs,
  fetchKinds,
  type CronJobRow,
  type JobCount,
  type JobKindOverview,
  type JobRow,
} from "@/lib/api";
import { SECTION_LABELS } from "@/lib/sections";
import { bulkActionReason } from "@/lib/batch";
import { groupFailures } from "@/lib/errors";
import { AdminButton } from "@/components/AdminAction";
import { BulkActionDialog } from "@/components/BulkActionDialog";
import { formatNumber } from "@/lib/number";
import { Page, PageHeader } from "@/components/Page";
import { FilterLink, JobLink, QueueLink, ScheduleLink } from "@/components/links";
import { ListState } from "@/components/ListState";
import { ResourceLoadError, ResourceNotFound } from "@/components/ResourceNotFound";
import { JsonView } from "@/components/JsonView";
import { CronExpr } from "@/components/CronExpr";
import { Time } from "@/components/Time";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import {
  DescriptionDetails,
  DescriptionList,
  DescriptionTerm,
} from "@/components/ui/description-list";

/** How many recent failures the page reads to show and to find queues. */
export const KIND_FAILURE_SAMPLE = 50;
const FAILURE_GROUPS_SHOWN = 10;

function kindLabel(kind: JobKindOverview): string {
  return kind.display_name?.trim() ? kind.display_name : kind.kind;
}

function CountStat({
  label,
  count,
  isError,
  state,
  kind,
}: {
  label: string;
  count: JobCount | undefined;
  isError: boolean;
  state: string;
  kind: string;
}) {
  const text = isError
    ? "—"
    : count === undefined
      ? "…"
      : `${formatNumber(count.value)}${count.kind === "at_least" ? "+" : ""}`;
  return (
    <div className="rounded-lg border p-3" data-testid={`kind-${state}-count`}>
      <div className="text-xs text-muted-fg">{label}</div>
      <FilterLink filter={{ kind }} state={state} className="mt-1 block text-2xl font-semibold tabular-nums">
        {text}
      </FilterLink>
    </div>
  );
}

export function KindDetailPage() {
  const { kind: kindName = "" } = useParams({ strict: false }) as { kind?: string };
  const poll = usePollInterval();

  const kindsQuery = useQuery<JobKindOverview[]>({
    queryKey: ["kinds"],
    queryFn: fetchKinds,
    refetchInterval: poll.interval,
    staleTime: poll.staleTime,
  });
  const failedFilter = { kind: kindName, states: ["failed"] };
  const retryableFilter = { kind: kindName, states: ["retryable"] };
  // Counts read every matching job, so they refresh on demand, not per poll.
  const failedCount = useManagedQuery<JobCount>({
    queryKey: ["jobs-count", failedFilter],
    queryFn: () => fetchJobCount(failedFilter),
    staleTime: 30_000,
  });
  const retryableCount = useManagedQuery<JobCount>({
    queryKey: ["jobs-count", retryableFilter],
    queryFn: () => fetchJobCount(retryableFilter),
    staleTime: 30_000,
  });
  const failuresParams = { kind: kindName, state: "failed", limit: KIND_FAILURE_SAMPLE };
  const failures = useManagedQuery<JobRow[]>({
    queryKey: ["jobs", failuresParams],
    queryFn: () => fetchJobs(failuresParams),
  });
  const cronQuery = useManagedQuery<CronJobRow[]>({ queryKey: ["cron"], queryFn: fetchCronJobs });

  const kind = kindsQuery.data?.find((entry) => entry.kind === kindName);
  if (kindsQuery.isLoading) return <p className="text-sm text-muted-fg">Loading…</p>;
  if (!kind) {
    if (kindsQuery.isError) {
      return (
        <ResourceLoadError
          noun={`job kind "${kindName}"`}
          error={kindsQuery.error}
          onRetry={() => void kindsQuery.refetch()}
          backTo="/kinds"
          backLabel="Back to job kinds"
        />
      );
    }
    return (
      <ResourceNotFound
        title={`Job kind "${kindName}" not found`}
        reason="No retained job or declared descriptor has this kind."
        backTo="/kinds"
        backLabel="Back to job kinds"
      />
    );
  }

  const schedules = (cronQuery.data ?? []).filter((cron) => cron.kind === kind.kind);
  const failedJobs = failures.data ?? [];
  const queues = [...new Set([...failedJobs.map((job) => job.queue), ...schedules.map((cron) => cron.queue)])].sort();
  const label = kindLabel(kind);

  return (
    <Page width="readable">
      <PageHeader
        breadcrumbs={[{ label: SECTION_LABELS["/kinds"], href: "/kinds" }]}
        title={label}
        status={
          <>
            {label !== kind.kind && (
              <code className="rounded bg-muted px-1.5 py-0.5 text-sm">{kind.kind}</code>
            )}
            {!kind.descriptor_last_seen_at && <Badge intent="secondary">Not declared</Badge>}
            {kind.descriptor_stale && <Badge intent="secondary">Descriptor stale</Badge>}
            {kind.descriptor_mismatch && <Badge intent="warning">Descriptor drift</Badge>}
          </>
        }
        description={kind.description || undefined}
        actions={
          <>
            <FilterLink filter={{ kind: kind.kind }} className="text-sm">
              All jobs of this kind
            </FilterLink>
            <RetryAllFailed kind={kind.kind} count={failedCount.data} />
          </>
        }
      />

      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        <div className="rounded-lg border p-3">
          <div className="text-xs text-muted-fg">Retained jobs</div>
          <div className="mt-1 text-2xl font-semibold tabular-nums">{formatNumber(kind.job_count)}</div>
        </div>
        <div className="rounded-lg border p-3">
          <div className="text-xs text-muted-fg">Completed in the last hour</div>
          <div className="mt-1 text-2xl font-semibold tabular-nums">{formatNumber(kind.completed_last_hour)}</div>
        </div>
        <CountStat label="Failed" count={failedCount.data} isError={failedCount.isError} state="failed" kind={kind.kind} />
        <CountStat label="Retryable" count={retryableCount.data} isError={retryableCount.isError} state="retryable" kind={kind.kind} />
      </div>

      <Card>
        <CardHeader title="Descriptor" />
        <CardContent>
          <DescriptionList>
            <DescriptionTerm>Kind</DescriptionTerm>
            <DescriptionDetails>
              <code className="rounded bg-muted px-1.5 py-0.5 text-sm">{kind.kind}</code>
            </DescriptionDetails>
            <DescriptionTerm>Owner</DescriptionTerm>
            <DescriptionDetails>{kind.owner ?? "—"}</DescriptionDetails>
            <DescriptionTerm>Docs</DescriptionTerm>
            <DescriptionDetails>
              {kind.docs_url ? (
                <a href={kind.docs_url} target="_blank" rel="noreferrer" className="text-primary no-underline hover:underline">
                  {kind.docs_url}
                </a>
              ) : (
                "—"
              )}
            </DescriptionDetails>
            <DescriptionTerm>Output</DescriptionTerm>
            <DescriptionDetails>
              {kind.output ? (
                <div>
                  <code className="rounded bg-muted px-1 py-0.5 text-sm">{kind.output.type}</code>
                  {kind.output.schema !== undefined && kind.output.schema !== null && (
                    <details className="mt-1">
                      <summary className="cursor-pointer text-sm">Schema</summary>
                      <div className="mt-1">
                        <JsonView data={kind.output.schema} />
                      </div>
                    </details>
                  )}
                </div>
              ) : (
                "Not declared"
              )}
            </DescriptionDetails>
            <DescriptionTerm>Last declared</DescriptionTerm>
            <DescriptionDetails>
              <Time value={kind.descriptor_last_seen_at} fallback="Never" />
            </DescriptionDetails>
            {kind.tags.length > 0 && (
              <>
                <DescriptionTerm>Descriptor tags</DescriptionTerm>
                <DescriptionDetails>
                  <div className="flex flex-wrap gap-1">
                    {kind.tags.map((tag) => (
                      <Badge key={tag} intent="secondary">{tag}</Badge>
                    ))}
                  </div>
                </DescriptionDetails>
              </>
            )}
          </DescriptionList>
        </CardContent>
      </Card>

      <Card>
        <CardHeader
          title="Recent failures by error"
          description={`The ${KIND_FAILURE_SAMPLE} most recent failed jobs of this kind, grouped by error class.`}
        />
        <CardContent>
          {failedJobs.length === 0 ? (
            <ListState
              isLoading={failures.isLoading}
              error={failures.isError ? failures.error : null}
              onRetry={() => void failures.refetch()}
              noun="failed jobs"
              empty="No failed jobs of this kind."
            />
          ) : (
            <ul className="divide-y text-sm" data-testid="kind-failures">
              {groupFailures(failedJobs).slice(0, FAILURE_GROUPS_SHOWN).map((group) => (
                <li key={group.key} className="flex flex-wrap items-baseline gap-x-3 gap-y-1 py-2">
                  <span className="tabular-nums font-medium">{formatNumber(group.count)}×</span>
                  <span className="min-w-0 flex-1 truncate text-danger-subtle-fg" title={group.summary}>
                    {group.summary}
                  </span>
                  <span className="text-xs text-muted-fg">
                    newest <JobLink id={group.jobIds[0]!} />
                  </span>
                </li>
              ))}
            </ul>
          )}
        </CardContent>
      </Card>

      <div className="grid gap-6 md:grid-cols-2">
        <Card>
          <CardHeader title="Schedules that enqueue it" />
          <CardContent>
            {schedules.length === 0 ? (
              <ListState
                isLoading={cronQuery.isLoading}
                error={cronQuery.isError ? cronQuery.error : null}
                onRetry={() => void cronQuery.refetch()}
                noun="schedules"
                empty="No schedule enqueues this kind."
              />
            ) : (
              <ul className="space-y-2 text-sm" data-testid="kind-schedules">
                {schedules.map((cron) => (
                  <li key={cron.name} className="flex flex-wrap items-center gap-2">
                    <ScheduleLink name={cron.name} />
                    <CronExpr expr={cron.cron_expr} />
                  </li>
                ))}
              </ul>
            )}
          </CardContent>
        </Card>
        <Card>
          <CardHeader
            title="Queues"
            description={`Seen in its ${KIND_FAILURE_SAMPLE} most recent failures and its schedules, so the list may be incomplete. It runs in ${kind.queue_count} queue(s) in all.`}
          />
          <CardContent>
            {queues.length === 0 ? (
              <p className="text-sm text-muted-fg">None seen.</p>
            ) : (
              <ul className="flex flex-wrap gap-x-4 gap-y-1 text-sm" data-testid="kind-queues">
                {queues.map((queue) => (
                  <li key={queue}>
                    <QueueLink queue={queue} />
                  </li>
                ))}
              </ul>
            )}
          </CardContent>
        </Card>
      </div>
    </Page>
  );
}

/** Retries every failed job of a kind as one bulk operation. */
function RetryAllFailed({ kind, count }: { kind: string; count: JobCount | undefined }) {
  const [open, setOpen] = useState(false);
  const filter = toWireFilter({ kind, states: ["failed"] });
  const reason = bulkActionReason("retry", { filter, states: ["failed"], count });
  return (
    <>
      <AdminButton
        size="sm"
        intent="outline"
        isDisabled={reason !== null}
        disabledReason={reason ?? undefined}
        onPress={() => setOpen(true)}
      >
        Retry all failed{count ? ` (${count.kind === "at_least" ? `${formatNumber(count.value)}+` : formatNumber(count.value)})` : ""}…
      </AdminButton>
      <BulkActionDialog
        isOpen={open}
        onOpenChange={setOpen}
        payload={{ op_kind: "retry", filter, spec: {}, submitted_by: "ui" }}
      />
    </>
  );
}
