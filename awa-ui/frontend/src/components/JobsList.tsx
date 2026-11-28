import { useManagedQuery } from "@/hooks/use-managed-query";
import { Time } from "@/components/Time";
import {
  useMutation,
  useQueryClient,
} from "@tanstack/react-query";
import { useState, useCallback, useMemo } from "react";
import { useNavigate, useSearch } from "@tanstack/react-router";
import {
  fetchJobs,
  fetchJobCount,
  fetchStats,
  bulkRetry,
  bulkCancel,
} from "@/lib/api";
import { nextPageAnchor } from "@/lib/query";
import { toast } from "@/components/ui/toast";
import type { JobCount, JobRow, ListJobsParams, StateCounts } from "@/lib/api";
import { StateBadge } from "@/components/StateBadge";
import { jobStateMeta, TONE_TEXT } from "@/lib/states";
import { CheckIcon } from "@heroicons/react/16/solid";
import { SearchBar, parseSearch, termError, toRfc3339 } from "@/components/SearchBar";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { ConfirmAction } from "@/components/ConfirmAction";
import { AdminButton } from "@/components/AdminAction";
import { BulkScopeBar } from "@/components/BulkScopeBar";
import { JobLink, KindLink } from "@/components/links";
import { jobActionCopy, selectionCopy, selectionOutcome } from "@/lib/job-actions";
import { selectionTier } from "@/lib/confirm";
import {
  Table,
  TableHeader,
  TableBody,
  TableRow,
  TableCell,
  TableColumn,
} from "@/components/ui/table";
import {
  DEFAULT_PAGE_SIZE,
  PAGE_SIZES,
  RETRYABLE_STATES,
  TERMINAL_STATES,
} from "@/lib/constants";

const STATES = [
  "all",
  "available",
  "running",
  "scheduled",
  "retryable",
  "failed",
  "completed",
  "cancelled",
  "waiting_external",
] as const;

/** The most relevant timestamp for the job's state. */
function contextualTime(job: { state: string; created_at: string; attempted_at: string | null; finalized_at: string | null }): string {
  if (job.finalized_at && ["completed", "failed", "cancelled"].includes(job.state)) {
    return job.finalized_at;
  }
  if (job.attempted_at && ["running", "waiting_external"].includes(job.state)) {
    return job.attempted_at;
  }
  return job.created_at;
}

/** Label for contextual time column header */
function timeColumnLabel(state: string): string {
  if (["completed", "failed", "cancelled"].includes(state)) return "Finalized";
  if (["running", "waiting_external"].includes(state)) return "Started";
  return "Created";
}

// Treat empty display_name as missing — `??` alone would let
// `display_name: ""` render a blank label.
function jobKindLabel(job: JobRow): string {
  return job.kind_descriptor?.display_name?.trim()
    ? job.kind_descriptor.display_name
    : job.kind;
}

function queueLabel(job: JobRow): string {
  return job.queue_descriptor?.display_name?.trim()
    ? job.queue_descriptor.display_name
    : job.queue;
}

function lockedTerms(locked: JobsListProps["lockedFilter"]): string[] {
  if (!locked) return [];
  return [locked.kind && `kind:${locked.kind}`, locked.queue && `queue:${locked.queue}`].filter(
    (term): term is string => Boolean(term)
  );
}

function useJobFilters() {
  const searchParams = useSearch({ strict: false }) as Record<
    string,
    string | undefined
  >;
  return {
    state: searchParams.state ?? "all",
    search: searchParams.q ?? "",
    beforeId: searchParams.before_id
      ? Number(searchParams.before_id)
      : undefined,
    limit: searchParams.limit ? Number(searchParams.limit) : DEFAULT_PAGE_SIZE,
  };
}

export interface JobsListProps {
  /** Filter terms the page fixes; they apply to every read and can't be
   * removed from the search. */
  lockedFilter?: { queue?: string; kind?: string };
  /** Per-state counts for the pills when the global ones don't apply, e.g.
   * one queue's. Without them the pills show the whole install's counts. */
  stateCounts?: Partial<StateCounts>;
}

/** The filterable jobs list: state pills, search, selection actions, table
 * and phone cards, and pagination. Its filter lives in the current URL's
 * `state`, `q`, `before_id` and `limit`. */
export function JobsList({ lockedFilter, stateCounts }: JobsListProps) {
  const navigate = useNavigate();
  const filters = useJobFilters();
  const [selected, setSelected] = useState<Set<number>>(new Set());
  const queryClient = useQueryClient();

  const searchFilters = { ...parseSearch(filters.search), ...lockedFilter };

  const filterParams: ListJobsParams = {
    states: filters.state === "all" ? undefined : [filters.state],
    kind: searchFilters.kind,
    queue: searchFilters.queue,
    tags: searchFilters.tags,
    ids: searchFilters.ids,
    created_at_gte: toRfc3339(searchFilters.createdGte),
    created_at_lt: toRfc3339(searchFilters.createdLt),
  };
  const params: ListJobsParams = {
    ...filterParams,
    before_id: filters.beforeId,
    limit: filters.limit,
  };
  const hasFilter =
    filters.state !== "all" || Object.keys(searchFilters).length > 0;

  const hasSel = selected.size > 0;
  const searchError = termError(filters.search);

  const jobsQuery = useManagedQuery<JobRow[]>({
    queryKey: ["jobs", params],
    queryFn: () => fetchJobs(params),
    enabled: !searchError,
    paused: hasSel,
  });

  // The new-operation form reads tags as a comma list, so a tag with a
  // comma can't be carried over.
  const tagWithComma = (searchFilters.tags ?? []).some((tag) => tag.includes(","));
  const batchSearch: Record<string, string> = Object.fromEntries(
    Object.entries({
      kind: searchFilters.kind,
      queue: searchFilters.queue,
      tags: searchFilters.tags?.join(", "),
      ids: searchFilters.ids?.join(", "),
      state: filters.state === "all" ? undefined : filters.state,
      created_at_gte: searchFilters.createdGte,
      created_at_lt: searchFilters.createdLt,
    }).filter((entry): entry is [string, string] => Boolean(entry[1]))
  );

  // Counted when the filter changes, not on every poll: a count reads the
  // whole matching population.
  const countQuery = useManagedQuery<JobCount>({
    queryKey: ["jobs-count", filterParams],
    queryFn: () => fetchJobCount(filterParams),
    enabled: hasFilter && !searchError,
    staleTime: 30_000,
  });

  // Bulk actions on the whole filter change live jobs only. Dead letters are
  // failed, so only an all-states or failed filter can match any of them.
  const mayMatchDeadLetters = filters.state === "all" || filters.state === "failed";
  const liveFilterParams: ListJobsParams = { ...filterParams, dead_lettered: false };
  const liveCountQuery = useManagedQuery<JobCount>({
    queryKey: ["jobs-count", liveFilterParams],
    queryFn: () => fetchJobCount(liveFilterParams),
    enabled: hasFilter && !searchError && mayMatchDeadLetters,
    staleTime: 30_000,
  });
  const liveCount = mayMatchDeadLetters ? liveCountQuery.data : countQuery.data;

  const statsQuery = useManagedQuery<StateCounts>({
    queryKey: ["stats"],
    queryFn: fetchStats,
    enabled: !stateCounts,
  });
  const counts: Partial<StateCounts> | undefined = stateCounts ?? statsQuery.data;

  const finishSelection = (action: "retry" | "cancel", requested: number, changed: JobRow[]) => {
    const outcome = selectionOutcome(action, selectedJobs, requested, changed.length);
    setSelected(new Set());
    void queryClient.invalidateQueries({ queryKey: ["jobs"] });
    void queryClient.invalidateQueries({ queryKey: ["jobs-count"] });
    void queryClient.invalidateQueries({ queryKey: ["stats"] });
    toast.success(outcome.message, outcome.detail ? { description: outcome.detail } : undefined);
  };
  // Run from a confirmation dialog, which shows their errors.
  const retryMutation = useMutation({
    mutationFn: (ids: number[]) => bulkRetry(ids),
    onSuccess: (changed, ids) => finishSelection("retry", ids.length, changed),
  });
  const cancelMutation = useMutation({
    mutationFn: (ids: number[]) => bulkCancel(ids),
    onSuccess: (changed, ids) => finishSelection("cancel", ids.length, changed),
  });

  // A malformed term is left out of the query key, so the cache can hold the
  // wider search's rows; show none of them beside the error.
  const jobs = searchError ? [] : jobsQuery.data ?? [];
  const [confirming, setConfirming] = useState<"retry" | "cancel" | null>(null);
  const [lastConfirming, setLastConfirming] = useState<"retry" | "cancel">("retry");
  const confirm = (action: "retry" | "cancel") => {
    setLastConfirming(action);
    setConfirming(action);
  };

  // Compute which selected jobs are retryable vs cancellable. Dead-lettered
  // jobs are retried through the DLQ page; the job endpoints skip them.
  const { selectedJobs, retryableIds, waitingRetryCount, autoRetryingCount, cancellableIds } = useMemo(() => {
    const selectedJobs = jobs.filter((j) => selected.has(j.id) && !j.dlq);
    return {
      selectedJobs,
      retryableIds: selectedJobs
        .filter((j) => RETRYABLE_STATES.has(j.state))
        .map((j) => j.id),
      waitingRetryCount: selectedJobs.filter((j) => j.state === "waiting_external").length,
      autoRetryingCount: selectedJobs.filter((j) => j.state === "retryable").length,
      cancellableIds: selectedJobs
        .filter((j) => !TERMINAL_STATES.has(j.state))
        .map((j) => j.id),
    };
  }, [jobs, selected]);

  const setUrlParams = useCallback(
    (updates: Record<string, string | undefined>) => {
      const current = new URLSearchParams(window.location.search);
      for (const [k, v] of Object.entries(updates)) {
        if (v === undefined || v === "" || v === "all") {
          current.delete(k);
        } else {
          current.set(k, v);
        }
      }
      void navigate({
        to: ".",
        search: Object.fromEntries(current.entries()),
        replace: true,
      });
    },
    [navigate]
  );

  const setStateFilter = (state: string) => {
    setSelected(new Set());
    setUrlParams({
      state: state === "all" ? undefined : state,
      before_id: undefined,
    });
  };

  // Any change that shifts the visible result set clears bulk selection —
  // otherwise the toolbar count reflects ids that are no longer visible.
  const setSearch = (q: string) => {
    setSelected(new Set());
    setUrlParams({ q: q || undefined, before_id: undefined });
  };

  const setBeforeId = (id: number | undefined) => {
    setSelected(new Set());
    setUrlParams({
      before_id: id !== undefined ? String(id) : undefined,
    });
  };

  const total = statsQuery.data && !stateCounts
    ? Object.values(statsQuery.data).reduce((a, b) => a + b, 0)
    : undefined;

  const lastJob = jobs[jobs.length - 1];
  const nextAnchor = nextPageAnchor(jobs, filters.limit, jobsQuery);

  return (
    <div className="space-y-4" data-testid="jobs-list">
      {jobsQuery.isPlaceholderData && <p role="status">Previous filter results are shown while the new read finishes.</p>}

      {/* Compact state filter pills + search on same row */}
      <div className="flex flex-wrap items-center gap-2">
        {STATES.map((s) => {
          const count = s === "all" ? total : counts?.[s];
          const isActive = filters.state === s;
          const meta = s === "all" ? null : jobStateMeta(s);
          const Icon = isActive ? CheckIcon : meta?.Icon;
          return (
            <button
              key={s}
              type="button"
              aria-pressed={isActive}
              data-testid="state-pill"
              data-state={s}
              onClick={() => setStateFilter(s)}
              className={[
                "inline-flex items-center gap-1.5 rounded-full border px-3 py-1 text-sm font-medium transition-colors",
                isActive
                  ? "border-primary bg-primary text-primary-fg"
                  : "border-border bg-bg text-muted-fg hover:bg-secondary hover:text-fg",
              ].join(" ")}
            >
              {Icon && (
                <Icon
                  aria-hidden="true"
                  className={`size-3.5 ${isActive || !meta ? "" : TONE_TEXT[meta.tone]}`}
                />
              )}
              {meta ? meta.shortLabel : "All"}
              {count !== undefined && count > 0 && (
                <span
                  className={
                    isActive
                      ? "text-primary-fg/80 text-xs"
                      : "text-muted-fg text-xs"
                  }
                >
                  {count.toLocaleString()}
                </span>
              )}
            </button>
          );
        })}
      </div>

      {/* Search bar with integrated filter chips */}
      <SearchBar value={filters.search} onChange={setSearch} locked={lockedTerms(lockedFilter)} />

      {searchError && (
        <p className="text-sm text-danger" role="alert">
          {searchError}. No jobs are listed until the search is fixed.
        </p>
      )}

      {hasFilter && !searchError && (
        <div className="flex flex-wrap items-center gap-3 text-sm text-muted-fg">
          {countQuery.data && (
            <span data-testid="match-count">
              {countQuery.data.kind === "at_least"
                ? `${countQuery.data.value.toLocaleString()}+`
                : countQuery.data.value.toLocaleString()}{" "}
              matching{countQuery.isObservationStale ? " (stale)" : ""}
            </span>
          )}
          {countQuery.isError && (
            <span className="text-danger" data-testid="match-count-error">
              Count failed: {countQuery.error.message}
            </span>
          )}
        </div>
      )}

      {(hasFilter || hasSel) && (
        <BulkScopeBar
          target="jobs"
          filter={filterParams}
          count={searchError ? undefined : liveCount}
          unavailableReason={searchError ? "Fix the search first" : undefined}
          formPrefill={
            tagWithComma ? "A tag containing a comma can't be carried into the new-operation form" : batchSearch
          }
          selection={{
            count: selected.size,
            actions: (
              <>
                <AdminButton
                  intent="primary"
                  size="sm"
                  onPress={() => confirm("retry")}
                  isDisabled={retryMutation.isPending || retryableIds.length === 0}
                  disabledReason="None of the selected jobs is failed, cancelled or waiting"
                >
                  Retry{retryableIds.length > 0 ? ` (${retryableIds.length})…` : ""}
                </AdminButton>
                <AdminButton
                  intent="outline"
                  size="sm"
                  className="text-danger"
                  onPress={() => confirm("cancel")}
                  isDisabled={cancelMutation.isPending || cancellableIds.length === 0}
                  disabledReason="All the selected jobs have already finished"
                >
                  Cancel{cancellableIds.length > 0 ? ` (${cancellableIds.length})…` : ""}
                </AdminButton>
                <Badge intent="warning">Updates paused</Badge>
                {autoRetryingCount > 0 && (
                  <span className="text-xs text-muted-fg" data-testid="bulk-retry-hint">
                    {autoRetryingCount} retryable job(s) already retry automatically
                    and are not retried here.
                  </span>
                )}
              </>
            ),
          }}
        />
      )}

      {/* Mobile card layout */}
      <div className="space-y-2 sm:hidden">
        {jobsQuery.isLoading && (
          <p className="py-8 text-center text-sm text-muted-fg">Loading…</p>
        )}
        {!jobsQuery.isLoading && jobsQuery.isError && (
          <p className="py-8 text-center text-sm text-danger-subtle-fg">
            Failed to load jobs: {jobsQuery.error.message}
          </p>
        )}
        {!jobsQuery.isLoading && !jobsQuery.isError && !searchError && jobs.length === 0 && (
          <p className="py-8 text-center text-sm text-muted-fg">No jobs found.</p>
        )}
        {jobs.map((job) => (
          <div
            key={job.id}
            className={[
              "rounded-lg border p-3 transition-colors",
              selected.has(job.id) ? "border-primary bg-primary-subtle/30" : "",
            ].join(" ")}
          >
            <div className="flex items-start gap-3">
              <input
                type="checkbox"
                disabled={jobsQuery.isPlaceholderData}
                checked={selected.has(job.id)}
                onChange={() => {
                  setSelected((prev) => {
                    const next = new Set(prev);
                    if (next.has(job.id)) next.delete(job.id);
                    else next.add(job.id);
                    return next;
                  });
                }}
                className="mt-1 shrink-0"
              />
              <div
                className="min-w-0 flex-1 cursor-pointer"
                onClick={() =>
                  void navigate({
                    to: "/jobs/$id",
                    params: { id: String(job.id) },
                  })
                }
              >
                <div className="flex items-center gap-2">
                  <span className="font-medium">
                    {jobKindLabel(job)}
                    <span className="ml-1.5 text-xs text-muted-fg">#{job.id}</span>
                  </span>
                  <StateBadge state={job.state} />
                  {(job.priority !== 2 || job.original_priority !== job.priority) && (
                    <Badge
                      intent={job.priority === 1 ? "danger" : "secondary"}
                      className="text-[10px]"
                    >
                      P{job.priority}{job.original_priority !== job.priority ? ` (was ${job.original_priority})` : ""}
                    </Badge>
                  )}
                </div>
                <div className="mt-1 flex flex-wrap gap-x-3 gap-y-0.5 text-xs text-muted-fg">
                  <span>{queueLabel(job)}</span>
                  {job.queue_descriptor?.display_name && <span>{job.queue}</span>}
                  <span>{job.attempt}/{job.max_attempts}</span>
                  <Time value={contextualTime(job)} />
                </div>
                {job.kind_descriptor?.description && (
                  <p className="mt-1 text-xs text-muted-fg">
                    {job.kind_descriptor.description}
                  </p>
                )}
                {job.tags.length > 0 && (
                  <div className="mt-1.5 flex flex-wrap gap-1">
                    {job.tags.slice(0, 3).map((t) => (
                      <Badge key={t} intent="secondary" className="text-[10px]">
                        {t}
                      </Badge>
                    ))}
                    {job.tags.length > 3 && (
                      <span className="text-[10px] text-muted-fg">
                        +{job.tags.length - 3}
                      </span>
                    )}
                  </div>
                )}
              </div>
            </div>
          </div>
        ))}
      </div>

      {/* Desktop table */}
      <Table
        aria-label="Jobs"
        selectionMode={jobsQuery.isPlaceholderData ? "none" : "multiple"}
        selectedKeys={selected}
        onSelectionChange={(keys) => {
          if (jobsQuery.isPlaceholderData) return;
          if (keys === "all") {
            setSelected(new Set(jobs.map((j) => j.id)));
          } else {
            setSelected(new Set(keys as Set<number>));
          }
        }}
        className="hidden sm:block"
      >
        <TableHeader>
          <TableColumn isRowHeader>Kind</TableColumn>
          <TableColumn>State</TableColumn>
          <TableColumn>Queue</TableColumn>
          <TableColumn className="text-right">Pri</TableColumn>
          <TableColumn className="text-right">Attempt</TableColumn>
          <TableColumn>Tags</TableColumn>
          <TableColumn>{timeColumnLabel(filters.state)}</TableColumn>
        </TableHeader>
        <TableBody
          renderEmptyState={() => {
            if (jobsQuery.isLoading) {
              return (
                <div className="flex h-32 items-center justify-center text-sm text-muted-fg">
                  Loading…
                </div>
              );
            }
            if (jobsQuery.isError) {
              return (
                <div className="flex h-32 items-center justify-center text-sm text-danger-subtle-fg">
                  Failed to load jobs: {jobsQuery.error.message}
                </div>
              );
            }
            // The search error above says why nothing is listed.
            if (searchError) return null;
            return (
              <div className="flex h-32 items-center justify-center text-sm text-muted-fg">
                No jobs found.
              </div>
            );
          }}
        >
          {jobs.map((job) => (
            <TableRow
              key={job.id}
              id={job.id}
              onAction={() =>
                void navigate({
                  to: "/jobs/$id",
                  params: { id: String(job.id) },
                })
              }
              className="cursor-pointer"
            >
              <TableCell className="font-medium">
                <div>
                  <div>{jobKindLabel(job)}
                <span className="ml-1.5 text-xs text-muted-fg">#{job.id}</span>
                  </div>
                  {job.kind_descriptor?.display_name && (
                    <div className="text-xs font-normal text-muted-fg">{job.kind}</div>
                  )}
                </div>
              </TableCell>
              <TableCell>
                <StateBadge state={job.state} />
              </TableCell>
              <TableCell className="text-muted-fg">
                <div>
                  <div>{queueLabel(job)}</div>
                  {job.queue_descriptor?.display_name && (
                    <div className="text-xs">{job.queue}</div>
                  )}
                </div>
              </TableCell>
              <TableCell className="text-right tabular-nums">
                {job.priority !== 2 ? (
                  <Badge intent={job.priority === 1 ? "danger" : "secondary"} className="text-[10px]">
                    {job.original_priority !== job.priority
                      ? `${job.priority} (was ${job.original_priority})`
                      : String(job.priority)}
                  </Badge>
                ) : job.original_priority !== 2 ? (
                  <span className="text-muted-fg">2 <span className="text-[10px]">(was {job.original_priority})</span></span>
                ) : (
                  <span className="text-muted-fg">2</span>
                )}
              </TableCell>
              <TableCell className="text-right tabular-nums">
                {job.attempt}/{job.max_attempts}
              </TableCell>
              <TableCell>
                {job.tags.length > 0 ? (
                  <div className="flex flex-wrap gap-1">
                    {job.tags.slice(0, 3).map((t) => (
                      <Badge key={t} intent="secondary" className="text-[10px]">
                        {t}
                      </Badge>
                    ))}
                    {job.tags.length > 3 && (
                      <span className="text-xs text-muted-fg">
                        +{job.tags.length - 3}
                      </span>
                    )}
                  </div>
                ) : (
                  <span className="text-muted-fg">-</span>
                )}
              </TableCell>
              <TableCell className="text-muted-fg">
                <Time value={contextualTime(job)} />
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>

      {/* Pagination + page size */}
      <div className="flex flex-wrap items-center justify-between gap-3 pt-2">
        <div className="flex items-center gap-2">
          {jobs.length === filters.limit && lastJob && (
            <Button
              intent="outline"
              size="sm"
              isDisabled={!nextAnchor}
              onPress={() => {
                if (nextAnchor) setBeforeId(nextAnchor.id);
              }}
            >
              Next page
            </Button>
          )}
          {filters.beforeId !== undefined && (
            <Button
              intent="outline"
              size="sm"
              onPress={() => setBeforeId(undefined)}
            >
              Back to first
            </Button>
          )}
          {jobs.length > 0 && (
            <span className="text-sm text-muted-fg">
              Showing {jobs.length} job{jobs.length !== 1 ? "s" : ""}
              {total !== undefined && ` of ${total.toLocaleString()} total`}
            </span>
          )}
        </div>
        <div className="flex items-center gap-1.5 text-sm text-muted-fg">
          <span>Per page:</span>
          {PAGE_SIZES.map((size) => (
            <button
              key={size}
              onClick={() => {
                setSelected(new Set());
                setUrlParams({
                  limit: size === DEFAULT_PAGE_SIZE ? undefined : String(size),
                  before_id: undefined,
                });
              }}
              className={[
                "rounded px-2 py-0.5 text-sm transition-colors",
                filters.limit === size
                  ? "bg-primary text-primary-fg"
                  : "hover:bg-secondary",
              ].join(" ")}
            >
              {size}
            </button>
          ))}
        </div>
      </div>

      <SelectionConfirm
        action={lastConfirming}
        isOpen={confirming !== null}
        onOpenChange={(open) => !open && setConfirming(null)}
        jobs={jobs.filter((job) => (lastConfirming === "retry" ? retryableIds : cancellableIds).includes(job.id))}
        waiting={waitingRetryCount}
        onConfirm={(ids) => (lastConfirming === "retry" ? retryMutation.mutateAsync(ids) : cancelMutation.mutateAsync(ids))}
      />
    </div>
  );
}

const SELECTION_PREVIEW_ROWS = 10;

/** Confirms Retry or Cancel on selected rows: one job by name, several with a list of them. */
function SelectionConfirm({
  action,
  isOpen,
  onOpenChange,
  jobs,
  waiting,
  onConfirm,
}: {
  action: "retry" | "cancel";
  isOpen: boolean;
  onOpenChange: (open: boolean) => void;
  jobs: JobRow[];
  waiting: number;
  onConfirm: (ids: number[]) => Promise<unknown>;
}) {
  const tier = selectionTier(jobs.length);
  const only = jobs.length === 1 ? jobs[0] : undefined;
  const copy = only ? jobActionCopy(only, action) : selectionCopy(action, jobs.length, waiting);
  const shown = jobs.slice(0, SELECTION_PREVIEW_ROWS);
  return (
    <ConfirmAction
      isOpen={isOpen}
      onOpenChange={onOpenChange}
      tier={tier}
      {...copy}
      onConfirm={() => onConfirm(jobs.map((job) => job.id))}
    >
      {tier === 2 && (
        <ul className="divide-y rounded-md border text-sm" aria-label="Selected jobs" data-testid="selection-preview">
          {shown.map((job) => (
            <li key={job.id} className="flex items-center gap-3 px-3 py-1.5">
              <JobLink id={job.id} />
              <KindLink kind={job.kind} descriptor={job.kind_descriptor} className="min-w-0 truncate" />
              <span className="ml-auto"><StateBadge state={job.state} /></span>
            </li>
          ))}
          {jobs.length > shown.length && (
            <li className="px-3 py-1.5 text-muted-fg">and {(jobs.length - shown.length).toLocaleString()} more</li>
          )}
        </ul>
      )}
    </ConfirmAction>
  );
}
