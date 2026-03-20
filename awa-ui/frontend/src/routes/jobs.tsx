import {
  useQuery,
  useMutation,
  useQueryClient,
} from "@tanstack/react-query";
import { useState, useCallback, useMemo } from "react";
import { useNavigate, useSearch } from "@tanstack/react-router";
import {
  fetchJobs,
  fetchStats,
  fetchQueues,
  bulkRetry,
  bulkCancel,
  pauseQueue,
  resumeQueue,
  drainQueue,
} from "@/lib/api";
import { toast } from "@/components/ui/toast";
import type { JobRow, ListJobsParams, StateCounts, QueueStats } from "@/lib/api";
import { StateBadge } from "@/components/StateBadge";
import { SearchBar, parseSearch } from "@/components/SearchBar";
import { Heading } from "@/components/ui/heading";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import {
  Table,
  TableHeader,
  TableBody,
  TableRow,
  TableCell,
  TableColumn,
} from "@/components/ui/table";

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

const DEFAULT_PAGE_SIZE = 50;
const PAGE_SIZES = [20, 50, 100] as const;

function timeAgo(dateStr: string): string {
  const seconds = Math.floor(
    (Date.now() - new Date(dateStr).getTime()) / 1000
  );
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

/** Show the most relevant timestamp based on job state */
function contextualTime(job: { state: string; created_at: string; attempted_at: string | null; finalized_at: string | null }): string {
  if (job.finalized_at && ["completed", "failed", "cancelled"].includes(job.state)) {
    return timeAgo(job.finalized_at);
  }
  if (job.attempted_at && ["running", "waiting_external"].includes(job.state)) {
    return timeAgo(job.attempted_at);
  }
  return timeAgo(job.created_at);
}

/** Label for contextual time column header */
function timeColumnLabel(state: string): string {
  if (["completed", "failed", "cancelled"].includes(state)) return "Finalized";
  if (["running", "waiting_external"].includes(state)) return "Started";
  return "Created";
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

export function JobsPage() {
  const navigate = useNavigate();
  const filters = useJobFilters();
  const [selected, setSelected] = useState<Set<number>>(new Set());
  const queryClient = useQueryClient();

  const searchFilters = parseSearch(filters.search);

  const params: ListJobsParams = {
    state: filters.state === "all" ? undefined : filters.state,
    kind: searchFilters.kind,
    queue: searchFilters.queue,
    tag: searchFilters.tag,
    before_id: filters.beforeId,
    limit: filters.limit,
  };

  const hasSel = selected.size > 0;

  const jobsQuery = useQuery<JobRow[]>({
    queryKey: ["jobs", params],
    queryFn: () => fetchJobs(params),
    refetchInterval: hasSel ? false : 2000,
  });

  const statsQuery = useQuery<StateCounts>({
    queryKey: ["stats"],
    queryFn: fetchStats,
  });

  const retryMutation = useMutation({
    mutationFn: (ids: number[]) => bulkRetry(ids),
    onSuccess: (_data, ids) => {
      setSelected(new Set());
      void queryClient.invalidateQueries({ queryKey: ["jobs"] });
      void queryClient.invalidateQueries({ queryKey: ["stats"] });
      toast.success(`Retried ${ids.length} job(s)`);
    },
    onError: () => {
      toast.error("Failed to retry jobs");
    },
  });

  const cancelMutation = useMutation({
    mutationFn: (ids: number[]) => bulkCancel(ids),
    onSuccess: (_data, ids) => {
      setSelected(new Set());
      void queryClient.invalidateQueries({ queryKey: ["jobs"] });
      void queryClient.invalidateQueries({ queryKey: ["stats"] });
      toast.success(`Cancelled ${ids.length} job(s)`);
    },
    onError: () => {
      toast.error("Failed to cancel jobs");
    },
  });

  const jobs = jobsQuery.data ?? [];
  const [showCancelConfirm, setShowCancelConfirm] = useState(false);
  const [showDrainConfirm, setShowDrainConfirm] = useState(false);

  // Queue context: when filtering by a single queue, show queue actions
  const activeQueue = searchFilters.queue;
  const queuesQuery = useQuery<QueueStats[]>({
    queryKey: ["queues"],
    queryFn: fetchQueues,
    enabled: !!activeQueue,
  });
  const queueStats = activeQueue
    ? queuesQuery.data?.find((q) => q.queue === activeQueue)
    : undefined;

  const pauseQueueMutation = useMutation({
    mutationFn: () => pauseQueue(activeQueue!, "ui"),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["queues"] });
      toast.success(`Queue "${activeQueue}" paused`);
    },
    onError: () => toast.error("Failed to pause queue"),
  });
  const resumeQueueMutation = useMutation({
    mutationFn: () => resumeQueue(activeQueue!),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["queues"] });
      toast.success(`Queue "${activeQueue}" resumed`);
    },
    onError: () => toast.error("Failed to resume queue"),
  });
  const drainQueueMutation = useMutation({
    mutationFn: () => drainQueue(activeQueue!),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["queues"] });
      void queryClient.invalidateQueries({ queryKey: ["jobs"] });
      toast.success(`Queue "${activeQueue}" drained`);
    },
    onError: () => toast.error("Failed to drain queue"),
  });

  // Compute which selected jobs are retryable vs cancellable
  const RETRYABLE_STATES = new Set(["failed", "cancelled", "waiting_external"]);
  const TERMINAL_STATES = new Set(["completed", "failed", "cancelled"]);
  const { retryableIds, cancellableIds } = useMemo(() => {
    const selectedJobs = jobs.filter((j) => selected.has(j.id));
    return {
      retryableIds: selectedJobs
        .filter((j) => RETRYABLE_STATES.has(j.state))
        .map((j) => j.id),
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
        to: "/jobs",
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

  const setSearch = (q: string) => {
    setUrlParams({ q: q || undefined, before_id: undefined });
  };


  const setBeforeId = (id: number | undefined) => {
    setUrlParams({
      before_id: id !== undefined ? String(id) : undefined,
    });
  };

  const total = statsQuery.data
    ? Object.values(statsQuery.data).reduce((a, b) => a + b, 0)
    : undefined;

  const lastJob = jobs[jobs.length - 1];

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-4">
        <Heading level={2}>
          {activeQueue ? `Queue: ${activeQueue}` : "Jobs"}
        </Heading>
        {total !== undefined && !activeQueue && (
          <span className="text-sm text-muted-fg">
            {total.toLocaleString()} total
          </span>
        )}
      </div>

      {/* Queue context banner — shown when filtering by a single queue */}
      {activeQueue && queueStats && (
        <div className="flex flex-wrap items-center gap-3 rounded-lg border bg-muted/30 p-3">
          {queueStats.paused ? (
            <Badge intent="warning">Paused</Badge>
          ) : (
            <Badge intent="success">Active</Badge>
          )}
          <span className="text-sm text-muted-fg">
            {queueStats.available} available &middot;{" "}
            {queueStats.running} running &middot;{" "}
            <span className={queueStats.failed > 0 ? "text-danger" : ""}>
              {queueStats.failed} failed
            </span>{" "}
            &middot; {queueStats.completed_last_hour}/hr
          </span>
          <div className="flex gap-1">
            {queueStats.paused ? (
              <Button
                intent="outline"
                size="xs"
                onPress={() => resumeQueueMutation.mutate()}
              >
                Resume
              </Button>
            ) : (
              <Button
                intent="outline"
                size="xs"
                onPress={() => pauseQueueMutation.mutate()}
              >
                Pause
              </Button>
            )}
            <Button
              intent="outline"
              size="xs"
              className="text-danger"
              onPress={() => setShowDrainConfirm(true)}
            >
              Drain
            </Button>
          </div>
        </div>
      )}

      {/* Compact state filter pills + search on same row */}
      <div className="flex flex-wrap items-center gap-2">
        {STATES.map((s) => {
          const count = s === "all" ? total : statsQuery.data?.[s];
          const isActive = filters.state === s;
          return (
            <button
              key={s}
              onClick={() => setStateFilter(s)}
              className={[
                "inline-flex items-center gap-1.5 rounded-full border px-3 py-1 text-sm font-medium transition-colors",
                isActive
                  ? "border-primary bg-primary text-primary-fg"
                  : "border-border bg-bg text-muted-fg hover:bg-secondary hover:text-fg",
              ].join(" ")}
            >
              {s === "waiting_external" ? "waiting" : s}
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
      <SearchBar value={filters.search} onChange={setSearch} />

      {/* Bulk action toolbar */}
      {selected.size > 0 && (
        <div className="flex items-center gap-3 rounded-lg bg-muted p-3">
          <span className="text-sm font-medium">{selected.size} selected</span>
          <Badge intent="warning">Updates paused</Badge>
          <Button
            intent="primary"
            size="sm"
            onPress={() => retryMutation.mutate(retryableIds)}
            isDisabled={retryMutation.isPending || retryableIds.length === 0}
          >
            Retry{retryableIds.length > 0 ? ` (${retryableIds.length})` : ""}
          </Button>
          <Button
            intent="outline"
            size="sm"
            className="text-danger"
            onPress={() => setShowCancelConfirm(true)}
            isDisabled={cancelMutation.isPending || cancellableIds.length === 0}
          >
            Cancel{cancellableIds.length > 0 ? ` (${cancellableIds.length})` : ""}
          </Button>
        </div>
      )}

      {/* Mobile card layout */}
      <div className="space-y-2 sm:hidden">
        {jobsQuery.isLoading && (
          <p className="py-8 text-center text-sm text-muted-fg">Loading...</p>
        )}
        {!jobsQuery.isLoading && jobs.length === 0 && (
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
                  <span className="font-medium">{job.kind}</span>
                  <StateBadge state={job.state} />
                  {job.priority !== 2 && (
                    <Badge
                      intent={job.priority === 1 ? "danger" : "secondary"}
                      className="text-[10px]"
                    >
                      P{job.priority}
                    </Badge>
                  )}
                </div>
                <div className="mt-1 flex flex-wrap gap-x-3 gap-y-0.5 text-xs text-muted-fg">
                  <span>{job.queue}</span>
                  <span>{job.attempt}/{job.max_attempts}</span>
                  <span>{contextualTime(job)}</span>
                </div>
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
        selectionMode="multiple"
        selectedKeys={selected}
        onSelectionChange={(keys) => {
          if (keys === "all") {
            setSelected(new Set(jobs.map((j) => j.id)));
          } else {
            setSelected(new Set(keys as Set<number>));
          }
        }}
        className="hidden sm:table"
      >
        <TableHeader>
          <TableColumn isRowHeader>Kind</TableColumn>
          <TableColumn>State</TableColumn>
          <TableColumn>Queue</TableColumn>
          <TableColumn>Pri</TableColumn>
          <TableColumn>Attempt</TableColumn>
          <TableColumn>Tags</TableColumn>
          <TableColumn>{timeColumnLabel(filters.state)}</TableColumn>
        </TableHeader>
        <TableBody
          renderEmptyState={() =>
            jobsQuery.isLoading ? (
              <div className="flex h-32 items-center justify-center text-sm text-muted-fg">
                Loading...
              </div>
            ) : (
              <div className="flex h-32 items-center justify-center text-sm text-muted-fg">
                No jobs found.
              </div>
            )
          }
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
              <TableCell className="font-medium">{job.kind}</TableCell>
              <TableCell>
                <StateBadge state={job.state} />
              </TableCell>
              <TableCell className="text-muted-fg">{job.queue}</TableCell>
              <TableCell>
                {job.priority !== 2 ? (
                  <Badge intent={job.priority === 1 ? "danger" : "secondary"} className="text-[10px]">
                    {job.priority}
                  </Badge>
                ) : (
                  <span className="text-muted-fg/40">2</span>
                )}
              </TableCell>
              <TableCell>
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
                {contextualTime(job)}
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
              onPress={() => setBeforeId(lastJob.id)}
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
              {total !== undefined && ` of ~${total.toLocaleString()}`}
            </span>
          )}
        </div>
        <div className="flex items-center gap-1.5 text-sm text-muted-fg">
          <span>Per page:</span>
          {PAGE_SIZES.map((size) => (
            <button
              key={size}
              onClick={() =>
                setUrlParams({
                  limit: size === DEFAULT_PAGE_SIZE ? undefined : String(size),
                  before_id: undefined,
                })
              }
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

      <ConfirmDialog
        isOpen={showCancelConfirm}
        onOpenChange={setShowCancelConfirm}
        title={`Cancel ${cancellableIds.length} job(s)`}
        description={`This will cancel ${cancellableIds.length} non-terminal job(s). Running jobs will be marked as cancelled. This action cannot be undone.`}
        confirmLabel="Cancel jobs"
        confirmIntent="danger"
        onConfirm={() => cancelMutation.mutate(cancellableIds)}
        isPending={cancelMutation.isPending}
      />

      {activeQueue && (
        <ConfirmDialog
          isOpen={showDrainConfirm}
          onOpenChange={setShowDrainConfirm}
          title={`Drain queue "${activeQueue}"`}
          description="This will cancel all available, scheduled, retryable, and waiting_external jobs in this queue. Running jobs will not be affected."
          confirmLabel="Drain queue"
          confirmIntent="danger"
          onConfirm={() => drainQueueMutation.mutate()}
          isPending={drainQueueMutation.isPending}
        />
      )}
    </div>
  );
}
