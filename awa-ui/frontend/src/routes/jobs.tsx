import {
  useQuery,
  useMutation,
  useQueryClient,
} from "@tanstack/react-query";
import { useState, useCallback } from "react";
import { useNavigate, useSearch } from "@tanstack/react-router";
import { fetchJobs, fetchStats, bulkRetry, bulkCancel } from "@/lib/api";
import { toast } from "@/components/ui/toast";
import type { JobRow, ListJobsParams, StateCounts } from "@/lib/api";
import { StateBadge } from "@/components/StateBadge";
import { SearchBar, parseSearch } from "@/components/SearchBar";
import { Heading } from "@/components/ui/heading";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
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

const PAGE_SIZE = 50;

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
    limit: searchParams.limit ? Number(searchParams.limit) : PAGE_SIZE,
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
    refetchInterval: hasSel ? false : 5000,
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
        <Heading level={2}>Jobs</Heading>
        {total !== undefined && (
          <span className="text-sm text-muted-fg">
            {total.toLocaleString()} total
          </span>
        )}
      </div>

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

      {/* Search bar */}
      <SearchBar value={filters.search} onChange={setSearch} />

      {/* Active filters display */}
      {(searchFilters.kind || searchFilters.queue || searchFilters.tag) && (
        <div className="flex items-center gap-2 text-sm">
          <span className="text-muted-fg">Filters:</span>
          {searchFilters.kind && (
            <Badge intent="primary">kind:{searchFilters.kind}</Badge>
          )}
          {searchFilters.queue && (
            <Badge intent="primary">queue:{searchFilters.queue}</Badge>
          )}
          {searchFilters.tag && (
            <Badge intent="primary">tag:{searchFilters.tag}</Badge>
          )}
          <Button intent="outline" size="xs" onPress={() => setSearch("")}>
            Clear
          </Button>
        </div>
      )}

      {/* Bulk action toolbar */}
      {selected.size > 0 && (
        <div className="flex items-center gap-3 rounded-lg bg-muted p-3">
          <span className="text-sm">{selected.size} selected</span>
          <Badge intent="warning">Updates paused</Badge>
          <Button
            intent="primary"
            size="sm"
            onPress={() => retryMutation.mutate([...selected])}
            isDisabled={retryMutation.isPending}
          >
            Retry
          </Button>
          <Button
            intent="danger"
            size="sm"
            onPress={() => cancelMutation.mutate([...selected])}
            isDisabled={cancelMutation.isPending}
          >
            Cancel
          </Button>
        </div>
      )}

      {/* Job table — kind is the primary column, no ID */}
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
      >
        <TableHeader>
          <TableColumn isRowHeader>Kind</TableColumn>
          <TableColumn>State</TableColumn>
          <TableColumn>Queue</TableColumn>
          <TableColumn>Attempt</TableColumn>
          <TableColumn>Tags</TableColumn>
          <TableColumn>Created</TableColumn>
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
                {timeAgo(job.created_at)}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>

      {/* Pagination */}
      {jobs.length === filters.limit && lastJob && (
        <div className="flex gap-2 pt-2">
          <Button
            intent="outline"
            size="sm"
            onPress={() => setBeforeId(lastJob.id)}
          >
            Next page
          </Button>
          {filters.beforeId !== undefined && (
            <Button
              intent="outline"
              size="sm"
              onPress={() => setBeforeId(undefined)}
            >
              Back to first
            </Button>
          )}
        </div>
      )}
    </div>
  );
}
