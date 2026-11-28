import { useManagedQuery } from "@/hooks/use-managed-query";
import { Time } from "@/components/Time";
import { useCallback } from "react";
import { useNavigate, useSearch } from "@tanstack/react-router";
import { fetchDlq, fetchJobCount } from "@/lib/api";
import type { DlqRow, JobCount, JobFilterParams, ListDlqParams } from "@/lib/api";
import { SearchBar, parseSearch, termError, toRfc3339 } from "@/components/SearchBar";
import { Button } from "@/components/ui/button";
import { BulkScopeBar } from "@/components/BulkScopeBar";
import {
  Table,
  TableHeader,
  TableBody,
  TableRow,
  TableCell,
  TableColumn,
} from "@/components/ui/table";
import { DEFAULT_PAGE_SIZE } from "@/lib/constants";
import { DlqLink, KindLink, QueueLink } from "@/components/links";
import { listMatchesFilter, nextPageAnchor } from "@/lib/query";

/// Coerce a URL-param string to a positive integer, rejecting `NaN`,
/// negatives, zero, and floats. Returns `undefined` on invalid input so
/// callers can fall back to their default instead of forwarding junk
/// to the backend.
function parsePositiveInt(value: string | undefined): number | undefined {
  if (!value) return undefined;
  const n = Number(value);
  return Number.isInteger(n) && n > 0 ? n : undefined;
}

function useDlqFilters() {
  const searchParams = useSearch({ strict: false }) as Record<
    string,
    string | undefined
  >;
  return {
    search: searchParams.q ?? "",
    beforeId: parsePositiveInt(searchParams.before_id),
    beforeDlqAt: searchParams.before_dlq_at,
    limit: parsePositiveInt(searchParams.limit) ?? DEFAULT_PAGE_SIZE,
  };
}

export interface DlqListProps {
  /** Filter terms the page fixes; they apply to every read and bulk action
   * and can't be removed from the search. */
  lockedFilter?: { queue?: string; kind?: string };
}

/** The filterable dead-letter list with its search, bulk actions and
 * pagination. Its filter lives in the current URL's `q`, `before_id`,
 * `before_dlq_at` and `limit`. */
export function DlqList({ lockedFilter }: DlqListProps) {
  const navigate = useNavigate();
  const filters = useDlqFilters();

  const searchFilters = { ...parseSearch(filters.search), ...lockedFilter };
  // A term that doesn't parse is left out of a request, so it would widen
  // what is listed and what a bulk action touches.
  const searchError = termError(filters.search);

  const params: ListDlqParams = {
    kind: searchFilters.kind,
    queue: searchFilters.queue,
    tags: searchFilters.tags,
    ids: searchFilters.ids,
    created_at_gte: toRfc3339(searchFilters.createdGte),
    created_at_lt: toRfc3339(searchFilters.createdLt),
    before_id: filters.beforeId,
    before_dlq_at: filters.beforeDlqAt,
    limit: filters.limit,
  };

  const dlqQuery = useManagedQuery<DlqRow[]>({
    queryKey: ["dlq", params],
    queryFn: () => fetchDlq(params),
    enabled: !searchError,
  });


  const filterParams: JobFilterParams = {
    kind: searchFilters.kind,
    queue: searchFilters.queue,
    tags: searchFilters.tags,
    ids: searchFilters.ids,
    created_at_gte: toRfc3339(searchFilters.createdGte),
    created_at_lt: toRfc3339(searchFilters.createdLt),
  };
  const hasActiveFilter = Object.keys(searchFilters).length > 0;
  // Counted when the filter changes, not on every poll.
  const countQuery = useManagedQuery<JobCount>({
    queryKey: ["jobs-count", { ...filterParams, dead_lettered: true }],
    queryFn: () => fetchJobCount({ ...filterParams, dead_lettered: true }),
    enabled: hasActiveFilter && !searchError,
    staleTime: 30_000,
  });

  // As on the jobs page: cached rows for the wider search stay hidden.
  const rows = searchError ? [] : dlqQuery.data ?? [];
  // Bulk actions are judged from the list beside them, so they wait for this
  // filter's rows; the previous filter's placeholder rows don't count.
  const listUnavailable = !listMatchesFilter(dlqQuery);
  const nextAnchor = nextPageAnchor(rows, filters.limit, dlqQuery);

  const setUrlParams = useCallback(
    (updates: Record<string, string | undefined>) => {
      const current = new URLSearchParams(window.location.search);
      for (const [k, v] of Object.entries(updates)) {
        if (v === undefined || v === "") {
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

  const setSearch = (q: string) => {
    setUrlParams({ q: q || undefined, before_id: undefined, before_dlq_at: undefined });
  };

  return (
    <div className="space-y-4" data-testid="dlq-list">
      {dlqQuery.isPlaceholderData && <p role="status">Previous filter results are shown while the new read finishes.</p>}

      <SearchBar
        value={filters.search}
        onChange={setSearch}
        locked={[lockedFilter?.kind && `kind:${lockedFilter.kind}`, lockedFilter?.queue && `queue:${lockedFilter.queue}`].filter(
          (term): term is string => Boolean(term)
        )}
      />

      {hasActiveFilter && (
        <BulkScopeBar
          target="dlq"
          filter={filterParams}
          count={countQuery.data}
          unavailableReason={
            searchError
              ? "Fix the search first"
              : dlqQuery.isError
                ? "The list didn't load"
                : listUnavailable
                  ? "Waiting for the list to load"
                  : undefined
          }
        />
      )}

      {searchError && (
        <p className="text-sm text-danger" role="alert">
          {searchError}. Nothing is listed and bulk actions are off until the
          search is fixed.
        </p>
      )}

      <Table>
        <TableHeader>
          <TableColumn>ID</TableColumn>
          <TableColumn>Kind</TableColumn>
          <TableColumn>Queue</TableColumn>
          <TableColumn>Reason</TableColumn>
          <TableColumn>Attempts</TableColumn>
          <TableColumn>DLQ'd</TableColumn>
        </TableHeader>
        <TableBody>
          {dlqQuery.isError && (
            <TableRow>
              <TableCell colSpan={6}>
                <div className="text-danger py-6" role="alert">
                  Failed to load the DLQ: {dlqQuery.error.message}
                </div>
              </TableCell>
            </TableRow>
          )}
          {rows.length === 0 && !dlqQuery.isLoading && !dlqQuery.isError && !searchError && (
            <TableRow>
              <TableCell colSpan={6}>
                <div className="text-muted-fg py-6">
                  DLQ is empty{hasActiveFilter ? " (matching this filter)" : ""}. Terminal failures appear here only for queues with DLQ on.
                </div>
              </TableCell>
            </TableRow>
          )}
          {rows.map((row) => (
            <TableRow key={row.id}>
              <TableCell>
                <DlqLink id={row.id}>{row.id}</DlqLink>
              </TableCell>
              <TableCell>
                <KindLink kind={row.kind} descriptor={row.kind_descriptor} />
              </TableCell>
              <TableCell>
                <QueueLink queue={row.queue} descriptor={row.queue_descriptor} />
              </TableCell>
              <TableCell>{row.dlq_reason}</TableCell>
              <TableCell>
                {row.attempt} / {row.max_attempts}
              </TableCell>
              <TableCell><Time value={row.dlq_at} /></TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>

      {rows.length >= (filters.limit ?? DEFAULT_PAGE_SIZE) && (
        <div className="flex justify-end">
          <Button
            intent="outline"
            size="xs"
            isDisabled={!nextAnchor}
            onPress={() => {
              const last = nextAnchor;
              if (last) {
                // Emit the full (dlq_at, id) cursor so pagination matches
                // the backend sort order even when older failed rows are
                // bulk-moved into DLQ after newer ones.
                setUrlParams({
                  before_id: String(last.id),
                  before_dlq_at: last.dlq_at,
                });
              }
            }}
          >
            Next page
          </Button>
        </div>
      )}

    </div>
  );
}
