import {
  useQuery,
  useMutation,
  useQueryClient,
} from "@tanstack/react-query";
import { useState, useCallback } from "react";
import { useNavigate, useSearch, Link } from "@tanstack/react-router";
import {
  fetchDlq,
  fetchDlqDepth,
  bulkRetryDlq,
  bulkPurgeDlq,
} from "@/lib/api";
import type { DlqRow, ListDlqParams } from "@/lib/api";
import { useReadOnly } from "@/hooks/use-read-only";
import { toast } from "@/components/ui/toast";
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
import { timeAgo } from "@/lib/time";
import { DEFAULT_PAGE_SIZE } from "@/lib/constants";
import { usePollInterval } from "@/hooks/use-poll-interval";

function useDlqFilters() {
  const searchParams = useSearch({ strict: false }) as Record<
    string,
    string | undefined
  >;
  return {
    search: searchParams.q ?? "",
    beforeId: searchParams.before_id
      ? Number(searchParams.before_id)
      : undefined,
    limit: searchParams.limit ? Number(searchParams.limit) : DEFAULT_PAGE_SIZE,
  };
}

export function DlqPage() {
  const navigate = useNavigate();
  const filters = useDlqFilters();
  const queryClient = useQueryClient();
  const readOnly = useReadOnly();
  const poll = usePollInterval();

  const searchFilters = parseSearch(filters.search);

  const params: ListDlqParams = {
    kind: searchFilters.kind,
    queue: searchFilters.queue,
    tag: searchFilters.tag,
    before_id: filters.beforeId,
    limit: filters.limit,
  };

  const dlqQuery = useQuery<DlqRow[]>({
    queryKey: ["dlq", params],
    queryFn: () => fetchDlq(params),
    refetchInterval: poll.interval,
    staleTime: poll.staleTime,
  });

  const depthQuery = useQuery({
    queryKey: ["dlq-depth"],
    queryFn: fetchDlqDepth,
    refetchInterval: poll.interval,
    staleTime: poll.staleTime,
  });

  const retryFilterMutation = useMutation({
    mutationFn: () =>
      bulkRetryDlq({
        kind: searchFilters.kind,
        queue: searchFilters.queue,
        tag: searchFilters.tag,
      }),
    onSuccess: (res) => {
      void queryClient.invalidateQueries({ queryKey: ["dlq"] });
      void queryClient.invalidateQueries({ queryKey: ["dlq-depth"] });
      void queryClient.invalidateQueries({ queryKey: ["jobs"] });
      toast.success(`Retried ${res.count} DLQ row(s)`);
    },
    onError: () => toast.error("Failed to retry DLQ rows"),
  });

  const purgeFilterMutation = useMutation({
    mutationFn: () =>
      bulkPurgeDlq({
        kind: searchFilters.kind,
        queue: searchFilters.queue,
        tag: searchFilters.tag,
      }),
    onSuccess: (res) => {
      void queryClient.invalidateQueries({ queryKey: ["dlq"] });
      void queryClient.invalidateQueries({ queryKey: ["dlq-depth"] });
      toast.success(`Purged ${res.count} DLQ row(s)`);
    },
    onError: () => toast.error("Failed to purge DLQ rows"),
  });

  const [showPurgeConfirm, setShowPurgeConfirm] = useState(false);

  const rows = dlqQuery.data ?? [];
  const hasActiveFilter =
    !!searchFilters.kind || !!searchFilters.queue || !!searchFilters.tag;

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
        to: "/dlq",
        search: Object.fromEntries(current.entries()),
        replace: true,
      });
    },
    [navigate]
  );

  const setSearch = (q: string) => {
    setUrlParams({ q: q || undefined, before_id: undefined });
  };

  return (
    <div className="space-y-4 p-4">
      <div className="flex items-center justify-between">
        <Heading>Dead Letter Queue</Heading>
        {depthQuery.data && (
          <div className="flex gap-2 items-center">
            <Badge intent="danger">
              {depthQuery.data.total.toLocaleString()} total
            </Badge>
            {depthQuery.data.by_queue.slice(0, 5).map((q) => (
              <Badge key={q.queue} intent="secondary">
                {q.queue}: {q.count.toLocaleString()}
              </Badge>
            ))}
          </div>
        )}
      </div>

      <SearchBar value={filters.search} onChange={setSearch} />

      {hasActiveFilter && !readOnly && (
        <div className="flex gap-2">
          <Button
            size="xs"
            onPress={() => retryFilterMutation.mutate()}
            isDisabled={retryFilterMutation.isPending || rows.length === 0}
          >
            Retry matching ({rows.length}+)
          </Button>
          <Button
            intent="outline"
            size="xs"
            className="text-danger"
            onPress={() => setShowPurgeConfirm(true)}
            isDisabled={purgeFilterMutation.isPending || rows.length === 0}
          >
            Purge matching
          </Button>
        </div>
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
          {rows.length === 0 && !dlqQuery.isLoading && (
            <TableRow>
              <TableCell>
                <div className="text-muted-fg py-6">
                  DLQ is empty{hasActiveFilter ? " (matching this filter)" : ""}.
                </div>
              </TableCell>
            </TableRow>
          )}
          {rows.map((row) => (
            <TableRow key={row.id}>
              <TableCell>
                <Link
                  to="/dlq/$id"
                  params={{ id: String(row.id) }}
                  className="font-mono underline"
                >
                  {row.id}
                </Link>
              </TableCell>
              <TableCell className="font-mono">{row.kind}</TableCell>
              <TableCell>
                <Link
                  to="/queues/$name"
                  params={{ name: row.queue }}
                  className="underline"
                >
                  {row.queue}
                </Link>
              </TableCell>
              <TableCell>{row.dlq_reason}</TableCell>
              <TableCell>
                {row.attempt} / {row.max_attempts}
              </TableCell>
              <TableCell>{timeAgo(row.dlq_at)}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>

      {rows.length >= (filters.limit ?? DEFAULT_PAGE_SIZE) && (
        <div className="flex justify-end">
          <Button
            intent="outline"
            size="xs"
            onPress={() => {
              const last = rows[rows.length - 1];
              if (last) {
                setUrlParams({ before_id: String(last.id) });
              }
            }}
          >
            Next page
          </Button>
        </div>
      )}

      <ConfirmDialog
        isOpen={showPurgeConfirm}
        onOpenChange={setShowPurgeConfirm}
        title="Purge DLQ rows"
        description="This permanently deletes every DLQ row matching the current filter."
        confirmLabel="Purge"
        confirmIntent="danger"
        onConfirm={() => purgeFilterMutation.mutate()}
      />
    </div>
  );
}
