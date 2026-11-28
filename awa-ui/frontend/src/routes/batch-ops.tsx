import { useManagedQuery } from "@/hooks/use-managed-query";
import { Time } from "@/components/Time";
import { Link, useNavigate, useSearch } from "@tanstack/react-router";
import { useState } from "react";
import type { BatchOperation } from "@/lib/api";
import { ListState } from "@/components/ListState";
import { batchOpsQuery, BATCH_OPS_LIST_LIMIT, olderBatchOpsQuery } from "@/lib/shared-queries";
import { BatchStateBadge } from "@/components/StateBadge";
import { BatchOpLink } from "@/components/links";
import { changedNothing, isCancellable, operationLabel, progressText } from "@/lib/batch";
import { AdminButton } from "@/components/AdminAction";
import { CancelOperationDialog } from "@/components/CancelOperationDialog";
import { Page, PageHeader } from "@/components/Page";
import { Button, buttonStyles } from "@/components/ui/button";
import {
  Table,
  TableHeader,
  TableBody,
  TableRow,
  TableCell,
  TableColumn,
} from "@/components/ui/table";
import { formatNumber } from "@/lib/number";
import { RowCards } from "@/components/RowCards";

/** The `(submitted_at, id)` cursor in the URL, when both halves are there. */
function usePageCursor() {
  const search = useSearch({ strict: false }) as Record<string, unknown>;
  const at = typeof search.before_submitted_at === "string" ? search.before_submitted_at : undefined;
  const id = typeof search.before_id === "string" ? search.before_id : undefined;
  return at && id ? { before_submitted_at: at, before_id: id } : null;
}

export function BatchOpsPage() {
  const navigate = useNavigate();
  const cursor = usePageCursor();
  // The newest page shares the sidebar badge's read; older pages are read by cursor.
  const operationsQuery = useManagedQuery(cursor ? olderBatchOpsQuery(cursor) : batchOpsQuery);

  const [cancelling, setCancelling] = useState<BatchOperation | null>(null);

  const operations = operationsQuery.data ?? [];
  const last = operations.length >= BATCH_OPS_LIST_LIMIT ? operations[operations.length - 1] : undefined;
  const goToPage = (next: { before_submitted_at: string; before_id: string } | null) =>
    void navigate({ to: "/batch-ops", search: next ?? {} });

  return (
    <Page>
      <PageHeader
        title="Bulk operations"
        description="Durable changes to many jobs at once, applied a chunk at a time by the maintenance leader."
        actions={
          <Link to="/batch-ops/new" className={buttonStyles({ intent: "primary", size: "sm" })}>
            New operation
          </Link>
        }
      />

      {operations.length === 0 && (
        <ListState
          isLoading={operationsQuery.isLoading}
          error={operationsQuery.isError ? operationsQuery.error : null}
          onRetry={() => void operationsQuery.refetch()}
          noun="batch operations"
          empty={cursor ? "No older operations." : "No batch operations yet."}
        />
      )}

      <RowCards
        label="Bulk operations"
        items={operations.map((operation) => ({
          key: operation.id,
          title: operationLabel(operation.op_kind),
          titleSuffix: operation.id.slice(0, 8),
          link: { to: "/batch-ops/$id", params: { id: operation.id } },
          status: (
            <BatchStateBadge state={operation.state} tone={changedNothing(operation) ? "retryable" : undefined} />
          ),
          why: operation.last_error ? (
            <span className="text-danger-subtle-fg">{operation.last_error}</span>
          ) : changedNothing(operation) ? (
            "No jobs changed"
          ) : undefined,
          meta: (
            <>
              <span>{progressText(operation)}</span>
              <Time value={operation.submitted_at} />
            </>
          ),
          actions: isCancellable(operation.state) ? (
            <AdminButton size="xs" intent="outline" onPress={() => setCancelling(operation)}>
              Cancel…
            </AdminButton>
          ) : undefined,
        }))}
      />

      {operations.length > 0 && (
      <Table aria-label="Batch operations" className="hidden sm:block">
        <TableHeader>
          <TableColumn>ID</TableColumn>
          <TableColumn>Kind</TableColumn>
          <TableColumn>State</TableColumn>
          <TableColumn>Progress</TableColumn>
          <TableColumn>Processed</TableColumn>
          <TableColumn>Skipped</TableColumn>
          <TableColumn>Errors</TableColumn>
          <TableColumn>Submitted</TableColumn>
          <TableColumn>Action</TableColumn>
        </TableHeader>
        <TableBody>
          {operations.map((operation) => (
            <TableRow
              key={operation.id}
              id={operation.id}
              className="cursor-pointer"
              onAction={() => void navigate({ to: "/batch-ops/$id", params: { id: operation.id } })}
            >
              <TableCell className="text-xs">
                <BatchOpLink id={operation.id} />
              </TableCell>
              <TableCell>{operationLabel(operation.op_kind)}</TableCell>
              <TableCell>
                <BatchStateBadge state={operation.state} tone={changedNothing(operation) ? "retryable" : undefined} />
                {changedNothing(operation) && (
                  <div className="mt-1 text-xs text-muted-fg" data-testid="batch-op-changed-nothing">
                    No jobs changed
                  </div>
                )}
                {operation.last_error && (
                  <div className="mt-1 max-w-xs whitespace-normal break-words text-xs text-danger-subtle-fg" data-testid="batch-op-last-error">
                    {operation.last_error}
                  </div>
                )}
              </TableCell>
              <TableCell className="tabular-nums">{progressText(operation)}</TableCell>
              <TableCell className="tabular-nums">{formatNumber(operation.processed)}</TableCell>
              <TableCell className="tabular-nums">{formatNumber(operation.skipped)}</TableCell>
              <TableCell className="tabular-nums">{formatNumber(operation.errored)}</TableCell>
              <TableCell><Time value={operation.submitted_at} /></TableCell>
              <TableCell>
                {isCancellable(operation.state) ? (
                  <AdminButton size="sm" intent="outline" onPress={() => setCancelling(operation)} aria-label={`Cancel operation ${operation.id.slice(0, 8)}`}>
                    Cancel…
                  </AdminButton>
                ) : null}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
      )}
      {(last || cursor) && (
        <div className="flex flex-wrap items-center justify-between gap-2" data-testid="batch-ops-paging">
          <p className="text-sm text-muted-fg">
            {cursor ? (
              <>Operations submitted before <Time value={cursor.before_submitted_at} />.</>
            ) : (
              `Showing the ${BATCH_OPS_LIST_LIMIT} most recent operations.`
            )}
          </p>
          <div className="flex gap-2">
            {cursor && (
              <Button intent="outline" size="xs" onPress={() => goToPage(null)}>
                Newest operations
              </Button>
            )}
            {last && (
              <Button
                intent="outline"
                size="xs"
                onPress={() => goToPage({ before_submitted_at: last.submitted_at, before_id: last.id })}
              >
                Older operations
              </Button>
            )}
          </div>
        </div>
      )}
      <CancelOperationDialog
        operation={cancelling}
        isOpen={cancelling !== null}
        onOpenChange={(open) => !open && setCancelling(null)}
      />
    </Page>
  );
}
