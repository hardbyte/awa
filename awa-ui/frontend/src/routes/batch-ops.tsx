import { useManagedQuery } from "@/hooks/use-managed-query";
import { Time } from "@/components/Time";
import { Link, useNavigate } from "@tanstack/react-router";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { cancelBatchOperation } from "@/lib/api";
import { ListState } from "@/components/ListState";
import { batchOpsQuery, BATCH_OPS_LIST_LIMIT } from "@/lib/shared-queries";
import { BatchStateBadge } from "@/components/StateBadge";
import { BatchOpLink } from "@/components/links";
import { changedNothing, isCancellable, operationLabel, progressText } from "@/lib/batch";
import { useReadOnly } from "@/hooks/use-read-only";
import { toast } from "@/components/ui/toast";
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

export function BatchOpsPage() {
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const readOnly = useReadOnly();
  const operationsQuery = useManagedQuery(batchOpsQuery);

  const cancelMutation = useMutation({
    mutationFn: (id: string) => cancelBatchOperation(id),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["batch-ops"] });
      toast.success("Cancellation requested");
    },
    onError: (error) => toast.error(error.message),
  });

  const operations = operationsQuery.data ?? [];

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

      <Table aria-label="Batch operations">
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
        <TableBody
          renderEmptyState={() => (
            <ListState
              isLoading={operationsQuery.isLoading}
              error={operationsQuery.isError ? operationsQuery.error : null}
              onRetry={() => void operationsQuery.refetch()}
              noun="batch operations"
              empty="No batch operations yet."
            />
          )}
        >
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
                  <Button size="sm" intent="outline" onPress={() => cancelMutation.mutate(operation.id)} isDisabled={readOnly || cancelMutation.isPending}>Cancel</Button>
                ) : null}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
      {operations.length >= BATCH_OPS_LIST_LIMIT && (
        <p className="text-sm text-muted-fg">
          Showing the {BATCH_OPS_LIST_LIMIT} most recent operations.
        </p>
      )}
    </Page>
  );
}
