import { useMutation, useQueryClient } from "@tanstack/react-query";
import { cancelBatchOperation, type BatchOperation } from "@/lib/api";
import { operationLabel } from "@/lib/batch";
import { formatNumber } from "@/lib/number";
import { ConfirmAction } from "@/components/ConfirmAction";
import { toast } from "@/components/ui/toast";

/** Confirms cancelling a bulk operation, saying what it has already changed. */
export function CancelOperationDialog({
  operation,
  isOpen,
  onOpenChange,
}: {
  operation: BatchOperation | null;
  isOpen: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const queryClient = useQueryClient();
  const cancel = useMutation({
    mutationFn: (id: string) => cancelBatchOperation(id),
    onSuccess: (_data, id) => {
      void queryClient.invalidateQueries({ queryKey: ["batch-op", id] });
      void queryClient.invalidateQueries({ queryKey: ["batch-ops"] });
      toast.success(`Cancellation of operation ${id.slice(0, 8)} requested`);
    },
  });
  if (!operation) return null;
  const short = operation.id.slice(0, 8);
  const done = operation.processed;
  const total = operation.total_matched;
  return (
    <ConfirmAction
      isOpen={isOpen}
      onOpenChange={onOpenChange}
      tier={1}
      intent="danger"
      title={`Cancel operation ${short}?`}
      description={
        `The ${operationLabel(operation.op_kind)} operation stops before its next chunk. ` +
        (done > 0
          ? `The ${formatNumber(done)}${total !== null ? ` of ${formatNumber(total)}` : ""} job(s) it has already changed stay changed.`
          : "It hasn't changed any jobs yet.")
      }
      confirmLabel={`Cancel operation ${short}`}
      onConfirm={() => cancel.mutateAsync(operation.id)}
    />
  );
}
