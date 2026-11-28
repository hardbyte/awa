import type { UseMutationResult } from "@tanstack/react-query";
import { ConfirmAction } from "@/components/ConfirmAction";

/** Confirms draining a queue: cancelling its available, scheduled, retryable and waiting jobs. */
export function DrainDialog({
  queue,
  isOpen,
  onOpenChange,
  drain,
}: {
  queue: string;
  isOpen: boolean;
  onOpenChange: (open: boolean) => void;
  drain: UseMutationResult<{ drained: number }, Error, string>;
}) {
  return (
    <ConfirmAction
      isOpen={isOpen}
      onOpenChange={onOpenChange}
      tier={2}
      intent="danger"
      title={`Drain queue "${queue}"?`}
      description={
        <>
          Cancels every available, scheduled, retryable and waiting-for-callback job in <code>{queue}</code>. Running
          jobs are not affected. Cancelled jobs can be retried later.
        </>
      }
      confirmLabel={`Drain "${queue}"`}
      onConfirm={() => drain.mutateAsync(queue)}
    />
  );
}
