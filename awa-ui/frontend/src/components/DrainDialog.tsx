import { useQuery, type UseMutationResult } from "@tanstack/react-query";
import { fetchJobCount } from "@/lib/api";
import { formatCount } from "@/lib/batch";
import { ConfirmAction } from "@/components/ConfirmAction";

/** The states drain cancels, as the server's drain does. */
const DRAINED_STATES = ["available", "scheduled", "retryable", "waiting_external"];

/**
 * Confirms draining a queue: cancelling its available, scheduled, retryable
 * and waiting jobs. Shows how many that is and asks for the queue's name.
 */
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
  const count = useQuery({
    queryKey: ["drain-count", queue],
    queryFn: () => fetchJobCount({ queue, states: DRAINED_STATES }),
    enabled: isOpen && queue !== "",
    staleTime: 0,
    gcTime: 0,
    retry: false,
    refetchOnWindowFocus: false,
  });
  const jobs = count.data
    ? `${formatCount(count.data)} job${count.data.kind === "exact" && count.data.value === 1 ? "" : "s"}`
    : count.isError
      ? "every job"
      : "…";
  return (
    <ConfirmAction
      isOpen={isOpen}
      onOpenChange={onOpenChange}
      tier={3}
      intent="danger"
      title={`Drain queue "${queue}"?`}
      description={
        <>
          Cancels <strong data-testid="drain-count">{jobs}</strong> in <code>{queue}</code>: every available, scheduled,
          retryable and waiting-for-callback job. Running jobs are not affected. It acts on the whole queue, whatever
          filter a list shows. Cancelled jobs can be retried later.
        </>
      }
      confirmText={queue}
      confirmTextLabel="the queue name"
      confirmLabel={`Drain "${queue}"`}
      onConfirm={() => drain.mutateAsync(queue)}
    >
      {count.isError && (
        <p role="alert" className="text-danger-subtle-fg">
          Couldn't count them: {count.error.message}
        </p>
      )}
    </ConfirmAction>
  );
}
