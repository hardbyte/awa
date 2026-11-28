import { useState } from "react";
import { useSearch } from "@tanstack/react-router";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useManagedQuery } from "@/hooks/use-managed-query";
import { useReadOnly } from "@/hooks/use-read-only";
import {
  drainQueue,
  fetchQueues,
  fetchStats,
  pauseQueue,
  resumeQueue,
  type QueueStats,
  type StateCounts,
} from "@/lib/api";
import { parseSearch } from "@/components/SearchBar";
import { JobsList } from "@/components/JobsList";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { Page, PageHeader } from "@/components/Page";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { toast } from "@/components/ui/toast";

export function JobsPage() {
  const queryClient = useQueryClient();
  const search = useSearch({ strict: false }) as Record<string, string | undefined>;
  const searchFilters = parseSearch(search.q ?? "");
  const readOnly = useReadOnly();

  const statsQuery = useManagedQuery<StateCounts>({
    queryKey: ["stats"],
    queryFn: fetchStats,
  });
  const total = statsQuery.data
    ? Object.values(statsQuery.data).reduce((a, b) => a + b, 0)
    : undefined;

  const [showDrainConfirm, setShowDrainConfirm] = useState(false);

  // Queue context: when filtering by a single queue, show queue actions
  const activeQueue = searchFilters.queue;
  const queuesQuery = useManagedQuery<QueueStats[]>({
    queryKey: ["queues"],
    queryFn: fetchQueues,
    enabled: !!activeQueue,
  });
  const queueStats = activeQueue
    ? queuesQuery.data?.find((q) => q.queue === activeQueue)
    : undefined;
  const activeQueueLabel = queueStats?.display_name?.trim()
    ? queueStats.display_name
    : activeQueue;

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
    onSuccess: (result) => {
      void queryClient.invalidateQueries({ queryKey: ["queues"] });
      void queryClient.invalidateQueries({ queryKey: ["jobs"] });
      toast.success(`Queue "${activeQueue}" drained: ${result.drained.toLocaleString()} job(s) cancelled`);
    },
    onError: () => toast.error("Failed to drain queue"),
  });

  return (
    <Page className="space-y-4">
      <PageHeader
        title={activeQueue ? `Queue: ${activeQueueLabel}` : "Jobs"}
        status={
          activeQueue && queueStats?.display_name ? (
            <span className="text-sm text-muted-fg">{queueStats.queue}</span>
          ) : total !== undefined && !activeQueue ? (
            <span className="text-sm text-muted-fg">{total.toLocaleString()} total</span>
          ) : null
        }
      />

      {/* Queue context banner — shown when filtering by a single queue */}
      {activeQueue && queueStats && (
        <div className="flex flex-wrap items-center gap-3 rounded-lg border bg-muted/30 p-3">
          {queueStats.paused ? (
            <Badge intent="warning">Paused</Badge>
          ) : (
            <Badge intent="success">Active</Badge>
          )}
          {queueStats.description && (
            <span className="text-sm text-muted-fg">{queueStats.description}</span>
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
                isDisabled={readOnly}
              >
                Resume
              </Button>
            ) : (
              <Button
                intent="outline"
                size="xs"
                onPress={() => pauseQueueMutation.mutate()}
                isDisabled={readOnly}
              >
                Pause
              </Button>
            )}
            <Button
              intent="outline"
              size="xs"
              className="text-danger"
              onPress={() => setShowDrainConfirm(true)}
              isDisabled={readOnly}
            >
              Drain
            </Button>
          </div>
        </div>
      )}

      <JobsList />

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
    </Page>
  );
}
