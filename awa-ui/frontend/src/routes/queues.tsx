import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  fetchQueues,
  fetchQueueRuntime,
  pauseQueue,
  resumeQueue,
  drainQueue,
} from "@/lib/api";
import type { QueueStats } from "@/lib/api";
import { compareQueuePressure, queueNeedsReview } from "@/lib/operations";
import { useReadOnly } from "@/hooks/use-read-only";
import { usePollInterval } from "@/hooks/use-poll-interval";
import { toast } from "@/components/ui/toast";
import { Button } from "@/components/ui/button";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { QueuePressure } from "@/components/QueuePressure";
import { SnapshotStatus } from "@/components/SnapshotStatus";

export function QueuesPage() {
  const queryClient = useQueryClient();
  const [drainTarget, setDrainTarget] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [reviewOnly, setReviewOnly] = useState(false);
  const [sort, setSort] = useState("wait");
  const poll = usePollInterval();
  const readOnly = useReadOnly();
  const queuesQuery = useQuery({
    queryKey: ["queues"],
    queryFn: fetchQueues,
    refetchInterval: poll.interval,
    staleTime: poll.staleTime,
  });
  const runtimeQuery = useQuery({
    queryKey: ["queue-runtime"],
    queryFn: fetchQueueRuntime,
    refetchInterval: poll.interval,
    staleTime: poll.staleTime,
  });
  const refresh = () => {
    void queryClient.invalidateQueries({ queryKey: ["queues"] });
    void queryClient.invalidateQueries({ queryKey: ["queue"] });
  };
  const pauseMutation = useMutation({
    mutationFn: (queue: string) => pauseQueue(queue, "ui"),
    onSuccess: () => {
      refresh();
      toast.success("Queue paused");
    },
    onError: () => toast.error("Failed to pause queue"),
  });
  const resumeMutation = useMutation({
    mutationFn: resumeQueue,
    onSuccess: () => {
      refresh();
      toast.success("Queue resumed");
    },
    onError: () => toast.error("Failed to resume queue"),
  });
  const drainMutation = useMutation({
    mutationFn: drainQueue,
    onSuccess: () => {
      setDrainTarget(null);
      refresh();
      void queryClient.invalidateQueries({ queryKey: ["jobs"] });
      toast.success("Queue drained");
    },
    onError: () => toast.error("Failed to drain queue"),
  });
  const pending =
    pauseMutation.isPending ||
    resumeMutation.isPending ||
    drainMutation.isPending;
  const allQueues = queuesQuery.data ?? [];
  const queues = allQueues
    .filter(
      (queue) =>
        (!reviewOnly || queueNeedsReview(queue)) &&
        `${queue.queue} ${queue.display_name ?? ""} ${queue.owner ?? ""}`
          .toLowerCase()
          .includes(search.toLowerCase()),
    )
    .sort((left, right) =>
      sort === "name"
        ? left.queue.localeCompare(right.queue)
        : sort === "failed"
          ? right.failed - left.failed || compareQueuePressure(left, right)
          : sort === "available"
            ? right.available - left.available ||
              compareQueuePressure(left, right)
            : compareQueuePressure(left, right),
    );
  const runtimeByQueue = new Map(
    (runtimeQuery.data ?? []).map((summary) => [summary.queue, summary]),
  );
  function controls(queue: QueueStats) {
    return (
      <div className="queue-action-buttons">
        <Button
          intent="outline"
          size="sm"
          isDisabled={readOnly || pending}
          onPress={() =>
            queue.paused
              ? resumeMutation.mutate(queue.queue)
              : pauseMutation.mutate(queue.queue)
          }
        >
          {queue.paused ? "Resume" : "Pause"}
        </Button>
        <Button
          intent="plain"
          size="sm"
          className="text-danger"
          isDisabled={readOnly || pending}
          onPress={() => setDrainTarget(queue.queue)}
        >
          Drain…
        </Button>
      </div>
    );
  }
  return (
    <div className="operations-overview">
      <div className="page-intro">
        <div>
          <p className="eyebrow">Operations / queues</p>
          <h1>Queues</h1>
          <p>Find waiting work, inspect failures, and control processing.</p>
        </div>
      </div>
      <SnapshotStatus sources={[queuesQuery, runtimeQuery]} />
      {(queuesQuery.isError || runtimeQuery.isError) && (
        <div role="alert" className="source-error">
          {queuesQuery.isError
            ? "Queue snapshot unavailable or out of date. "
            : ""}
          {runtimeQuery.isError
            ? "Worker coverage unavailable or out of date. "
            : ""}
          Use Refresh data to retry.
        </div>
      )}
      <div className="queue-filters">
        <label>
          Find a queue
          <input
            type="search"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            placeholder="Name or owner"
          />
        </label>
        <label>
          Sort by
          <select
            value={sort}
            onChange={(event) => setSort(event.target.value)}
          >
            <option value="wait">Longest available wait</option>
            <option value="available">Most available jobs</option>
            <option value="failed">Most failed jobs</option>
            <option value="name">Queue name</option>
          </select>
        </label>
        <label className="review-toggle">
          <input
            type="checkbox"
            checked={reviewOnly}
            onChange={(event) => setReviewOnly(event.target.checked)}
          />
          Needs review
        </label>
      </div>
      <p className="text-sm text-muted-fg">
        {queues.length} of {allQueues.length} queues · Needs review includes
        paused, retryable, failed, and descriptor issues.
      </p>
      <section className="operations-panel">
        {queues.length ? (
          <QueuePressure
            queues={queues}
            runtime={runtimeByQueue}
            actions={controls}
          />
        ) : (
          <p className="panel-empty">
            {queuesQuery.isPending
              ? "Loading queues…"
              : queuesQuery.isError
                ? "Queue data unavailable."
                : allQueues.length
                  ? "No queues match these filters."
                  : "No queues yet. Queues appear when registered or used."}
          </p>
        )}
        <p className="panel-footnote">
          Available wait excludes scheduled, retryable, running, and external
          waits. Completions cover the last 60 minutes. Pause stops new claims;
          running jobs continue.
        </p>
      </section>
      <ConfirmDialog
        isOpen={drainTarget !== null}
        onOpenChange={(open) => {
          if (!open) setDrainTarget(null);
        }}
        title={`Drain queue "${drainTarget}"`}
        description="This will cancel all available, scheduled, retryable, and waiting_external jobs in this queue. Running jobs will not be affected."
        confirmLabel="Drain queue"
        confirmIntent="danger"
        onConfirm={() => {
          if (drainTarget) drainMutation.mutate(drainTarget);
        }}
        isPending={drainMutation.isPending}
      />
    </div>
  );
}
