import { useState } from "react";
import {
  useQuery,
  useMutation,
  useQueryClient,
} from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import {
  fetchDlqDepth,
  fetchQueues,
  fetchQueueRuntime,
  pauseQueue,
  resumeQueue,
  drainQueue,
} from "@/lib/api";
import { useReadOnly } from "@/hooks/use-read-only";
import { toast } from "@/components/ui/toast";
import type { QueueRuntimeSummary, QueueStats } from "@/lib/api";
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
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { LagValue } from "@/components/LagValue";
import { usePollInterval } from "@/hooks/use-poll-interval";
import { timeAgo } from "@/lib/time";

export function QueuesPage() {
  const queryClient = useQueryClient();
  const [drainTarget, setDrainTarget] = useState<string | null>(null);
  const poll = usePollInterval();

  const queuesQuery = useQuery<QueueStats[]>({
    queryKey: ["queues"],
    queryFn: fetchQueues,
    refetchInterval: poll.interval, staleTime: poll.staleTime,
  });

  const dlqDepthQuery = useQuery({
    queryKey: ["dlq-depth"],
    queryFn: fetchDlqDepth,
    refetchInterval: poll.interval,
    staleTime: poll.staleTime,
  });

  const runtimeQuery = useQuery<QueueRuntimeSummary[]>({
    queryKey: ["queue-runtime"],
    queryFn: fetchQueueRuntime,
    refetchInterval: poll.interval, staleTime: poll.staleTime,
  });
  const readOnly = useReadOnly();

  const pauseMutation = useMutation({
    mutationFn: (queue: string) => pauseQueue(queue, "ui"),
    onSuccess: (_data, queue) => {
      void queryClient.invalidateQueries({ queryKey: ["queues"] });
      toast.success(`Queue "${queue}" paused`);
    },
    onError: () => {
      toast.error("Failed to pause queue");
    },
  });

  const resumeMutation = useMutation({
    mutationFn: (queue: string) => resumeQueue(queue),
    onSuccess: (_data, queue) => {
      void queryClient.invalidateQueries({ queryKey: ["queues"] });
      toast.success(`Queue "${queue}" resumed`);
    },
    onError: () => {
      toast.error("Failed to resume queue");
    },
  });

  const drainMutation = useMutation({
    mutationFn: (queue: string) => drainQueue(queue),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["queues"] });
      void queryClient.invalidateQueries({ queryKey: ["jobs"] });
      toast.success(`Queue drained`);
    },
    onError: () => {
      toast.error("Failed to drain queue");
    },
  });

  const queues = queuesQuery.data ?? [];
  const runtimeByQueue = new Map<string, QueueRuntimeSummary>(
    (runtimeQuery.data ?? []).map(
      (summary): [string, QueueRuntimeSummary] => [summary.queue, summary],
    ),
  );

  function capacityLabel(runtime: QueueRuntimeSummary | undefined): string {
    if (!runtime?.config) return "—";
    if (runtime.config.mode === "weighted") {
      return `min ${runtime.config.min_workers ?? 0} / w ${runtime.config.weight ?? 1}`;
    }
    return `max ${runtime.config.max_workers ?? 0}`;
  }

  function rateLimitLabel(runtime: QueueRuntimeSummary | undefined): string {
    if (!runtime?.config?.rate_limit) return "—";
    return `${runtime.config.rate_limit.max_rate}/s (${runtime.config.rate_limit.burst})`;
  }

  function runtimeHealthLabel(runtime: QueueRuntimeSummary | undefined): string {
    if (!runtime) return "—";
    return `${runtime.healthy_instances}/${runtime.live_instances || runtime.instance_count}`;
  }

  function queueLabel(q: QueueStats): string {
    // Treat empty display_name as missing — `??` alone would let
    // `display_name: ""` render a blank label.
    return q.display_name?.trim() ? q.display_name : q.queue;
  }

  function descriptorSyncLabel(q: QueueStats): string {
    if (q.descriptor_mismatch) return "Descriptor drift across live runtimes";
    if (!q.descriptor_last_seen_at) return "Descriptor not declared";
    return q.descriptor_stale
      ? `Descriptor stale · seen ${timeAgo(q.descriptor_last_seen_at)}`
      : `Descriptor seen ${timeAgo(q.descriptor_last_seen_at)}`;
  }

  const dlqByQueue = new Map<string, number>(
    (dlqDepthQuery.data?.by_queue ?? []).map(({ queue, count }) => [queue, count]),
  );

  return (
    <div className="space-y-4">
      <Heading level={2}>Queues</Heading>

      {dlqDepthQuery.data && dlqDepthQuery.data.total > 0 && (
        <Link
          to="/dlq"
          className="flex items-center justify-between rounded-lg border border-danger/30 bg-danger/5 p-3 no-underline hover:bg-danger/10"
        >
          <div className="flex items-center gap-2">
            <Badge intent="danger">DLQ</Badge>
            <span className="text-sm">
              {dlqDepthQuery.data.total.toLocaleString()} permanently failed job(s)
              {dlqDepthQuery.data.by_queue.length > 0 &&
                ` across ${dlqDepthQuery.data.by_queue.length} queue(s)`}
            </span>
          </div>
          <span className="text-sm text-muted-fg">Inspect →</span>
        </Link>
      )}

      {/* Mobile card layout */}
      {queues.length > 0 && (
        <div className="space-y-3 sm:hidden">
          {queues.map((q) => {
            const runtime = runtimeByQueue.get(q.queue);
            return (
              <div key={q.queue} className="rounded-lg border p-4">
              <div className="flex items-center justify-between">
                <div>
                  <Link
                    to="/queues/$name"
                    params={{ name: q.queue }}
                    className="font-medium text-primary no-underline hover:underline"
                  >
                    {queueLabel(q)}
                  </Link>
                  {q.display_name?.trim() && (
                    <div className="text-xs text-muted-fg">{q.queue}</div>
                  )}
                  <div className="mt-1 text-xs text-muted-fg">
                    {descriptorSyncLabel(q)}
                  </div>
                </div>
                <div className="flex flex-wrap gap-1">
                  {q.paused ? (
                    <Badge intent="warning">Paused</Badge>
                  ) : (
                    <Badge intent="success">Active</Badge>
                  )}
                  {q.descriptor_stale && (
                    <Badge intent="warning">Descriptor stale</Badge>
                  )}
                  {q.descriptor_mismatch && (
                    <Badge intent="danger">Descriptor drift</Badge>
                  )}
                </div>
              </div>
              {q.description && (
                <p className="mt-2 text-sm text-muted-fg">{q.description}</p>
              )}
              <div className="mt-2 grid grid-cols-2 gap-x-4 gap-y-1 text-sm">
                <span className="text-muted-fg">Total queued</span>
                <span>{q.total_queued.toLocaleString()}</span>
                <span className="text-muted-fg">Scheduled</span>
                <span>{q.scheduled.toLocaleString()}</span>
                <span className="text-muted-fg">Available</span>
                <span>{q.available.toLocaleString()}</span>
                <span className="text-muted-fg">Retryable</span>
                <span>{q.retryable.toLocaleString()}</span>
                <span className="text-muted-fg">Running</span>
                <span>{q.running.toLocaleString()}</span>
                <span className="text-muted-fg">Failed</span>
                <span className={q.failed > 0 ? "text-danger" : ""}>{q.failed.toLocaleString()}</span>
                {q.waiting_external > 0 && (
                  <>
                    <span className="text-muted-fg">Waiting</span>
                    <span>{q.waiting_external.toLocaleString()}</span>
                  </>
                )}
                <span className="text-muted-fg">Lag</span>
                <span><LagValue seconds={q.lag_seconds} /></span>
                <span className="text-muted-fg">Capacity</span>
                <span>{capacityLabel(runtime)}</span>
                <span className="text-muted-fg">Rate limit</span>
                <span>{rateLimitLabel(runtime)}</span>
                <span className="text-muted-fg">Healthy nodes</span>
                <span>{runtimeHealthLabel(runtime)}</span>
                {runtime?.config_mismatch && (
                  <>
                    <span className="text-muted-fg">Config</span>
                    <span className="text-warning-fg">Mismatch</span>
                  </>
                )}
              </div>
              <div className="mt-3 flex gap-2">
                {q.paused ? (
                  <Button
                    intent="outline"
                    size="xs"
                    onPress={() => resumeMutation.mutate(q.queue)}
                    isDisabled={readOnly}
                  >
                    Resume
                  </Button>
                ) : (
                  <Button
                    intent="outline"
                    size="xs"
                    onPress={() => pauseMutation.mutate(q.queue)}
                    isDisabled={readOnly}
                  >
                    Pause
                  </Button>
                )}
                <Button
                  intent="outline"
                  size="xs"
                  className="text-danger"
                  onPress={() => setDrainTarget(q.queue)}
                  isDisabled={readOnly}
                >
                  Drain
                </Button>
              </div>
              </div>
            );
          })}
        </div>
      )}

      {/* Desktop table layout */}
      {queues.length > 0 ? (
        <Table aria-label="Queues" className="hidden sm:table">
          <TableHeader>
            <TableColumn isRowHeader>Queue</TableColumn>
            <TableColumn>Total queued</TableColumn>
            <TableColumn>Scheduled</TableColumn>
            <TableColumn>Available</TableColumn>
            <TableColumn>Retryable</TableColumn>
            <TableColumn>Running</TableColumn>
            <TableColumn>Failed</TableColumn>
            <TableColumn>Waiting</TableColumn>
            <TableColumn>Completed/hr</TableColumn>
            <TableColumn>Lag (s)</TableColumn>
            <TableColumn>Mode</TableColumn>
            <TableColumn>Capacity</TableColumn>
            <TableColumn>Rate limit</TableColumn>
            <TableColumn>Runtime</TableColumn>
            <TableColumn>Status</TableColumn>
            <TableColumn>Actions</TableColumn>
          </TableHeader>
          <TableBody>
            {queues.map((q) => {
              const runtime = runtimeByQueue.get(q.queue);
              return (
                <TableRow key={q.queue} id={q.queue}>
                  <TableCell className="font-medium">
                    <div>
                      <Link
                        to="/queues/$name"
                        params={{ name: q.queue }}
                        className="text-primary no-underline hover:underline"
                      >
                        {queueLabel(q)}
                      </Link>
                      {q.display_name && (
                        <div className="text-xs font-normal text-muted-fg">{q.queue}</div>
                      )}
                      {q.description && (
                        <div className="mt-1 text-xs font-normal text-muted-fg">
                          {q.description}
                        </div>
                      )}
                      <div className="mt-1 text-xs font-normal text-muted-fg">
                        {descriptorSyncLabel(q)}
                      </div>
                    </div>
                  </TableCell>
                  <TableCell>{q.total_queued.toLocaleString()}</TableCell>
                  <TableCell>{q.scheduled.toLocaleString()}</TableCell>
                  <TableCell>{q.available.toLocaleString()}</TableCell>
                  <TableCell>{q.retryable.toLocaleString()}</TableCell>
                  <TableCell>{q.running.toLocaleString()}</TableCell>
                  <TableCell>
                    <span className={q.failed > 0 ? "text-danger" : ""}>
                      {q.failed.toLocaleString()}
                    </span>
                    {dlqByQueue.get(q.queue) ? (
                      <Link
                        to="/dlq"
                        search={{ q: `queue:${q.queue}` }}
                        className="ml-2 text-xs text-danger underline"
                        title="DLQ depth"
                      >
                        (+{dlqByQueue.get(q.queue)!.toLocaleString()} DLQ)
                      </Link>
                    ) : null}
                  </TableCell>
                  <TableCell>
                    {q.waiting_external > 0 ? q.waiting_external.toLocaleString() : "-"}
                  </TableCell>
                  <TableCell>{q.completed_last_hour}</TableCell>
                  <TableCell>
                    <LagValue seconds={q.lag_seconds} />
                  </TableCell>
                  <TableCell>
                    {runtime?.config ? (
                      <Badge
                        intent={
                          runtime.config.mode === "weighted" ? "secondary" : "outline"
                        }
                      >
                        {runtime.config.mode === "weighted" ? "Weighted" : "Reserved"}
                      </Badge>
                    ) : (
                      "—"
                    )}
                  </TableCell>
                  <TableCell>{capacityLabel(runtime)}</TableCell>
                  <TableCell>{rateLimitLabel(runtime)}</TableCell>
                  <TableCell>
                    <div className="text-sm">
                      {runtime ? (
                        <>
                          <div>{runtimeHealthLabel(runtime)} healthy</div>
                          {runtime.config_mismatch ? (
                            <div className="text-warning-fg">Config mismatch</div>
                          ) : (
                            <div className="text-muted-fg">
                              {runtime.stale_instances > 0
                                ? `${runtime.stale_instances} stale`
                                : `${runtime.instance_count} nodes`}
                            </div>
                          )}
                        </>
                      ) : (
                        <span className="text-muted-fg">—</span>
                      )}
                    </div>
                  </TableCell>
                  <TableCell>
                    <div className="flex flex-wrap gap-1">
                      {q.paused ? (
                        <Badge intent="warning">Paused</Badge>
                      ) : (
                        <Badge intent="success">Active</Badge>
                      )}
                      {q.descriptor_stale && (
                        <Badge intent="warning">Descriptor stale</Badge>
                      )}
                      {q.descriptor_mismatch && (
                        <Badge intent="danger">Descriptor drift</Badge>
                      )}
                    </div>
                  </TableCell>
                  <TableCell>
                    <div className="flex gap-1">
                      {q.paused ? (
                        <Button
                          intent="outline"
                          size="xs"
                          onPress={() => resumeMutation.mutate(q.queue)}
                          isDisabled={readOnly}
                        >
                          Resume
                        </Button>
                      ) : (
                        <Button
                          intent="outline"
                          size="xs"
                          onPress={() => pauseMutation.mutate(q.queue)}
                          isDisabled={readOnly}
                        >
                          Pause
                        </Button>
                      )}
                      <Button
                        intent="outline"
                        size="xs"
                        className="text-danger"
                        onPress={() => setDrainTarget(q.queue)}
                        isDisabled={readOnly}
                      >
                        Drain
                      </Button>
                    </div>
                  </TableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
      ) : queuesQuery.isLoading ? (
        <p className="text-sm text-muted-fg">Loading...</p>
      ) : (
        <p className="text-sm text-muted-fg">No queues found.</p>
      )}

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
