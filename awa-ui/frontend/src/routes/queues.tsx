import { useState } from "react";
import {
  useQuery,
  useMutation,
  useQueryClient,
} from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import {
  fetchQueues,
  pauseQueue,
  resumeQueue,
  drainQueue,
} from "@/lib/api";
import { toast } from "@/components/ui/toast";
import type { QueueStats } from "@/lib/api";
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

export function QueuesPage() {
  const queryClient = useQueryClient();
  const [drainTarget, setDrainTarget] = useState<string | null>(null);

  const queuesQuery = useQuery<QueueStats[]>({
    queryKey: ["queues"],
    queryFn: fetchQueues,
  });

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

  return (
    <div className="space-y-4">
      <Heading level={2}>Queues</Heading>

      {queues.length > 0 ? (
        <Table aria-label="Queues">
          <TableHeader>
            <TableColumn isRowHeader>Queue</TableColumn>
            <TableColumn>Available</TableColumn>
            <TableColumn>Running</TableColumn>
            <TableColumn>Failed</TableColumn>
            <TableColumn>Completed/hr</TableColumn>
            <TableColumn>Lag (s)</TableColumn>
            <TableColumn>Status</TableColumn>
            <TableColumn>Actions</TableColumn>
          </TableHeader>
          <TableBody>
            {queues.map((q) => (
              <TableRow key={q.queue} id={q.queue}>
                <TableCell className="font-medium">
                  <Link
                    to="/queues/$name"
                    params={{ name: q.queue }}
                    className="text-primary no-underline hover:underline"
                  >
                    {q.queue}
                  </Link>
                </TableCell>
                <TableCell>{q.available}</TableCell>
                <TableCell>{q.running}</TableCell>
                <TableCell>
                  <span className={q.failed > 0 ? "text-danger" : ""}>
                    {q.failed}
                  </span>
                </TableCell>
                <TableCell>{q.completed_last_hour}</TableCell>
                <TableCell>
                  <LagValue seconds={q.lag_seconds} />
                </TableCell>
                <TableCell>
                  {q.paused ? (
                    <Badge intent="warning">Paused</Badge>
                  ) : (
                    <Badge intent="success">Active</Badge>
                  )}
                </TableCell>
                <TableCell>
                  <div className="flex gap-1">
                    {q.paused ? (
                      <Button
                        intent="outline"
                        size="xs"
                        onPress={() => resumeMutation.mutate(q.queue)}
                      >
                        Resume
                      </Button>
                    ) : (
                      <Button
                        intent="outline"
                        size="xs"
                        onPress={() => pauseMutation.mutate(q.queue)}
                      >
                        Pause
                      </Button>
                    )}
                    <Button
                      intent="outline"
                      size="xs"
                      className="text-danger"
                      onPress={() => setDrainTarget(q.queue)}
                    >
                      Drain
                    </Button>
                  </div>
                </TableCell>
              </TableRow>
            ))}
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
