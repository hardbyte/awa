import { useState } from "react";
import {
  useQuery,
  useMutation,
  useQueryClient,
} from "@tanstack/react-query";
import { useParams, Link, useNavigate } from "@tanstack/react-router";
import {
  fetchJobs,
  fetchQueues,
  pauseQueue,
  resumeQueue,
  drainQueue,
} from "@/lib/api";
import type { JobRow, QueueStats } from "@/lib/api";
import { StateBadge } from "@/components/StateBadge";
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

function timeAgo(dateStr: string): string {
  const seconds = Math.floor(
    (Date.now() - new Date(dateStr).getTime()) / 1000
  );
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

export function QueueDetailPage() {
  const { name } = useParams({ strict: false });
  const queueName = name ?? "";
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const [showDrainConfirm, setShowDrainConfirm] = useState(false);

  const queuesQuery = useQuery<QueueStats[]>({
    queryKey: ["queues"],
    queryFn: fetchQueues,
  });

  const jobsQuery = useQuery<JobRow[]>({
    queryKey: ["jobs", { queue: queueName, limit: 50 }],
    queryFn: () => fetchJobs({ queue: queueName, limit: 50 }),
    enabled: !!queueName,
  });

  const queueStats = queuesQuery.data?.find((q) => q.queue === queueName);

  const pauseMutation = useMutation({
    mutationFn: () => pauseQueue(queueName, "ui"),
    onSuccess: () =>
      void queryClient.invalidateQueries({ queryKey: ["queues"] }),
  });

  const resumeMutation = useMutation({
    mutationFn: () => resumeQueue(queueName),
    onSuccess: () =>
      void queryClient.invalidateQueries({ queryKey: ["queues"] }),
  });

  const drainMutation = useMutation({
    mutationFn: () => drainQueue(queueName),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["queues"] });
      void queryClient.invalidateQueries({ queryKey: ["jobs"] });
    },
  });

  const jobs = jobsQuery.data ?? [];

  return (
    <div className="space-y-6">
      {/* Header with queue info and actions */}
      <div className="flex flex-wrap items-center gap-4">
        <Heading level={2}>Queue: {queueName}</Heading>
        {queueStats && (
          queueStats.paused ? (
            <Badge intent="warning">Paused</Badge>
          ) : (
            <Badge intent="success">Active</Badge>
          )
        )}
        <div className="flex gap-1">
          {queueStats?.paused ? (
            <Button
              intent="outline"
              size="sm"
              onPress={() => resumeMutation.mutate()}
            >
              Resume
            </Button>
          ) : (
            <Button
              intent="outline"
              size="sm"
              onPress={() => pauseMutation.mutate()}
            >
              Pause
            </Button>
          )}
          <Button
            intent="outline"
            size="sm"
            className="text-danger"
            onPress={() => setShowDrainConfirm(true)}
          >
            Drain
          </Button>
        </div>
      </div>

      {/* Stats summary */}
      {queueStats && (
        <div className="flex flex-wrap gap-4 text-sm text-muted-fg">
          <span>Available: {queueStats.available}</span>
          <span>Running: {queueStats.running}</span>
          <span
            className={queueStats.failed > 0 ? "text-danger" : ""}
          >
            Failed: {queueStats.failed}
          </span>
          <span>Completed/hr: {queueStats.completed_last_hour}</span>
          <span>
            Lag:{" "}
            {queueStats.lag_seconds != null
              ? `${queueStats.lag_seconds.toFixed(1)}s`
              : "-"}
          </span>
        </div>
      )}

      {/* Job table */}
      <Table aria-label={`Jobs in queue ${queueName}`}>
        <TableHeader>
          <TableColumn isRowHeader>ID</TableColumn>
          <TableColumn>State</TableColumn>
          <TableColumn>Kind</TableColumn>
          <TableColumn>Attempt</TableColumn>
          <TableColumn>Created</TableColumn>
        </TableHeader>
        <TableBody
          renderEmptyState={() =>
            jobsQuery.isLoading ? (
              <div className="flex h-32 items-center justify-center text-sm text-muted-fg">
                Loading...
              </div>
            ) : (
              <div className="flex h-32 items-center justify-center text-sm text-muted-fg">
                No jobs in this queue.
              </div>
            )
          }
        >
          {jobs.map((job) => (
            <TableRow
              key={job.id}
              id={job.id}
              onAction={() =>
                void navigate({
                  to: "/jobs/$id",
                  params: { id: String(job.id) },
                })
              }
              className="cursor-pointer"
            >
              <TableCell className="font-mono">{job.id}</TableCell>
              <TableCell>
                <StateBadge state={job.state} />
              </TableCell>
              <TableCell>{job.kind}</TableCell>
              <TableCell>
                {job.attempt}/{job.max_attempts}
              </TableCell>
              <TableCell>{timeAgo(job.created_at)}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>

      <div>
        <Link
          to="/queues"
          className="text-sm text-primary no-underline hover:underline"
        >
          &larr; All queues
        </Link>
      </div>

      <ConfirmDialog
        isOpen={showDrainConfirm}
        onOpenChange={setShowDrainConfirm}
        title={`Drain queue "${queueName}"`}
        description="This will cancel all available, scheduled, retryable, and waiting_external jobs in this queue. Running jobs will not be affected."
        confirmLabel="Drain queue"
        confirmIntent="danger"
        onConfirm={() => drainMutation.mutate()}
        isPending={drainMutation.isPending}
      />
    </div>
  );
}
