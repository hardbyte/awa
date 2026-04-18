import { useState } from "react";
import {
  useQuery,
  useMutation,
  useQueryClient,
} from "@tanstack/react-query";
import { Link, useNavigate, useParams } from "@tanstack/react-router";
import {
  drainQueue,
  fetchQueue,
  fetchQueueRuntime,
  pauseQueue,
  resumeQueue,
} from "@/lib/api";
import { useReadOnly } from "@/hooks/use-read-only";
import { toast } from "@/components/ui/toast";
import { Heading } from "@/components/ui/heading";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import {
  DescriptionDetails,
  DescriptionList,
  DescriptionTerm,
} from "@/components/ui/description-list";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { LagValue } from "@/components/LagValue";
import type { QueueRuntimeSummary } from "@/lib/api";

export function QueueDetailPage() {
  const { name } = useParams({ strict: false });
  const queueName = name ?? "";
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const readOnly = useReadOnly();
  const [showDrainConfirm, setShowDrainConfirm] = useState(false);

  const queueQuery = useQuery({
    queryKey: ["queue", queueName],
    queryFn: () => fetchQueue(queueName),
    enabled: !!queueName,
  });

  const runtimeQuery = useQuery<QueueRuntimeSummary[]>({
    queryKey: ["queue-runtime"],
    queryFn: fetchQueueRuntime,
    enabled: !!queueName,
  });

  const pauseMutation = useMutation({
    mutationFn: () => pauseQueue(queueName, "ui"),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["queue", queueName] });
      void queryClient.invalidateQueries({ queryKey: ["queues"] });
      toast.success(`Queue "${queueName}" paused`);
    },
    onError: () => toast.error("Failed to pause queue"),
  });

  const resumeMutation = useMutation({
    mutationFn: () => resumeQueue(queueName),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["queue", queueName] });
      void queryClient.invalidateQueries({ queryKey: ["queues"] });
      toast.success(`Queue "${queueName}" resumed`);
    },
    onError: () => toast.error("Failed to resume queue"),
  });

  const drainMutation = useMutation({
    mutationFn: () => drainQueue(queueName),
    onSuccess: () => {
      setShowDrainConfirm(false);
      void queryClient.invalidateQueries({ queryKey: ["queue", queueName] });
      void queryClient.invalidateQueries({ queryKey: ["queues"] });
      void queryClient.invalidateQueries({ queryKey: ["jobs"] });
      toast.success(`Queue "${queueName}" drained`);
    },
    onError: () => toast.error("Failed to drain queue"),
  });

  if (!queueName) {
    return <p className="text-sm text-muted-fg">Queue not found.</p>;
  }
  if (queueQuery.isLoading) {
    return <p className="text-sm text-muted-fg">Loading...</p>;
  }
  if (queueQuery.error) {
    return (
      <p className="text-sm text-danger">Error: {String(queueQuery.error)}</p>
    );
  }
  if (!queueQuery.data) {
    return <p className="text-sm text-muted-fg">Queue not found.</p>;
  }

  const queue = queueQuery.data;
  const runtime = runtimeQuery.data?.find((entry) => entry.queue === queue.queue);
  const title = queue.display_name ?? queue.queue;

  return (
    <div className="space-y-6">
      <Link
        to="/queues"
        className="text-sm text-muted-fg no-underline hover:text-fg"
      >
        &larr; Back to queues
      </Link>

      <div className="flex flex-wrap items-center gap-4">
        <Heading level={2}>{title}</Heading>
        {queue.display_name && (
          <code className="rounded bg-muted px-1.5 py-0.5 text-sm">
            {queue.queue}
          </code>
        )}
        {queue.paused ? (
          <Badge intent="warning">Paused</Badge>
        ) : (
          <Badge intent="success">Active</Badge>
        )}
      </div>

      {queue.description && (
        <p className="max-w-3xl text-sm text-muted-fg">{queue.description}</p>
      )}

      <div className="flex flex-wrap gap-2">
        <Button
          intent="primary"
          size="sm"
          onPress={() => void navigate({ to: "/jobs", search: { q: `queue:${queue.queue}` } })}
        >
          View jobs
        </Button>
        {queue.paused ? (
          <Button
            intent="outline"
            size="sm"
            onPress={() => resumeMutation.mutate()}
            isDisabled={readOnly || resumeMutation.isPending}
          >
            Resume
          </Button>
        ) : (
          <Button
            intent="outline"
            size="sm"
            onPress={() => pauseMutation.mutate()}
            isDisabled={readOnly || pauseMutation.isPending}
          >
            Pause
          </Button>
        )}
        <Button
          intent="outline"
          size="sm"
          className="text-danger"
          onPress={() => setShowDrainConfirm(true)}
          isDisabled={readOnly || drainMutation.isPending}
        >
          Drain
        </Button>
      </div>

      <Card>
        <CardHeader title="Descriptor" />
        <CardContent>
          <DescriptionList>
            <DescriptionTerm>Queue key</DescriptionTerm>
            <DescriptionDetails>
              <code className="rounded bg-muted px-1.5 py-0.5 text-sm">
                {queue.queue}
              </code>
            </DescriptionDetails>

            {queue.owner && (
              <>
                <DescriptionTerm>Owner</DescriptionTerm>
                <DescriptionDetails>{queue.owner}</DescriptionDetails>
              </>
            )}

            {queue.docs_url && (
              <>
                <DescriptionTerm>Docs</DescriptionTerm>
                <DescriptionDetails>
                  <a
                    href={queue.docs_url}
                    target="_blank"
                    rel="noreferrer"
                    className="text-primary no-underline hover:underline"
                  >
                    {queue.docs_url}
                  </a>
                </DescriptionDetails>
              </>
            )}

            {queue.tags.length > 0 && (
              <>
                <DescriptionTerm>Tags</DescriptionTerm>
                <DescriptionDetails>
                  <div className="flex flex-wrap gap-1">
                    {queue.tags.map((tag) => (
                      <Badge key={tag} intent="secondary">
                        {tag}
                      </Badge>
                    ))}
                  </div>
                </DescriptionDetails>
              </>
            )}
          </DescriptionList>
        </CardContent>
      </Card>

      <Card>
        <CardHeader title="Queue stats" />
        <CardContent>
          <DescriptionList>
            <DescriptionTerm>Total queued</DescriptionTerm>
            <DescriptionDetails>{queue.total_queued.toLocaleString()}</DescriptionDetails>

            <DescriptionTerm>Scheduled</DescriptionTerm>
            <DescriptionDetails>{queue.scheduled.toLocaleString()}</DescriptionDetails>

            <DescriptionTerm>Available</DescriptionTerm>
            <DescriptionDetails>{queue.available.toLocaleString()}</DescriptionDetails>

            <DescriptionTerm>Retryable</DescriptionTerm>
            <DescriptionDetails>{queue.retryable.toLocaleString()}</DescriptionDetails>

            <DescriptionTerm>Running</DescriptionTerm>
            <DescriptionDetails>{queue.running.toLocaleString()}</DescriptionDetails>

            <DescriptionTerm>Waiting external</DescriptionTerm>
            <DescriptionDetails>{queue.waiting_external.toLocaleString()}</DescriptionDetails>

            <DescriptionTerm>Failed</DescriptionTerm>
            <DescriptionDetails>{queue.failed.toLocaleString()}</DescriptionDetails>

            <DescriptionTerm>Completed last hour</DescriptionTerm>
            <DescriptionDetails>{queue.completed_last_hour.toLocaleString()}</DescriptionDetails>

            <DescriptionTerm>Lag</DescriptionTerm>
            <DescriptionDetails>
              <LagValue seconds={queue.lag_seconds} />
            </DescriptionDetails>
          </DescriptionList>
        </CardContent>
      </Card>

      {runtime && (
        <Card>
          <CardHeader title="Runtime" />
          <CardContent>
            <DescriptionList>
              <DescriptionTerm>Instances</DescriptionTerm>
              <DescriptionDetails>
                {runtime.live_instances}/{runtime.instance_count} live
              </DescriptionDetails>

              <DescriptionTerm>Healthy instances</DescriptionTerm>
              <DescriptionDetails>{runtime.healthy_instances}</DescriptionDetails>

              <DescriptionTerm>In flight</DescriptionTerm>
              <DescriptionDetails>{runtime.total_in_flight}</DescriptionDetails>

              {runtime.config && (
                <>
                  <DescriptionTerm>Mode</DescriptionTerm>
                  <DescriptionDetails>{runtime.config.mode}</DescriptionDetails>

                  <DescriptionTerm>Poll interval</DescriptionTerm>
                  <DescriptionDetails>{runtime.config.poll_interval_ms} ms</DescriptionDetails>
                </>
              )}

              {runtime.config_mismatch && (
                <>
                  <DescriptionTerm>Config</DescriptionTerm>
                  <DescriptionDetails>
                    <Badge intent="warning">Mismatch</Badge>
                  </DescriptionDetails>
                </>
              )}
            </DescriptionList>
          </CardContent>
        </Card>
      )}

      <ConfirmDialog
        isOpen={showDrainConfirm}
        onOpenChange={setShowDrainConfirm}
        onConfirm={() => drainMutation.mutate()}
        title="Drain queue?"
        description={`Cancel all available, scheduled, retryable, and waiting jobs in "${queue.queue}". Running jobs are not affected.`}
        confirmLabel="Drain queue"
        isPending={drainMutation.isPending}
      />
    </div>
  );
}
