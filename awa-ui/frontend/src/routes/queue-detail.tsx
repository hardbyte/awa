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
  fetchQueueHealth,
  fetchQueueHistory,
  pauseQueue,
  resumeQueue,
} from "@/lib/api";
import { useReadOnly } from "@/hooks/use-read-only";
import { toast } from "@/components/ui/toast";
import { Heading } from "@/components/ui/heading";
import { Badge } from "@/components/ui/badge";
import { Button, buttonStyles } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import {
  DescriptionDetails,
  DescriptionList,
  DescriptionTerm,
} from "@/components/ui/description-list";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import {
  formatDuration,
  formatRate,
  LIMITED_BY_HINT,
  LIMITED_BY_LABEL,
} from "@/lib/operations";
import { HEALTH_BADGE, SPARK_TONE } from "@/components/QueueFlowTable";
import { HistorySpark } from "@/components/HistorySpark";
import type { QueueRuntimeSummary } from "@/lib/api";
import { usePollInterval } from "@/hooks/use-poll-interval";
import { SnapshotStatus } from "@/components/SnapshotStatus";
import { timeAgo } from "@/lib/time";
import {
  dlqPolicyIntent,
  dlqPolicyLabel,
  queueCapacityLabel,
  rateLimitLabel,
} from "@/components/RuntimeDisplay";

export function QueueDetailPage() {
  const { name } = useParams({ strict: false });
  const queueName = name ?? "";
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const readOnly = useReadOnly();
  const poll = usePollInterval();
  const [showDrainConfirm, setShowDrainConfirm] = useState(false);

  const queueQuery = useQuery({
    queryKey: ["queue", queueName],
    queryFn: () => fetchQueue(queueName),
    enabled: !!queueName,
    refetchInterval: poll.interval, staleTime: poll.staleTime,
  });

  const historyQuery = useQuery({
    queryKey: ["queue-history", 60],
    queryFn: () => fetchQueueHistory(60),
    enabled: !!queueName,
    refetchInterval: poll.interval, staleTime: poll.staleTime,
  });

  const healthQuery = useQuery({
    queryKey: ["queue-health"],
    queryFn: fetchQueueHealth,
    enabled: !!queueName,
    refetchInterval: poll.interval, staleTime: poll.staleTime,
  });

  const runtimeQuery = useQuery<QueueRuntimeSummary[]>({
    queryKey: ["queue-runtime"],
    queryFn: fetchQueueRuntime,
    enabled: !!queueName,
    refetchInterval: poll.interval, staleTime: poll.staleTime,
  });

  const pauseMutation = useMutation({
    mutationFn: () => pauseQueue(queueName, "ui"),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["queue", queueName] });
      void queryClient.invalidateQueries({ queryKey: ["queues"] });
      void queryClient.invalidateQueries({ queryKey: ["queue-health"] });
      toast.success(`Queue "${queueName}" paused`);
    },
    onError: () => toast.error("Failed to pause queue"),
  });

  const resumeMutation = useMutation({
    mutationFn: () => resumeQueue(queueName),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["queue", queueName] });
      void queryClient.invalidateQueries({ queryKey: ["queues"] });
      void queryClient.invalidateQueries({ queryKey: ["queue-health"] });
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
      void queryClient.invalidateQueries({ queryKey: ["queue-health"] });
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
  // Treat empty display_name as missing so a declared-but-blank row
  // doesn't render an empty page title.
  const title = queue.display_name?.trim() ? queue.display_name : queue.queue;
  const series = historyQuery.data?.queues[queue.queue];
  const assessment = healthQuery.data?.queues.find(
    (entry) => entry.queue === queue.queue,
  );
  const health = assessment?.health ?? "unknown";
  const flow = assessment?.flow;
  const flowFacts = [
    flow?.completed_per_minute != null &&
      `${formatRate(flow.completed_per_minute)} completed`,
    flow?.arrivals_per_minute != null &&
      `${formatRate(flow.arrivals_per_minute)} arriving`,
    flow?.failed_per_minute != null &&
      flow.failed_per_minute > 0 &&
      `${formatRate(flow.failed_per_minute)} failing`,
    flow?.mean_claim_wait_seconds != null &&
      `mean claim wait ${formatDuration(flow.mean_claim_wait_seconds)}`,
  ].filter(Boolean);
  const descriptorSyncLabel = queue.descriptor_last_seen_at
    ? timeAgo(queue.descriptor_last_seen_at)
    : null;

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
        {queue.display_name?.trim() && (
          <code className="rounded bg-muted px-1.5 py-0.5 text-sm">
            {queue.queue}
          </code>
        )}
        <Badge intent={HEALTH_BADGE[health]}>{assessment?.label ?? "…"}</Badge>
        {runtime?.config && (
          <Badge intent={dlqPolicyIntent(runtime.config)}>
            {dlqPolicyLabel(runtime.config)}
          </Badge>
        )}
        {queue.descriptor_stale && <Badge intent="warning">Descriptor stale</Badge>}
        {queue.descriptor_mismatch && <Badge intent="danger">Descriptor drift</Badge>}
      </div>

      <SnapshotStatus sources={[queueQuery, runtimeQuery, healthQuery, historyQuery]} />

      <div className="flex flex-wrap items-center gap-x-6 gap-y-2 rounded-lg border px-4 py-3">
        <div className="min-w-0">
          <p className="text-sm font-medium">{assessment?.reason ?? "Checking…"}</p>
          <p className="text-xs text-muted-fg">
            {queue.available.toLocaleString()} waiting
            {queue.available > 0 && ` · oldest ${formatDuration(queue.lag_seconds)}`}
            {flowFacts.length
              ? ` · ${flowFacts.join(" · ")} (last 5m)`
              : ` · ${formatRate(queue.completed_last_hour / 60)} completed (hour average)`}
          </p>
          {assessment?.limited_by && (
            <p className="text-xs" title={LIMITED_BY_HINT[assessment.limited_by]}>
              Limited by: {LIMITED_BY_LABEL[assessment.limited_by]}
            </p>
          )}
        </div>
        <HistorySpark
          data={series?.available ?? []}
          width={240}
          height={32}
          label={`${title} waiting jobs over the last hour`}
          className={SPARK_TONE[health]}
        />
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
        {runtime?.config?.dlq_enabled === true && (
          <Link
            to="/dlq"
            search={{ q: `queue:${queue.queue}` }}
            className={buttonStyles({ intent: "outline", size: "sm" })}
          >
            View DLQ rows
          </Link>
        )}
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

            <DescriptionTerm>Oldest available wait</DescriptionTerm>
            <DescriptionDetails>
              {formatDuration(queue.lag_seconds)}
            </DescriptionDetails>
          </DescriptionList>
        </CardContent>
      </Card>

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

            <DescriptionTerm>Descriptor health</DescriptionTerm>
            <DescriptionDetails>
              {queue.descriptor_mismatch ? (
                "Live runtimes disagree on this queue descriptor"
              ) : descriptorSyncLabel ? (
                <span>
                  {queue.descriptor_stale ? "Stale" : "Live"} · seen {descriptorSyncLabel}
                </span>
              ) : (
                "Not declared by a live worker"
              )}
            </DescriptionDetails>

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
                  <DescriptionDetails>
                    {runtime.config.mode === "weighted" ? "Weighted" : "Reserved"}
                  </DescriptionDetails>

                  <DescriptionTerm>Capacity</DescriptionTerm>
                  <DescriptionDetails>{queueCapacityLabel(runtime.config)}</DescriptionDetails>

                  <DescriptionTerm>Rate limit</DescriptionTerm>
                  <DescriptionDetails>{rateLimitLabel(runtime.config)}</DescriptionDetails>

                  <DescriptionTerm>Poll interval</DescriptionTerm>
                  <DescriptionDetails>{runtime.config.poll_interval_ms} ms</DescriptionDetails>

                  <DescriptionTerm>Deadline duration</DescriptionTerm>
                  <DescriptionDetails>{runtime.config.deadline_duration_secs} s</DescriptionDetails>

                  <DescriptionTerm>Priority aging</DescriptionTerm>
                  <DescriptionDetails>
                    {runtime.config.priority_aging_interval_secs} s
                  </DescriptionDetails>

                  <DescriptionTerm>DLQ policy</DescriptionTerm>
                  <DescriptionDetails>
                    <Badge intent={dlqPolicyIntent(runtime.config)}>
                      {dlqPolicyLabel(runtime.config)}
                    </Badge>
                  </DescriptionDetails>
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
