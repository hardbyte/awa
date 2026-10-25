import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useNavigate, useSearch } from "@tanstack/react-router";
import {
  fetchQueues,
  fetchQueueHealth,
  fetchQueueHistory,
  fetchQueueRuntime,
  pauseQueue,
  resumeQueue,
  drainQueue,
} from "@/lib/api";
import type { QueueStats } from "@/lib/api";
import {
  joinQueues,
  needsAttention,
  type AssessedQueue,
} from "@/lib/operations";
import { useReadOnly } from "@/hooks/use-read-only";
import { usePollInterval } from "@/hooks/use-poll-interval";
import { toast } from "@/components/ui/toast";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { Heading } from "@/components/ui/heading";
import {
  NativeSelect,
  NativeSelectContent,
} from "@/components/ui/native-select";
import { SearchField, SearchInput } from "@/components/ui/search-field";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { QueueFlowTable } from "@/components/QueueFlowTable";
import { SnapshotStatus } from "@/components/SnapshotStatus";

const SORTS: Record<
  string,
  { label: string; compare?: (a: AssessedQueue, b: AssessedQueue) => number }
> = {
  risk: { label: "Risk" },
  waiting: {
    label: "Most waiting",
    compare: (a, b) => b.queue.available - a.queue.available,
  },
  wait: {
    label: "Longest wait",
    compare: (a, b) =>
      (b.queue.lag_seconds ?? -1) - (a.queue.lag_seconds ?? -1),
  },
  failed: {
    label: "Most failed",
    compare: (a, b) => b.queue.failed - a.queue.failed,
  },
  name: {
    label: "Name",
    compare: (a, b) => a.queue.queue.localeCompare(b.queue.queue),
  },
};

export function QueuesPage() {
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const params = useSearch({ strict: false }) as Record<
    string,
    string | undefined
  >;
  const search = params.q ?? "";
  const sort = params.sort && SORTS[params.sort] ? params.sort : "risk";
  const attentionOnly = params.attention === "1";
  const setParam = (key: string, value: string | undefined) =>
    void navigate({
      to: "/queues",
      search: { ...params, [key]: value || undefined },
      replace: true,
    });

  const [drainTarget, setDrainTarget] = useState<string | null>(null);
  const poll = usePollInterval();
  const readOnly = useReadOnly();
  const polling = { refetchInterval: poll.interval, staleTime: poll.staleTime };
  const queuesQuery = useQuery({
    queryKey: ["queues"],
    queryFn: fetchQueues,
    ...polling,
  });
  const historyQuery = useQuery({
    queryKey: ["queue-history", 60],
    queryFn: () => fetchQueueHistory(60),
    ...polling,
  });
  const healthQuery = useQuery({
    queryKey: ["queue-health"],
    queryFn: fetchQueueHealth,
    ...polling,
  });
  const runtimeQuery = useQuery({
    queryKey: ["queue-runtime"],
    queryFn: fetchQueueRuntime,
    ...polling,
  });
  const runtimeByQueue = new Map(
    (runtimeQuery.data ?? []).map((summary) => [summary.queue, summary]),
  );

  const refresh = () => {
    void queryClient.invalidateQueries({ queryKey: ["queues"] });
    void queryClient.invalidateQueries({ queryKey: ["queue-health"] });
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

  const assessed = joinQueues(
    queuesQuery.data ?? [],
    healthQuery.data,
    historyQuery.data,
  );
  const needle = search.trim().toLowerCase();
  const compare = SORTS[sort]?.compare;
  const rows = assessed
    .filter(
      (row) =>
        (!attentionOnly || needsAttention(row)) &&
        `${row.queue.queue} ${row.queue.display_name ?? ""} ${row.queue.owner ?? ""}`
          .toLowerCase()
          .includes(needle),
    )
    .sort((a, b) =>
      compare
        ? compare(a, b) ||
          (a.assessment?.rank ?? 99) - (b.assessment?.rank ?? 99)
        : 0,
    );

  function controls(queue: QueueStats) {
    return (
      <div className="flex justify-end gap-1 max-@4xl:justify-start">
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
    <div className="space-y-6">
      <div className="space-y-1">
        <Heading level={2}>Queues</Heading>
        <SnapshotStatus
          sources={[queuesQuery, healthQuery, historyQuery, runtimeQuery]}
        />
      </div>
      {(queuesQuery.isError || runtimeQuery.isError) && (
        <div
          role="alert"
          className="rounded-lg border border-danger/40 bg-danger-subtle px-4 py-3 text-sm text-danger-subtle-fg"
        >
          {queuesQuery.isError
            ? "Queue snapshot unavailable or out of date. "
            : ""}
          {runtimeQuery.isError
            ? "Worker capacity unavailable or out of date. "
            : ""}
          Use Refresh data to retry.
        </div>
      )}

      <div className="flex flex-wrap items-end gap-3">
        <SearchField
          aria-label="Find a queue"
          value={search}
          onChange={(value) => setParam("q", value)}
          className="w-full sm:w-72"
        >
          <SearchInput placeholder="Find by name or owner" />
        </SearchField>
        <NativeSelect className="w-auto">
          <NativeSelectContent
            aria-label="Sort queues"
            value={sort}
            onChange={(event) =>
              setParam(
                "sort",
                event.target.value === "risk" ? undefined : event.target.value,
              )
            }
          >
            {Object.entries(SORTS).map(([value, option]) => (
              <option key={value} value={value}>
                Sort: {option.label}
              </option>
            ))}
          </NativeSelectContent>
        </NativeSelect>
        <Checkbox
          isSelected={attentionOnly}
          onChange={(selected) =>
            setParam("attention", selected ? "1" : undefined)
          }
          className="py-2"
        >
          Needs attention only
        </Checkbox>
        <span className="text-sm text-muted-fg sm:ml-auto">
          {rows.length} of {assessed.length} queues
        </span>
      </div>

      <Card className="py-0">
        <CardContent className="px-0 sm:px-0">
          {rows.length ? (
            <QueueFlowTable
              rows={rows}
              controls={controls}
              runtime={runtimeByQueue}
              label="Queues"
            />
          ) : (
            <p className="px-4 py-6 text-sm text-muted-fg sm:px-6">
              {queuesQuery.isPending
                ? "Loading queues…"
                : queuesQuery.isError
                  ? "Queue data unavailable."
                  : assessed.length
                    ? "No queues match these filters."
                    : "No queues yet. Queues appear when registered or used."}
            </p>
          )}
        </CardContent>
      </Card>

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
