import { useMemo, useState } from "react";
import {
  useQuery,
  useMutation,
  useQueryClient,
} from "@tanstack/react-query";
import {
  fetchCronJobs,
  fetchQueues,
  pauseCronJob,
  resumeCronJob,
  triggerCronJob,
} from "@/lib/api";
import { useReadOnly } from "@/hooks/use-read-only";
import { toast } from "@/components/ui/toast";
import type { CronJobRow, QueueOverview } from "@/lib/api";
import { Heading } from "@/components/ui/heading";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { CronExpr } from "@/components/CronExpr";
import { JsonView } from "@/components/JsonView";
import { timeAgo, timeUntil, formatInTimezone } from "@/lib/time";
import { usePollInterval } from "@/hooks/use-poll-interval";

export function CronPage() {
  const queryClient = useQueryClient();
  const [expandedName, setExpandedName] = useState<string | null>(null);
  const readOnly = useReadOnly();
  const poll = usePollInterval();

  const cronQuery = useQuery<CronJobRow[]>({
    queryKey: ["cron"],
    queryFn: fetchCronJobs,
    refetchInterval: poll.interval,
    staleTime: poll.staleTime,
  });

  const queuesQuery = useQuery<QueueOverview[]>({
    queryKey: ["queues"],
    queryFn: fetchQueues,
    refetchInterval: poll.interval,
    staleTime: poll.staleTime,
  });

  const pausedQueues = useMemo(() => {
    const set = new Set<string>();
    for (const q of queuesQuery.data ?? []) {
      if (q.paused) set.add(q.queue);
    }
    return set;
  }, [queuesQuery.data]);

  const invalidateCron = () =>
    queryClient.invalidateQueries({ queryKey: ["cron"] });

  const triggerMutation = useMutation({
    mutationFn: (name: string) => triggerCronJob(name),
    onSuccess: (_data, name) => {
      void queryClient.invalidateQueries({ queryKey: ["jobs"] });
      toast.success(`Cron job "${name}" triggered`);
    },
    onError: () => {
      toast.error("Failed to trigger cron job");
    },
  });

  const pauseMutation = useMutation({
    mutationFn: (name: string) => pauseCronJob(name),
    onSuccess: (_data, name) => {
      void invalidateCron();
      toast.success(`Paused "${name}"`);
    },
    onError: () => {
      toast.error("Failed to pause cron job");
    },
  });

  const resumeMutation = useMutation({
    mutationFn: (name: string) => resumeCronJob(name),
    onSuccess: (_data, name) => {
      void invalidateCron();
      toast.success(`Resumed "${name}"`);
    },
    onError: () => {
      toast.error("Failed to resume cron job");
    },
  });

  const cronJobs = cronQuery.data ?? [];

  return (
    <div className="space-y-4">
      <Heading level={2}>Cron Schedules</Heading>

      {cronJobs.length > 0 ? (
        <div className="space-y-3">
          {cronJobs.map((cj) => {
            const isExpanded = expandedName === cj.name;
            const domId = cj.name.replace(/[^a-zA-Z0-9_-]/g, "-");
            const summaryId = `cron-summary-${domId}`;
            const panelId = `cron-panel-${domId}`;
            const hasArgs =
              cj.args != null &&
              typeof cj.args === "object" &&
              Object.keys(cj.args as Record<string, unknown>).length > 0;
            const hasTags = cj.tags.length > 0;
            const hasMetadata =
              cj.metadata != null &&
              typeof cj.metadata === "object" &&
              Object.keys(cj.metadata as Record<string, unknown>).length > 0;
            const isPaused = cj.paused_at != null;
            const targetQueuePaused = pausedQueues.has(cj.queue);
            const mutating =
              pauseMutation.isPending || resumeMutation.isPending;

            return (
              <div
                key={cj.name}
                className="rounded-lg border transition-colors"
              >
                <div className="flex items-start gap-2 px-4 py-3">
                  <button
                    id={summaryId}
                    type="button"
                    aria-expanded={isExpanded}
                    aria-controls={panelId}
                    className="flex min-w-0 flex-1 flex-wrap items-center gap-3 rounded-md px-2 py-1 -mx-2 -my-1 text-left hover:bg-secondary/30 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-ring"
                    onClick={() =>
                      setExpandedName(isExpanded ? null : cj.name)
                    }
                  >
                    <svg
                      className={`size-4 shrink-0 text-muted-fg transition-transform ${
                        isExpanded ? "rotate-90" : ""
                      }`}
                      viewBox="0 0 20 20"
                      fill="currentColor"
                      aria-hidden="true"
                    >
                      <path
                        fillRule="evenodd"
                        d="M7.21 14.77a.75.75 0 0 1 .02-1.06L11.168 10 7.23 6.29a.75.75 0 1 1 1.04-1.08l4.5 4.25a.75.75 0 0 1 0 1.08l-4.5 4.25a.75.75 0 0 1-1.06-.02Z"
                        clipRule="evenodd"
                      />
                    </svg>

                    <span className="font-medium">{cj.name}</span>
                    {isPaused && (
                      <Badge intent="warning" className="text-[10px]">
                        paused
                      </Badge>
                    )}
                    <CronExpr expr={cj.cron_expr} />
                    {cj.timezone !== "UTC" && (
                      <span className="text-xs text-muted-fg">
                        {cj.timezone}
                      </span>
                    )}

                    <span className="text-sm text-muted-fg">{cj.kind}</span>
                    <span className="text-sm text-muted-fg">
                      &rarr; {cj.queue}
                    </span>
                    {targetQueuePaused && (
                      <Badge intent="warning" className="text-[10px]">
                        queue paused
                      </Badge>
                    )}

                    {cj.priority !== 2 && (
                      <Badge
                        intent={cj.priority === 1 ? "danger" : "secondary"}
                        className="text-[10px]"
                      >
                        P{cj.priority}
                      </Badge>
                    )}

                    <span className="ml-auto flex items-center gap-3 text-sm text-muted-fg">
                      {cj.next_fire_at && !isPaused && (
                        <span
                          className="text-success"
                          title={formatInTimezone(cj.next_fire_at, cj.timezone)}
                        >
                          {timeUntil(cj.next_fire_at)}
                        </span>
                      )}
                      {cj.last_enqueued_at ? (
                        <span title={new Date(cj.last_enqueued_at).toLocaleString()}>
                          {timeAgo(cj.last_enqueued_at)}
                        </span>
                      ) : (
                        "Never run"
                      )}
                    </span>
                  </button>

                  {isPaused ? (
                    <Button
                      intent="outline"
                      size="xs"
                      onPress={() => {
                        resumeMutation.mutate(cj.name);
                      }}
                      isDisabled={readOnly || mutating}
                    >
                      Resume
                    </Button>
                  ) : (
                    <Button
                      intent="outline"
                      size="xs"
                      onPress={() => {
                        pauseMutation.mutate(cj.name);
                      }}
                      isDisabled={readOnly || mutating}
                    >
                      Pause
                    </Button>
                  )}

                  <Button
                    intent="outline"
                    size="xs"
                    onPress={() => {
                      triggerMutation.mutate(cj.name);
                    }}
                    isDisabled={readOnly || triggerMutation.isPending}
                  >
                    Trigger now
                  </Button>
                </div>

                {/* Expanded detail */}
                {isExpanded && (
                  <div
                    id={panelId}
                    aria-labelledby={summaryId}
                    className="border-t px-4 py-3 space-y-3"
                  >
                    {isPaused && (
                      <div className="rounded-md bg-warning/10 border border-warning/30 px-3 py-2 text-sm">
                        <span className="font-medium">Paused</span>
                        {cj.paused_at && (
                          <>
                            {" "}
                            <span className="text-muted-fg">
                              since {new Date(cj.paused_at).toLocaleString()}
                            </span>
                          </>
                        )}
                        {cj.paused_by && (
                          <>
                            {" by "}
                            <span className="font-mono">{cj.paused_by}</span>
                          </>
                        )}
                        <span className="text-muted-fg">
                          {" "}— manual triggers still work; resume to restart automatic fires.
                        </span>
                      </div>
                    )}
                    {targetQueuePaused && (
                      <div className="rounded-md bg-warning/10 border border-warning/30 px-3 py-2 text-sm">
                        <span className="font-medium">Target queue paused.</span>
                        <span className="text-muted-fg">
                          {" "}Fires continue to enqueue jobs into{" "}
                          <span className="font-mono">{cj.queue}</span>; they
                          dispatch when the queue is resumed.
                        </span>
                      </div>
                    )}

                    <div className="grid grid-cols-2 gap-x-8 gap-y-2 text-sm sm:grid-cols-4">
                      <div>
                        <dt className="text-muted-fg">Kind</dt>
                        <dd className="font-medium">{cj.kind}</dd>
                      </div>
                      <div>
                        <dt className="text-muted-fg">Queue</dt>
                        <dd className="font-medium">{cj.queue}</dd>
                      </div>
                      <div>
                        <dt className="text-muted-fg">Priority</dt>
                        <dd className="font-medium">{cj.priority}</dd>
                      </div>
                      <div>
                        <dt className="text-muted-fg">Max attempts</dt>
                        <dd className="font-medium">{cj.max_attempts}</dd>
                      </div>
                    </div>

                    {hasTags && (
                      <div>
                        <dt className="text-sm text-muted-fg mb-1">Tags</dt>
                        <div className="flex flex-wrap gap-1">
                          {cj.tags.map((tag) => (
                            <Badge key={tag} intent="secondary" className="text-[10px]">
                              {tag}
                            </Badge>
                          ))}
                        </div>
                      </div>
                    )}

                    {hasArgs && (
                      <div>
                        <dt className="text-sm text-muted-fg mb-1">Arguments</dt>
                        <JsonView data={cj.args} />
                      </div>
                    )}

                    {hasMetadata && (
                      <div>
                        <dt className="text-sm text-muted-fg mb-1">Metadata</dt>
                        <JsonView data={cj.metadata} />
                      </div>
                    )}

                    {cj.next_fire_at && (
                      <div>
                        <dt className="text-sm text-muted-fg">
                          {isPaused ? "Next fire (if resumed)" : "Next fire"}
                        </dt>
                        <dd className="font-medium">
                          {formatInTimezone(cj.next_fire_at, cj.timezone)}
                          <span className="ml-2 text-muted-fg font-normal">
                            ({timeUntil(cj.next_fire_at)})
                          </span>
                        </dd>
                      </div>
                    )}

                    <div className="text-xs text-muted-fg">
                      Created {new Date(cj.created_at).toLocaleString()}
                      {" · "}
                      Updated {new Date(cj.updated_at).toLocaleString()}
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      ) : cronQuery.isLoading ? (
        <p className="text-sm text-muted-fg">Loading...</p>
      ) : (
        <p className="text-sm text-muted-fg">No cron schedules found.</p>
      )}
    </div>
  );
}
