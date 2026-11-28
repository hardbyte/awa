import { useManagedQuery } from "@/hooks/use-managed-query";
import { useState } from "react";
import { Link } from "@tanstack/react-router";
import {
  fetchQueues,
  fetchQueueRuntime,
} from "@/lib/api";
import { useQueueActions } from "@/hooks/use-queue-actions";
import { AdminButton, DisabledReason, useAdminGate } from "@/components/AdminAction";
import { DrainDialog } from "@/components/DrainDialog";
import type { QueueRuntimeSummary, QueueStats } from "@/lib/api";
import { Page, PageHeader } from "@/components/Page";
import { Badge } from "@/components/ui/badge";
import { Menu, MenuContent, MenuItem, MenuTrigger } from "@/components/ui/menu";
import {
  Table,
  TableHeader,
  TableBody,
  TableRow,
  TableCell,
  TableColumn,
} from "@/components/ui/table";
import { LagValue } from "@/components/LagValue";
import { dlqDepthQuery } from "@/lib/shared-queries";
import { formatNumber } from "@/lib/number";
import { timeAgo } from "@/lib/time";
import {
  dlqPolicyIntent,
  dlqPolicyLabel,
} from "@/components/RuntimeDisplay";

/** A queue's dead-letter count as a link to its Dead letters tab. */
function DlqCount({ queue, counts }: { queue: string; counts: Map<string, number> | null }) {
  if (!counts) return <span className="text-muted-fg">—</span>;
  const count = counts.get(queue) ?? 0;
  if (count === 0) return <span className="text-muted-fg/60">0</span>;
  return (
    <Link
      to="/queues/$name/dlq"
      params={{ name: queue }}
      className="text-danger-subtle-fg no-underline hover:underline"
      aria-label={`${count} dead letters in ${queue}`}
    >
      +{formatNumber(count)}
    </Link>
  );
}

export function QueuesPage() {
  const [drainTarget, setDrainTarget] = useState<string | null>(null);
  // Kept while the dialog animates out, so its title doesn't go blank.
  const [lastDrainTarget, setLastDrainTarget] = useState("");
  const openDrain = (queue: string) => {
    setLastDrainTarget(queue);
    setDrainTarget(queue);
  };

  const queuesQuery = useManagedQuery<QueueStats[]>({
    queryKey: ["queues"],
    queryFn: fetchQueues,
  });

  const runtimeQuery = useManagedQuery<QueueRuntimeSummary[]>({
    queryKey: ["queue-runtime"],
    queryFn: fetchQueueRuntime,
  });

  const actions = useQueueActions();
  const gate = useAdminGate();

  const depthQuery = useManagedQuery(dlqDepthQuery);
  const dlqByQueue = depthQuery.isError
    ? null
    : new Map((depthQuery.data?.by_queue ?? []).map((entry) => [entry.queue, entry.count]));

  const queues = queuesQuery.data ?? [];
  const runtimeByQueue = new Map<string, QueueRuntimeSummary>(
    (runtimeQuery.data ?? []).map(
      (summary): [string, QueueRuntimeSummary] => [summary.queue, summary],
    ),
  );

  function capacityLabel(runtime: QueueRuntimeSummary | undefined): string {
    if (!runtime?.config) return "—";
    if (runtime.config.mode === "weighted") {
      return `min ${runtime.config.min_workers ?? 0} / weight ${runtime.config.weight ?? 1}`;
    }
    return `max ${runtime.config.max_workers ?? 0}`;
  }

  function rateLimitLabel(runtime: QueueRuntimeSummary | undefined): string {
    if (!runtime?.config?.rate_limit) return "—";
    return `${runtime.config.rate_limit.max_rate}/s · burst ${runtime.config.rate_limit.burst}`;
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

  // Only surfaces a line when the descriptor needs attention — drift, stale,
  // or not declared. A fresh descriptor is implied by the row's presence.
  function descriptorSyncLabel(q: QueueStats): string | null {
    if (q.descriptor_mismatch) return "Descriptor drift across live runtimes";
    if (!q.descriptor_last_seen_at) return "Descriptor not declared";
    if (q.descriptor_stale) {
      return `Descriptor stale · seen ${timeAgo(q.descriptor_last_seen_at)}`;
    }
    return null;
  }

  return (
    <Page className="space-y-4">
      <PageHeader title="Queues" />

      {/* Mobile card layout */}
      {queues.length === 0 ? (
        <div
          className={`rounded-lg border p-6 text-center text-sm sm:hidden ${queuesQuery.isError ? "text-danger-subtle-fg" : "text-muted-fg"}`}
        >
          {queuesQuery.isLoading
            ? "Loading queues…"
            : queuesQuery.isError
              ? "Failed to load queues."
              : "No queues found."}
        </div>
      ) : (
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
                  {descriptorSyncLabel(q) && (
                    <div className="mt-1 text-xs text-muted-fg">
                      {descriptorSyncLabel(q)}
                    </div>
                  )}
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
                <span className="text-muted-fg">Dead letters</span>
                <span><DlqCount queue={q.queue} counts={dlqByQueue} /></span>
                <span className="text-muted-fg">Lag</span>
                <span><LagValue seconds={q.lag_seconds} /></span>
                <span className="text-muted-fg">Capacity</span>
                <span>{capacityLabel(runtime)}</span>
                <span className="text-muted-fg">Rate limit</span>
                <span>{rateLimitLabel(runtime)}</span>
                <span className="text-muted-fg">DLQ</span>
                <span>{dlqPolicyLabel(runtime?.config)}</span>
                <span className="text-muted-fg">Healthy nodes</span>
                <span>{runtimeHealthLabel(runtime)}</span>
                {runtime?.config_mismatch && (
                  <>
                    <span className="text-muted-fg">Config</span>
                    <span className="text-warning-subtle-fg">Mismatch</span>
                  </>
                )}
              </div>
              <div className="mt-3 flex gap-2">
                {q.paused ? (
                  <AdminButton intent="outline" size="xs" onPress={() => actions.resume.mutate({ queue: q.queue })}>
                    Resume
                  </AdminButton>
                ) : (
                  <AdminButton intent="outline" size="xs" onPress={() => actions.pause.mutate({ queue: q.queue })}>
                    Pause
                  </AdminButton>
                )}
                <AdminButton intent="outline" size="xs" className="text-danger" onPress={() => openDrain(q.queue)}>
                  Drain
                </AdminButton>
              </div>
              </div>
            );
          })}
        </div>
      )}

      {/* Desktop table layout */}
      <Table aria-label="Queues" className="hidden sm:block">
        <TableHeader>
          <TableColumn isRowHeader>Queue</TableColumn>
          <TableColumn className="text-right">Queued</TableColumn>
          <TableColumn className="text-right">Running</TableColumn>
          <TableColumn className="text-right">Retry</TableColumn>
          <TableColumn className="text-right">Failed</TableColumn>
          <TableColumn className="text-right">Dead letters</TableColumn>
          <TableColumn className="text-right">Completed/hr</TableColumn>
          <TableColumn>Capacity</TableColumn>
          <TableColumn>Status</TableColumn>
          <TableColumn>Actions</TableColumn>
        </TableHeader>
        <TableBody
          renderEmptyState={() => (
            <div
              className={`p-6 text-center text-sm ${queuesQuery.isError ? "text-danger-subtle-fg" : "text-muted-fg"}`}
            >
              {queuesQuery.isLoading
                ? "Loading queues…"
                : queuesQuery.isError
                  ? "Failed to load queues."
                  : "No queues found."}
            </div>
          )}
        >
          {queues.map((q) => {
              const runtime = runtimeByQueue.get(q.queue);
              return (
                <TableRow key={q.queue} id={q.queue}>
                  <TableCell className="max-w-[20rem] font-medium">
                    <div className="min-w-0">
                      <Link
                        to="/queues/$name"
                        params={{ name: q.queue }}
                        className="text-primary no-underline hover:underline"
                      >
                        {queueLabel(q)}
                      </Link>
                      {q.display_name && (
                        <div className="truncate text-xs font-normal text-muted-fg">
                          {q.queue}
                        </div>
                      )}
                      {q.description && (
                        <div
                          className="mt-0.5 truncate text-xs font-normal text-muted-fg"
                          title={q.description}
                        >
                          {q.description}
                        </div>
                      )}
                      {q.waiting_external > 0 && (
                        <div className="mt-0.5 text-xs text-muted-fg">
                          {q.waiting_external.toLocaleString()} waiting · lag{" "}
                          <LagValue seconds={q.lag_seconds} />
                        </div>
                      )}
                      {descriptorSyncLabel(q) && (
                        <div className="mt-0.5 truncate text-xs font-normal text-muted-fg">
                          {descriptorSyncLabel(q)}
                        </div>
                      )}
                    </div>
                  </TableCell>
                  <TableCell
                    className={`text-right tabular-nums ${
                      q.total_queued === 0 ? "text-muted-fg/60" : ""
                    }`}
                  >
                    {q.total_queued.toLocaleString()}
                  </TableCell>
                  <TableCell
                    className={`text-right tabular-nums ${
                      q.running === 0 ? "text-muted-fg/60" : ""
                    }`}
                  >
                    {q.running.toLocaleString()}
                  </TableCell>
                  <TableCell
                    className={`text-right tabular-nums ${
                      q.retryable === 0 ? "text-muted-fg/60" : "text-warning-subtle-fg"
                    }`}
                  >
                    {q.retryable.toLocaleString()}
                  </TableCell>
                  <TableCell
                    className={`text-right tabular-nums ${
                      q.failed === 0 ? "text-muted-fg/60" : "text-danger"
                    }`}
                  >
                    {q.failed.toLocaleString()}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    <DlqCount queue={q.queue} counts={dlqByQueue} />
                  </TableCell>
                  <TableCell className="text-right tabular-nums text-muted-fg">
                    {q.completed_last_hour.toLocaleString()}
                  </TableCell>
                  <TableCell>
                    <div className="flex flex-col gap-0.5 text-sm">
                      <div>
                        {runtime?.config ? (
                          <span className="text-muted-fg">
                            {runtime.config.mode === "weighted" ? "Weighted" : "Reserved"} ·{" "}
                            <span className="text-fg">{capacityLabel(runtime)}</span>
                          </span>
                        ) : (
                          <span className="text-muted-fg">—</span>
                        )}
                      </div>
                      {rateLimitLabel(runtime) !== "—" && (
                        <div className="text-xs text-muted-fg">
                          rate {rateLimitLabel(runtime)}
                        </div>
                      )}
                      {runtime && (
                        <div className="text-xs text-muted-fg">
                          {runtime.config_mismatch ? (
                            <span className="text-warning-subtle-fg">Config mismatch</span>
                          ) : (
                            `${runtimeHealthLabel(runtime)} healthy`
                          )}
                        </div>
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
                      {runtime?.config && (
                        <Badge intent={dlqPolicyIntent(runtime.config)}>
                          {dlqPolicyLabel(runtime.config)}
                        </Badge>
                      )}
                    </div>
                  </TableCell>
                  <TableCell>
                    <div className="flex justify-end">
                      <DisabledReason reason={gate.reason}>
                      {(describedBy) => (
                      <Menu>
                        <MenuTrigger
                          isDisabled={gate.disabled}
                          aria-describedby={describedBy}
                          aria-label={`Actions for ${q.queue}`}
                          className="inline-flex size-7 items-center justify-center rounded text-muted-fg hover:bg-muted hover:text-fg disabled:opacity-40"
                        >
                          <svg
                            viewBox="0 0 20 20"
                            fill="currentColor"
                            aria-hidden="true"
                            className="size-4"
                          >
                            <path d="M10 6a1.5 1.5 0 1 1 0-3 1.5 1.5 0 0 1 0 3zm0 5.5a1.5 1.5 0 1 1 0-3 1.5 1.5 0 0 1 0 3zm0 5.5a1.5 1.5 0 1 1 0-3 1.5 1.5 0 0 1 0 3z" />
                          </svg>
                        </MenuTrigger>
                        <MenuContent placement="bottom end">
                          {q.paused ? (
                            <MenuItem onAction={() => actions.resume.mutate({ queue: q.queue })}>
                              Resume queue
                            </MenuItem>
                          ) : (
                            <MenuItem onAction={() => actions.pause.mutate({ queue: q.queue })}>
                              Pause queue
                            </MenuItem>
                          )}
                          <MenuItem
                            intent="danger"
                            onAction={() => openDrain(q.queue)}
                          >
                            Drain queue…
                          </MenuItem>
                        </MenuContent>
                      </Menu>
                      )}
                      </DisabledReason>
                    </div>
                  </TableCell>
                </TableRow>
              );
            })}
        </TableBody>
      </Table>

      <DrainDialog
        queue={drainTarget ?? lastDrainTarget}
        isOpen={drainTarget !== null}
        onOpenChange={(open) => {
          if (!open) setDrainTarget(null);
        }}
        drain={actions.drain}
      />
    </Page>
  );
}
