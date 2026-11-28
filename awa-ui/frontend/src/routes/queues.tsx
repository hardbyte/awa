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
import { DlqOnBadge } from "@/components/RuntimeDisplay";
import { RowCards } from "@/components/RowCards";
import { formatDuration } from "@/lib/time";

/** A queue's dead-letter count as a link to its Dead letters tab. */
function DlqCount({ queue, counts }: { queue: string; counts: Map<string, number> | null }) {
  if (!counts) return <span className="text-muted-fg">—</span>;
  const count = counts.get(queue) ?? 0;
  if (count === 0) return <span className="text-muted-fg">0</span>;
  return (
    <Link
      to="/queues/$name/dlq"
      params={{ name: queue }}
      className="text-warning-subtle-fg no-underline hover:underline"
      aria-label={`${count} dead letters in ${queue}`}
    >
      {formatNumber(count)}
    </Link>
  );
}

/** Badges for the exceptions only: paused, DLQ on, descriptor trouble and config drift. */
function QueueStatusBadges({ queue, runtime }: { queue: QueueStats; runtime: QueueRuntimeSummary | undefined }) {
  const any =
    queue.paused || runtime?.config?.dlq_enabled === true || queue.descriptor_stale || queue.descriptor_mismatch || runtime?.config_mismatch;
  if (!any) return null;
  return (
    <div className="flex flex-wrap gap-1">
      {queue.paused && <Badge intent="warning">Paused</Badge>}
      <DlqOnBadge config={runtime?.config} />
      {queue.descriptor_stale && <Badge intent="secondary">Descriptor stale</Badge>}
      {queue.descriptor_mismatch && <Badge intent="warning">Descriptor drift</Badge>}
      {runtime?.config_mismatch && <Badge intent="warning">Config mismatch</Badge>}
    </div>
  );
}

/** The one fact a phone card leads with. */
function queueWhy(q: QueueStats, deadLetters: number): string {
  const parts = [
    q.failed > 0 && `${formatNumber(q.failed)} failed`,
    deadLetters > 0 && `${formatNumber(deadLetters)} dead letters`,
    q.retryable > 0 && `${formatNumber(q.retryable)} retrying`,
    q.available > 0 &&
      `${formatNumber(q.available)} ready${q.lag_seconds !== null ? `, oldest ${formatDuration(q.lag_seconds)}` : ""}`,
  ].filter(Boolean);
  return parts.length > 0 ? parts.join(" · ") : "Nothing waiting";
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

      <RowCards
        label="Queues"
        placeholder={
          queuesQuery.isLoading ? (
            "Loading queues…"
          ) : queuesQuery.isError ? (
            <span className="text-danger-subtle-fg">Failed to load queues.</span>
          ) : (
            "No queues found."
          )
        }
        items={queues.map((q) => {
          const runtime = runtimeByQueue.get(q.queue);
          const deadLetters = dlqByQueue?.get(q.queue) ?? 0;
          return {
            key: q.queue,
            title: queueLabel(q),
            link: { to: "/queues/$name", params: { name: q.queue } },
            status: <QueueStatusBadges queue={q} runtime={runtime} />,
            why: queueWhy(q, deadLetters),
            meta: (
              <>
                <span>{formatNumber(q.running)} running</span>
                <span>{formatNumber(q.completed_last_hour)} completed/hr</span>
              </>
            ),
            actions: (
              <>
                {q.paused ? (
                  <AdminButton intent="outline" size="xs" onPress={() => actions.resume.mutate({ queue: q.queue })}>
                    Resume
                  </AdminButton>
                ) : (
                  <AdminButton intent="outline" size="xs" onPress={() => actions.pause.mutate({ queue: q.queue })}>
                    Pause
                  </AdminButton>
                )}
                <AdminButton intent="outline" size="xs" onPress={() => openDrain(q.queue)}>
                  Drain…
                </AdminButton>
              </>
            ),
          };
        })}
      />

      {/* Desktop table layout */}
      <Table aria-label="Queues" className="hidden sm:block">
        <TableHeader>
          <TableColumn isRowHeader>Queue</TableColumn>
          <TableColumn className="text-right">Ready</TableColumn>
          <TableColumn className="text-right">Running</TableColumn>
          <TableColumn className="text-right">Retrying</TableColumn>
          <TableColumn className="text-right">Failed</TableColumn>
          <TableColumn className="text-right">Dead letters</TableColumn>
          <TableColumn className="text-right">Deferred</TableColumn>
          <TableColumn className="text-right">Completed/hr</TableColumn>
          <TableColumn>Capacity</TableColumn>
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
                  <TableCell className="max-w-[13rem] font-medium">
                    <div className="min-w-0">
                      <Link
                        to="/queues/$name"
                        params={{ name: q.queue }}
                        className="text-primary no-underline hover:underline"
                        title={q.display_name?.trim() ? q.queue : undefined}
                      >
                        {queueLabel(q)}
                      </Link>
                      <div className="mt-1 empty:hidden">
                        <QueueStatusBadges queue={q} runtime={runtime} />
                      </div>
                      {q.description && (
                        <div className="mt-0.5 truncate text-xs font-normal text-muted-fg" title={q.description}>
                          {q.description}
                        </div>
                      )}
                      {descriptorSyncLabel(q) && (
                        <div className="mt-0.5 truncate text-xs font-normal text-muted-fg">
                          {descriptorSyncLabel(q)}
                        </div>
                      )}
                    </div>
                  </TableCell>
                  <TableCell className={`text-right tabular-nums ${q.available === 0 ? "text-muted-fg" : ""}`}>
                    <div>{formatNumber(q.available)}</div>
                    {q.available > 0 && q.lag_seconds !== null && (
                      <div className="whitespace-nowrap text-xs text-muted-fg">
                        oldest <LagValue seconds={q.lag_seconds} />
                      </div>
                    )}
                  </TableCell>
                  <TableCell className={`text-right tabular-nums ${q.running === 0 ? "text-muted-fg" : ""}`}>
                    {formatNumber(q.running)}
                  </TableCell>
                  <TableCell className={`text-right tabular-nums ${q.retryable === 0 ? "text-muted-fg" : ""}`}>
                    {formatNumber(q.retryable)}
                  </TableCell>
                  <TableCell
                    className={`text-right tabular-nums ${q.failed === 0 ? "text-muted-fg" : "text-danger-subtle-fg"}`}
                  >
                    {formatNumber(q.failed)}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    <DlqCount queue={q.queue} counts={dlqByQueue} />
                  </TableCell>
                  <TableCell className="text-right tabular-nums text-muted-fg">
                    <div>{formatNumber(q.scheduled)}</div>
                    {q.waiting_external > 0 && (
                      <div className="text-xs">+{formatNumber(q.waiting_external)} waiting</div>
                    )}
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
