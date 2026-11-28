import { useMemo, useState } from "react";
import { useManagedQuery } from "@/hooks/use-managed-query";
import { useReadOnly } from "@/hooks/use-read-only";
import { useNow } from "@/hooks/use-now";
import { useCronActions } from "@/hooks/use-cron-actions";
import {
  fetchCronJobs,
  fetchCronReconciliation,
  fetchQueues,
  type CronJobRow,
  type QueueOverview,
} from "@/lib/api";
import { isOverdue } from "@/lib/cron";
import { Page, PageHeader } from "@/components/Page";
import { ListState } from "@/components/ListState";
import { describeCron } from "@/components/CronExpr";
import { CronPlanNotice } from "@/components/CronPlanNotice";
import { KindLink, QueueLink, ScheduleLink } from "@/components/links";
import { Time } from "@/components/Time";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableColumn,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

/** Badges for the states that need a look; a healthy schedule shows none. */
export function CronStatusBadges({
  cron,
  queuePaused,
  nowMs,
}: {
  cron: CronJobRow;
  queuePaused: boolean;
  nowMs: number;
}) {
  return (
    <div className="flex flex-wrap gap-1">
      {cron.retired_at && <Badge intent="warning" data-testid="cron-retired">Retired</Badge>}
      {cron.paused_at && <Badge intent="warning" data-testid="cron-paused">Paused</Badge>}
      {queuePaused && <Badge intent="warning" data-testid="cron-queue-paused">Queue paused</Badge>}
      {isOverdue(cron, nowMs) && <Badge intent="danger" data-testid="cron-overdue">Overdue</Badge>}
    </div>
  );
}

/** The schedule in words with the raw expression after it. */
export function CronSchedule({ cron }: { cron: Pick<CronJobRow, "cron_expr" | "timezone"> }) {
  const words = describeCron(cron.cron_expr);
  return (
    <div className="min-w-0">
      {words !== cron.cron_expr && <div>{words}</div>}
      <code className="font-mono text-xs text-muted-fg">{cron.cron_expr}</code>
      {cron.timezone !== "UTC" && <span className="ml-2 text-xs text-muted-fg">{cron.timezone}</span>}
    </div>
  );
}

export function CronPage() {
  const readOnly = useReadOnly();
  const nowMs = useNow();
  const actions = useCronActions();

  const cronQuery = useManagedQuery<CronJobRow[]>({
    queryKey: ["cron"],
    queryFn: fetchCronJobs,
  });
  const queuesQuery = useManagedQuery<QueueOverview[]>({
    queryKey: ["queues"],
    queryFn: fetchQueues,
  });
  const reconciliation = useManagedQuery({ queryKey: ["cron-reconciliation"], queryFn: fetchCronReconciliation });

  const pausedQueues = useMemo(
    () => new Set((queuesQuery.data ?? []).filter((q) => q.paused).map((q) => q.queue)),
    [queuesQuery.data]
  );

  const [adoptName, setAdoptName] = useState("");
  const [adoptOwner, setAdoptOwner] = useState("");
  const [expectedOwner, setExpectedOwner] = useState("");

  const cronJobs = cronQuery.data ?? [];
  const toggling = actions.pause.isPending || actions.resume.isPending;

  return (
    <Page className="space-y-4">
      <PageHeader title="Schedules" description="Cron schedules and the jobs they enqueue." />
      {(reconciliation.data ?? []).map((plan) => (
        <details key={plan.owner_id} className="rounded-lg border p-3">
          <summary className="cursor-pointer font-medium">
            {plan.owner_id}: {plan.blockers.length ? plan.blockers.join(", ").replace(/_/g, " ") : "converged"}
            {plan.retirements.length > 0 && ` · ${plan.retirements.length} schedules to retire`}
          </summary>
          <div className="space-y-2 pt-2 text-sm">
            <p>Grace remaining: {Math.ceil(plan.grace_remaining_ms / 1000)}s</p>
            {plan.conflicts.map((conflict) => <p key={conflict}>{conflict}</p>)}
            {plan.blocking_instances.length > 0 && <p>Unsupported instances: {plan.blocking_instances.join(", ")}</p>}
            {plan.retirements.length > 0 && <p>Would retire: {plan.retirements.join(", ")}</p>}
            {plan.declarations.map((d) => <p key={d.instance_id} className="break-all">{d.instance_id} · revision {d.revision} · manifest {d.desired_hash} · expires {d.expires_at}</p>)}
            {!readOnly && <div className="flex gap-2">
              <Button intent="outline" size="xs" onPress={() => actions.previewOwner({ action: "retire_owner", owner_id: plan.owner_id })}>Preview owner retirement</Button>
              <Button intent="outline" size="xs" onPress={() => actions.previewOwner({ action: "restore_owner", owner_id: plan.owner_id })}>Preview owner restoration</Button>
            </div>}
          </div>
        </details>
      ))}
      {reconciliation.isError && <p className="text-sm text-danger-subtle-fg" role="alert">Unable to load reconciliation status.</p>}
      {!readOnly && <details className="rounded-lg border p-3">
        <summary className="cursor-pointer">Adopt or transfer a schedule</summary>
        <div className="mt-2 flex flex-wrap gap-2">
          <input aria-label="Schedule name" placeholder="Schedule name" className="rounded border p-2" value={adoptName} onChange={(e) => setAdoptName(e.target.value)} />
          <input aria-label="New owner" placeholder="New owner" className="rounded border p-2" value={adoptOwner} onChange={(e) => setAdoptOwner(e.target.value)} />
          <input aria-label="Expected current owner" placeholder="Current owner (blank if unowned)" className="rounded border p-2" value={expectedOwner} onChange={(e) => setExpectedOwner(e.target.value)} />
          <Button isDisabled={!adoptName || !adoptOwner || actions.owner.isPending} onPress={() => actions.previewOwner({ action: "adopt", name: adoptName, owner_id: adoptOwner, expected_owner: expectedOwner || null })}>Preview</Button>
        </div>
      </details>}
      {actions.plan && (
        <CronPlanNotice
          plan={actions.plan}
          onApply={actions.applyPlan}
          onCancel={actions.clearPlan}
          isDisabled={readOnly || actions.owner.isPending}
        />
      )}

      <Table aria-label="Schedules">
        <TableHeader>
          <TableColumn isRowHeader>Name</TableColumn>
          <TableColumn>Schedule</TableColumn>
          <TableColumn>Next</TableColumn>
          <TableColumn>Last enqueued</TableColumn>
          <TableColumn>Target</TableColumn>
          <TableColumn>Status</TableColumn>
          <TableColumn>Actions</TableColumn>
        </TableHeader>
        <TableBody
          renderEmptyState={() => (
            <ListState
              isLoading={cronQuery.isLoading}
              error={cronQuery.isError ? cronQuery.error : null}
              onRetry={() => void cronQuery.refetch()}
              noun="cron schedules"
              empty="No cron schedules found."
            />
          )}
        >
          {cronJobs.map((cron) => {
            const isPaused = cron.paused_at != null;
            const isRetired = cron.retired_at != null;
            return (
              <TableRow key={cron.name} id={cron.name} data-testid="cron-row">
                <TableCell className="font-medium">
                  <ScheduleLink name={cron.name} />
                  {cron.owner_id && (
                    <div className="text-xs font-normal text-muted-fg">owner {cron.owner_id}</div>
                  )}
                </TableCell>
                <TableCell>
                  <CronSchedule cron={cron} />
                </TableCell>
                <TableCell>
                  {isRetired ? (
                    <span className="text-muted-fg">—</span>
                  ) : (
                    <span className={isPaused ? "text-muted-fg" : ""}>
                      <Time value={cron.next_fire_at} format="relative" />
                      {isPaused && <span className="sr-only"> (if resumed)</span>}
                    </span>
                  )}
                </TableCell>
                <TableCell>
                  <Time value={cron.last_enqueued_at} fallback="Never" />
                </TableCell>
                <TableCell className="text-sm">
                  <KindLink kind={cron.kind} /> <span aria-hidden="true">&rarr;</span>
                  <span className="sr-only"> in queue </span> <QueueLink queue={cron.queue} />
                </TableCell>
                <TableCell>
                  <CronStatusBadges cron={cron} queuePaused={pausedQueues.has(cron.queue)} nowMs={nowMs} />
                </TableCell>
                <TableCell>
                  <div className="flex flex-wrap gap-1">
                    {isPaused ? (
                      <Button intent="outline" size="xs" onPress={() => actions.resume.mutate(cron.name)} isDisabled={readOnly || toggling || isRetired}>
                        Resume
                      </Button>
                    ) : (
                      <Button intent="outline" size="xs" onPress={() => actions.pause.mutate(cron.name)} isDisabled={readOnly || toggling || isRetired}>
                        Pause
                      </Button>
                    )}
                    <Button intent="outline" size="xs" onPress={() => actions.trigger.mutate(cron.name)} isDisabled={readOnly || actions.trigger.isPending || isRetired}>
                      Trigger now
                    </Button>
                    {!readOnly && (
                      <Button intent="outline" size="xs" isDisabled={actions.owner.isPending} onPress={() => actions.previewOwner({ action: isRetired ? "restore" : "retire", name: cron.name })}>
                        {isRetired ? "Restore" : "Retire"}
                      </Button>
                    )}
                  </div>
                </TableCell>
              </TableRow>
            );
          })}
        </TableBody>
      </Table>
    </Page>
  );
}
