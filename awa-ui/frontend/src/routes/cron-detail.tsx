import { useParams } from "@tanstack/react-router";
import { useManagedQuery } from "@/hooks/use-managed-query";
import { AdminButton, useAdminGate } from "@/components/AdminAction";
import { CronControls } from "@/components/CronControls";
import { useNow } from "@/hooks/use-now";
import { useCronActions } from "@/hooks/use-cron-actions";
import { fetchCronJobs, fetchQueues, type CronJobRow, type QueueOverview } from "@/lib/api";
import { formatInTimezone } from "@/lib/time";
import { SECTION_LABELS } from "@/lib/sections";
import { Page, PageHeader } from "@/components/Page";
import { CronPlanNotice } from "@/components/CronPlanNotice";
import { FilterLink, KindLink, QueueLink, TagChip } from "@/components/links";
import { JsonView } from "@/components/JsonView";
import { Time } from "@/components/Time";
import { ResourceLoadError, ResourceNotFound } from "@/components/ResourceNotFound";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import {
  DescriptionDetails,
  DescriptionList,
  DescriptionTerm,
} from "@/components/ui/description-list";
import { CronSchedule, CronStatusBadges } from "./cron";

function hasEntries(value: unknown): boolean {
  return value != null && typeof value === "object" && Object.keys(value as Record<string, unknown>).length > 0;
}

export function CronDetailPage() {
  const { name = "" } = useParams({ strict: false }) as { name?: string };
  const gate = useAdminGate();
  const nowMs = useNow();
  const actions = useCronActions();

  const cronQuery = useManagedQuery<CronJobRow[]>({ queryKey: ["cron"], queryFn: fetchCronJobs });
  const queuesQuery = useManagedQuery<QueueOverview[]>({ queryKey: ["queues"], queryFn: fetchQueues });

  const cron = cronQuery.data?.find((entry) => entry.name === name);
  if (cronQuery.isLoading) return <p className="text-sm text-muted-fg">Loading…</p>;
  if (!cron) {
    if (cronQuery.isError) {
      return (
        <ResourceLoadError
          noun={`schedule "${name}"`}
          error={cronQuery.error}
          onRetry={() => void cronQuery.refetch()}
          backTo="/cron"
          backLabel="Back to schedules"
        />
      );
    }
    return (
      <ResourceNotFound
        title={`Schedule "${name}" not found`}
        reason="No schedule has this name. It may have been removed from its owner's manifest."
        backTo="/cron"
        backLabel="Back to schedules"
      />
    );
  }

  const isPaused = cron.paused_at != null;
  const isRetired = cron.retired_at != null;
  const queuePaused = (queuesQuery.data ?? []).some((q) => q.queue === cron.queue && q.paused);

  return (
    <Page width="readable">
      <PageHeader
        breadcrumbs={[{ label: SECTION_LABELS["/cron"], href: "/cron" }]}
        title={cron.name}
        status={<CronStatusBadges cron={cron} queuePaused={queuePaused} nowMs={nowMs} />}
        actions={
          <CronControls cron={cron} actions={actions} size="sm" />
        }
      />

      {isPaused && (
        <div className="rounded-md border border-warning/30 bg-warning/10 px-3 py-2 text-sm">
          <span className="font-medium">Paused</span> <Time value={cron.paused_at} />
          {cron.paused_by && (
            <>
              {" by "}
              <span className="font-mono">{cron.paused_by}</span>
            </>
          )}
          <span className="text-muted-fg">. Manual triggers still work; resume to restart automatic fires.</span>
        </div>
      )}
      {queuePaused && (
        <div className="rounded-md border border-warning/30 bg-warning/10 px-3 py-2 text-sm">
          <span className="font-medium">Target queue paused.</span>
          <span className="text-muted-fg">
            {" "}Fires still enqueue jobs into <span className="font-mono">{cron.queue}</span>; they run when the queue resumes.
          </span>
        </div>
      )}

      <Card>
        <CardHeader title="Definition" />
        <CardContent>
          <DescriptionList>
            <DescriptionTerm>Schedule</DescriptionTerm>
            <DescriptionDetails><CronSchedule cron={cron} /></DescriptionDetails>
            <DescriptionTerm>Time zone</DescriptionTerm>
            <DescriptionDetails>{cron.timezone}</DescriptionDetails>
            <DescriptionTerm>Kind</DescriptionTerm>
            <DescriptionDetails><KindLink kind={cron.kind} /></DescriptionDetails>
            <DescriptionTerm>Queue</DescriptionTerm>
            <DescriptionDetails><QueueLink queue={cron.queue} /></DescriptionDetails>
            <DescriptionTerm>Priority</DescriptionTerm>
            <DescriptionDetails>{cron.priority}</DescriptionDetails>
            <DescriptionTerm>Max attempts</DescriptionTerm>
            <DescriptionDetails>{cron.max_attempts}</DescriptionDetails>
            {cron.tags.length > 0 && (
              <>
                <DescriptionTerm>Tags</DescriptionTerm>
                <DescriptionDetails>
                  <div className="flex flex-wrap gap-1">
                    {cron.tags.map((tag) => <TagChip key={tag} tag={tag} />)}
                  </div>
                </DescriptionDetails>
              </>
            )}
            {hasEntries(cron.args) && (
              <>
                <DescriptionTerm>Arguments</DescriptionTerm>
                <DescriptionDetails><JsonView data={cron.args} /></DescriptionDetails>
              </>
            )}
            {hasEntries(cron.metadata) && (
              <>
                <DescriptionTerm>Metadata</DescriptionTerm>
                <DescriptionDetails><JsonView data={cron.metadata} /></DescriptionDetails>
              </>
            )}
          </DescriptionList>
        </CardContent>
      </Card>

      <Card>
        <CardHeader title="Runs" />
        <CardContent className="space-y-3">
          <DescriptionList>
            <DescriptionTerm>{isPaused ? "Next fire (if resumed)" : "Next fire"}</DescriptionTerm>
            <DescriptionDetails data-testid="cron-next-fire">
              {isRetired || !cron.next_fire_at ? (
                "—"
              ) : (
                <>
                  {formatInTimezone(cron.next_fire_at, cron.timezone)}{" "}
                  <span className="text-muted-fg">(<Time value={cron.next_fire_at} format="relative" />)</span>
                </>
              )}
            </DescriptionDetails>
            <DescriptionTerm>Last enqueued</DescriptionTerm>
            <DescriptionDetails>
              <Time value={cron.last_enqueued_at} fallback="Never" />
              <p className="text-xs text-muted-fg">Whether that run succeeded is not recorded yet.</p>
            </DescriptionDetails>
            <DescriptionTerm>Created</DescriptionTerm>
            <DescriptionDetails><Time value={cron.created_at} /></DescriptionDetails>
            <DescriptionTerm>Updated</DescriptionTerm>
            <DescriptionDetails><Time value={cron.updated_at} /></DescriptionDetails>
          </DescriptionList>
          <div className="flex flex-col gap-1 text-sm">
            <FilterLink filter={{ kind: cron.kind, cron: cron.name }} data-testid="cron-jobs-link">
              Jobs from this schedule
            </FilterLink>
            <FilterLink filter={{ kind: cron.kind, queue: cron.queue }}>
              Jobs of this kind in this queue
            </FilterLink>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader title="Ownership" />
        <CardContent className="space-y-3">
          <DescriptionList>
            <DescriptionTerm>Owner</DescriptionTerm>
            <DescriptionDetails>{cron.owner_id ?? "Unowned"}</DescriptionDetails>
            {isRetired && (
              <>
                <DescriptionTerm>Retired</DescriptionTerm>
                <DescriptionDetails>
                  <Time value={cron.retired_at} />
                  {cron.retired_by && <> by <span className="font-mono">{cron.retired_by}</span></>}
                  {cron.retired_revision && <> at revision <span className="font-mono">{cron.retired_revision}</span></>}
                </DescriptionDetails>
              </>
            )}
          </DescriptionList>
          <AdminButton
            intent="outline"
            size="sm"
            isDisabled={actions.owner.isPending}
            onPress={() => actions.previewOwner({ action: isRetired ? "restore" : "retire", name: cron.name })}
          >
            {isRetired ? "Preview restore" : "Preview retirement"}
          </AdminButton>
          {actions.plan && (
            <CronPlanNotice
              plan={actions.plan}
              onApply={actions.applyPlan}
              onCancel={actions.clearPlan}
              isDisabled={gate.disabled || actions.owner.isPending}
            />
          )}
        </CardContent>
      </Card>
    </Page>
  );
}
