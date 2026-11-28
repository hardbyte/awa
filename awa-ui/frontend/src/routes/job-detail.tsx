import { useManagedQuery } from "@/hooks/use-managed-query";
import { Time } from "@/components/Time";
import { useEffect, useState } from "react";
import {
  useMutation,
  useQueryClient,
} from "@tanstack/react-query";
import { useParams, useNavigate } from "@tanstack/react-router";
import { fetchJob, retryJob, cancelJob } from "@/lib/api";
import { useReadOnly } from "@/hooks/use-read-only";
import { toast } from "@/components/ui/toast";
import type { JobRow } from "@/lib/api";
import { StateBadge } from "@/components/StateBadge";
import { Page, PageHeader } from "@/components/Page";
import { KindLink, QueueLink, ScheduleLink, TagChip } from "@/components/links";
import { SECTION_LABELS } from "@/lib/sections";
import { Button } from "@/components/ui/button";
import { Card, CardHeader, CardContent } from "@/components/ui/card";
import {
  DescriptionList,
  DescriptionTerm,
  DescriptionDetails,
} from "@/components/ui/description-list";
import { Badge } from "@/components/ui/badge";
import { JsonView } from "@/components/JsonView";
import { ProgressDisplay } from "@/components/ProgressDisplay";
import { Copyable } from "@/components/CopyButton";
import { CopyButton } from "@/components/CopyButton";
import { JobTimeline } from "@/components/JobTimeline";
import { OutputCard } from "@/components/OutputCard";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { isNotFoundError, ResourceLoadError, ResourceNotFound } from "@/components/ResourceNotFound";

function hasMetadata(job: JobRow): boolean {
  return (
    job.metadata != null &&
    typeof job.metadata === "object" &&
    Object.keys(job.metadata as Record<string, unknown>).length > 0
  );
}

// Treat empty display_name the same as missing — the contract is
// "display name if set, otherwise the raw key". `??` alone would let
// `display_name: ""` render a blank label.
function descriptorLabel(
  displayName: string | null | undefined,
  fallback: string,
): string {
  return displayName?.trim() ? displayName : fallback;
}

/** The schedule that enqueued a job, from the metadata cron fires record. */
function scheduleOrigin(metadata: unknown): { name: string; manual: boolean } | null {
  if (!metadata || typeof metadata !== "object") return null;
  const fields = metadata as Record<string, unknown>;
  if (typeof fields.cron_name !== "string" || !fields.cron_name) return null;
  return { name: fields.cron_name, manual: fields.triggered_manually === true };
}

function jobKindLabel(job: JobRow): string {
  return descriptorLabel(job.kind_descriptor?.display_name, job.kind);
}

export function JobDetailPage() {
  const { id } = useParams({ strict: false });
  const jobId = Number(id);
  const queryClient = useQueryClient();
  const readOnly = useReadOnly();
  const navigate = useNavigate();
  const [confirmRetryWaiting, setConfirmRetryWaiting] = useState(false);

  const jobQuery = useManagedQuery<JobRow>({
    queryKey: ["job", jobId],
    queryFn: () => fetchJob(jobId),
    enabled: !isNaN(jobId),
  });

  // If this job has been moved to the DLQ, /jobs/:id retry/cancel routes will
  // fail (the row no longer lives in `jobs_hot`). Redirect to the DLQ detail
  // page which has the correct retry/purge actions wired to `/api/dlq/*`.
  useEffect(() => {
    if (jobQuery.data?.dlq) {
      void navigate({
        to: "/dlq/$id",
        params: { id: String(jobId) },
        replace: true,
      });
    }
  }, [jobQuery.data?.dlq, jobId, navigate]);

  const retryMutation = useMutation({
    mutationFn: () => retryJob(jobId),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["job", jobId] });
      void queryClient.invalidateQueries({ queryKey: ["jobs"] });
      toast.success("Job retried");
    },
    onError: () => {
      toast.error("Failed to retry job");
    },
  });

  const cancelMutation = useMutation({
    mutationFn: () => cancelJob(jobId),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["job", jobId] });
      void queryClient.invalidateQueries({ queryKey: ["jobs"] });
      toast.success("Job cancelled");
    },
    onError: () => {
      toast.error("Failed to cancel job");
    },
  });

  if (jobQuery.isLoading) {
    return <p className="text-sm text-muted-fg">Loading...</p>;
  }
  if (jobQuery.data === undefined && jobQuery.error && !isNotFoundError(jobQuery.error)) {
    return (
      <ResourceLoadError
        noun={`job #${id}`}
        error={jobQuery.error}
        onRetry={() => void jobQuery.refetch()}
        backTo="/jobs"
        backLabel="Back to jobs"
      />
    );
  }
  if (!jobQuery.data) {
    return (
      <ResourceNotFound
        title={`Job #${id} not found`}
        reason="No job has this ID. It may have been deleted by retention after it finished, or the link is wrong."
        backTo="/jobs"
        backLabel="Back to jobs"
      />
    );
  }

  const job = jobQuery.data;
  const canRetry = ["failed", "cancelled", "waiting_external"].includes(
    job.state
  );
  const canCancel = !["completed", "failed", "cancelled"].includes(job.state);
  const cronOrigin = scheduleOrigin(job.metadata);

  return (
    <Page width="readable">
      {jobQuery.isRetainedNotFound && <p role="status">Last observed job shown. The latest read reports that this job was not found.</p>}
      <PageHeader
        breadcrumbs={[{ label: SECTION_LABELS["/jobs"], href: "/jobs" }]}
        crumbLabel={`#${job.id}`}
        title={<>Job #{job.id} &mdash; {jobKindLabel(job)}</>}
        status={
          <>
            {job.kind_descriptor?.display_name?.trim() && (
              <code className="rounded bg-muted px-1.5 py-0.5 text-sm">{job.kind}</code>
            )}
            <StateBadge state={job.state} />
          </>
        }
        description={job.kind_descriptor?.description?.trim() || undefined}
      />

      {/* Actions */}
      <div className="flex flex-wrap items-center gap-2">
        {canRetry && (
          <Button
            intent={job.state === "waiting_external" ? "outline" : "primary"}
            size="sm"
            onPress={() =>
              job.state === "waiting_external"
                ? setConfirmRetryWaiting(true)
                : retryMutation.mutate()
            }
            isDisabled={readOnly || retryMutation.isPending}
          >
            {job.state === "waiting_external" ? "Restart…" : "Retry"}
          </Button>
        )}
        {canCancel && (
          <Button
            intent="outline"
            size="sm"
            className="text-danger"
            onPress={() => cancelMutation.mutate()}
            isDisabled={readOnly || cancelMutation.isPending}
          >
            Cancel
          </Button>
        )}
        {job.state === "retryable" && (
          <span className="text-sm text-muted-fg" data-testid="retryable-hint">
            Retries automatically{" "}
            {new Date(job.run_at).getTime() > Date.now() ? <Time value={job.run_at} format="relative" /> : "shortly"}.
            Manual retry is for failed or cancelled jobs.
          </span>
        )}
      </div>

      <ConfirmDialog
        isOpen={confirmRetryWaiting}
        onOpenChange={setConfirmRetryWaiting}
        title={`Restart job #${job.id}?`}
        description="This job is waiting for an external callback. Restarting runs it again from the first attempt and discards the pending callback, so work the handler already started (a payment, an email, a request) may happen twice. To finish it instead, resolve the callback; to stop it, cancel it."
        confirmLabel="Restart job"
        onConfirm={() => retryMutation.mutate()}
        isPending={retryMutation.isPending}
      />

      {/* Properties */}
      <DescriptionList>
        <DescriptionTerm>Queue</DescriptionTerm>
        <DescriptionDetails>
          <QueueLink queue={job.queue} descriptor={job.queue_descriptor} />
          {job.queue_descriptor?.display_name?.trim() && (
            <span className="ml-2 text-xs text-muted-fg">{job.queue}</span>
          )}
        </DescriptionDetails>

        <DescriptionTerm>Kind</DescriptionTerm>
        <DescriptionDetails>
          <KindLink kind={job.kind} descriptor={job.kind_descriptor} />
          {job.kind_descriptor?.display_name?.trim() && (
            <span className="ml-2 text-xs text-muted-fg">{job.kind}</span>
          )}
        </DescriptionDetails>

        {cronOrigin && (
          <>
            <DescriptionTerm>Created by</DescriptionTerm>
            <DescriptionDetails data-testid="job-cron-origin">
              Schedule <ScheduleLink name={cronOrigin.name} />
              {cronOrigin.manual && <span className="text-muted-fg"> (manual trigger)</span>}
            </DescriptionDetails>
          </>
        )}

        <DescriptionTerm>Priority</DescriptionTerm>
        <DescriptionDetails>
          {job.priority}
          {job.original_priority !== job.priority && (
            <span className="ml-1 text-zinc-500">(enqueued as {job.original_priority})</span>
          )}
        </DescriptionDetails>

        <DescriptionTerm>Attempt</DescriptionTerm>
        <DescriptionDetails>
          {job.attempt} / {job.max_attempts}
        </DescriptionDetails>

        <DescriptionTerm>Created</DescriptionTerm>
        <DescriptionDetails>
          <Time value={job.created_at} />
        </DescriptionDetails>

        {job.attempted_at && (
          <>
            <DescriptionTerm>Last attempt</DescriptionTerm>
            <DescriptionDetails>
              <Time value={job.attempted_at} />
            </DescriptionDetails>
          </>
        )}

        {job.finalized_at && (
          <>
            <DescriptionTerm>Finalized</DescriptionTerm>
            <DescriptionDetails>
              <Time value={job.finalized_at} />
            </DescriptionDetails>
          </>
        )}

        {job.tags.length > 0 && (
          <>
            <DescriptionTerm>Tags</DescriptionTerm>
            <DescriptionDetails>
              <div className="flex flex-wrap gap-1">
                {job.tags.map((tag) => (
                  <TagChip key={tag} tag={tag} />
                ))}
              </div>
            </DescriptionDetails>
          </>
        )}

        {job.kind_descriptor?.owner && (
          <>
            <DescriptionTerm>Owner</DescriptionTerm>
            <DescriptionDetails>{job.kind_descriptor.owner}</DescriptionDetails>
          </>
        )}

        {job.kind_descriptor?.docs_url && (
          <>
            <DescriptionTerm>Docs</DescriptionTerm>
            <DescriptionDetails>
              <a
                href={job.kind_descriptor.docs_url}
                target="_blank"
                rel="noreferrer"
                className="text-primary no-underline hover:underline"
              >
                {job.kind_descriptor.docs_url}
              </a>
            </DescriptionDetails>
          </>
        )}
      </DescriptionList>

      {/* Timeline */}
      <Card>
        <CardHeader title="Timeline" />
        <CardContent>
          <JobTimeline job={job} />
        </CardContent>
      </Card>

      {/* Progress */}
      {job.progress != null && (
        <Card>
          <CardHeader title="Progress" />
          <CardContent>
            <ProgressDisplay progress={job.progress} />
          </CardContent>
        </Card>
      )}

      {/* Webhook / Callback */}
      {(job.callback_id || job.callback_filter || job.callback_on_complete || job.callback_on_fail || job.callback_transform) && (
        <Card>
          <CardHeader
            title="Webhook Callback"
            description={
              job.state === "waiting_external"
                ? "Waiting for external system to resolve this callback"
                : "Callback configuration from when this job was in waiting_external state"
            }
          />
          <CardContent>
            <DescriptionList>
              {job.callback_id && (
                <>
                  <DescriptionTerm>Callback ID</DescriptionTerm>
                  <DescriptionDetails>
                    <Copyable value={job.callback_id}>
                      <code className="rounded bg-muted px-1.5 py-0.5 font-mono text-sm">
                        {job.callback_id}
                      </code>
                    </Copyable>
                  </DescriptionDetails>
                </>
              )}

              {job.callback_timeout_at && (
                <>
                  <DescriptionTerm>Timeout</DescriptionTerm>
                  <DescriptionDetails>
                    <Time value={job.callback_timeout_at} />
                    {job.state === "waiting_external" &&
                      new Date(job.callback_timeout_at) < new Date() && (
                        <Badge intent="danger" className="ml-2">
                          Expired
                        </Badge>
                      )}
                  </DescriptionDetails>
                </>
              )}

              {job.callback_filter && (
                <>
                  <DescriptionTerm>Filter (CEL)</DescriptionTerm>
                  <DescriptionDetails>
                    <code className="block rounded bg-muted px-3 py-2 font-mono text-sm">
                      {job.callback_filter}
                    </code>
                    <p className="mt-1 text-xs text-muted-fg">
                      Gate: must return true for the payload to be processed
                    </p>
                  </DescriptionDetails>
                </>
              )}

              {job.callback_on_fail && (
                <>
                  <DescriptionTerm>On Fail (CEL)</DescriptionTerm>
                  <DescriptionDetails>
                    <code className="block rounded bg-danger-subtle px-3 py-2 font-mono text-sm">
                      {job.callback_on_fail}
                    </code>
                    <p className="mt-1 text-xs text-muted-fg">
                      If true, the payload indicates failure (evaluated before on_complete)
                    </p>
                  </DescriptionDetails>
                </>
              )}

              {job.callback_on_complete && (
                <>
                  <DescriptionTerm>On Complete (CEL)</DescriptionTerm>
                  <DescriptionDetails>
                    <code className="block rounded bg-success-subtle px-3 py-2 font-mono text-sm">
                      {job.callback_on_complete}
                    </code>
                    <p className="mt-1 text-xs text-muted-fg">
                      If true, the payload indicates success
                    </p>
                  </DescriptionDetails>
                </>
              )}

              {job.callback_transform && (
                <>
                  <DescriptionTerm>Transform (CEL)</DescriptionTerm>
                  <DescriptionDetails>
                    <code className="block rounded bg-muted px-3 py-2 font-mono text-sm">
                      {job.callback_transform}
                    </code>
                    <p className="mt-1 text-xs text-muted-fg">
                      Reshapes the payload before returning to the caller
                    </p>
                  </DescriptionDetails>
                </>
              )}
            </DescriptionList>
          </CardContent>
        </Card>
      )}

      {/* Arguments */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <span className="font-semibold">Arguments</span>
            <CopyButton
              value={JSON.stringify(job.args, null, 2)}
              label="Copy arguments JSON"
            />
          </div>
        </CardHeader>
        <CardContent>
          <JsonView data={job.args} />
        </CardContent>
      </Card>

      {/* Output (ADR-048) */}
      <OutputCard
        output={job.output}
        errors={job.errors}
        outputType={job.kind_descriptor?.output?.type}
      />

      {/* Metadata */}
      {hasMetadata(job) && (
        <Card>
          <CardHeader>
            <div className="flex items-center justify-between">
              <span className="font-semibold">Metadata</span>
              <CopyButton
                value={JSON.stringify(job.metadata, null, 2)}
                label="Copy metadata JSON"
              />
            </div>
          </CardHeader>
          <CardContent>
            <JsonView data={job.metadata} />
          </CardContent>
        </Card>
      )}

      {/* Errors */}
      {job.errors && job.errors.length > 0 && (
        <Card>
          <CardHeader
            title={`Errors (${job.errors.length})`}
            className="text-danger"
          />
          <CardContent className="space-y-2">
            {[...job.errors].reverse().map((err, i) => (
              <div
                key={i}
                className="rounded-md border-l-[3px] border-danger bg-danger-subtle p-1"
              >
                <JsonView data={err} />
              </div>
            ))}
          </CardContent>
        </Card>
      )}
    </Page>
  );
}
