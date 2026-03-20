import {
  useQuery,
  useMutation,
  useQueryClient,
} from "@tanstack/react-query";
import { useParams, Link } from "@tanstack/react-router";
import { fetchJob, retryJob, cancelJob } from "@/lib/api";
import { toast } from "@/components/ui/toast";
import type { JobRow } from "@/lib/api";
import { StateBadge } from "@/components/StateBadge";
import { Heading } from "@/components/ui/heading";
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

function hasMetadata(job: JobRow): boolean {
  return (
    job.metadata != null &&
    typeof job.metadata === "object" &&
    Object.keys(job.metadata as Record<string, unknown>).length > 0
  );
}

export function JobDetailPage() {
  const { id } = useParams({ strict: false });
  const jobId = Number(id);
  const queryClient = useQueryClient();

  const jobQuery = useQuery<JobRow>({
    queryKey: ["job", jobId],
    queryFn: () => fetchJob(jobId),
    enabled: !isNaN(jobId),
  });

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
  if (jobQuery.error) {
    return (
      <p className="text-sm text-danger">Error: {String(jobQuery.error)}</p>
    );
  }
  if (!jobQuery.data) {
    return <p className="text-sm text-muted-fg">Job not found.</p>;
  }

  const job = jobQuery.data;
  const canRetry = ["failed", "cancelled", "waiting_external"].includes(
    job.state
  );
  const canCancel = !["completed", "failed", "cancelled"].includes(job.state);

  return (
    <div className="space-y-6">
      {/* Back link */}
      <Link
        to="/jobs"
        className="text-sm text-muted-fg no-underline hover:text-fg"
      >
        &larr; Back to jobs
      </Link>

      <div className="flex flex-wrap items-center gap-4">
        <Heading level={2}>
          Job #{job.id} &mdash; {job.kind}
        </Heading>
        <StateBadge state={job.state} />
      </div>

      {/* Actions */}
      <div className="flex gap-2">
        {canRetry && (
          <Button
            intent="primary"
            size="sm"
            onPress={() => retryMutation.mutate()}
            isDisabled={retryMutation.isPending}
          >
            Retry
          </Button>
        )}
        {canCancel && (
          <Button
            intent="outline"
            size="sm"
            className="text-danger"
            onPress={() => cancelMutation.mutate()}
            isDisabled={cancelMutation.isPending}
          >
            Cancel
          </Button>
        )}
      </div>

      {/* Properties */}
      <DescriptionList>
        <DescriptionTerm>Queue</DescriptionTerm>
        <DescriptionDetails>
          <Link
            to="/queues/$name"
            params={{ name: job.queue }}
            className="text-primary no-underline hover:underline"
          >
            {job.queue}
          </Link>
        </DescriptionDetails>

        <DescriptionTerm>Priority</DescriptionTerm>
        <DescriptionDetails>{job.priority}</DescriptionDetails>

        <DescriptionTerm>Attempt</DescriptionTerm>
        <DescriptionDetails>
          {job.attempt} / {job.max_attempts}
        </DescriptionDetails>

        <DescriptionTerm>Created</DescriptionTerm>
        <DescriptionDetails>
          {new Date(job.created_at).toLocaleString()}
        </DescriptionDetails>

        {job.attempted_at && (
          <>
            <DescriptionTerm>Last attempt</DescriptionTerm>
            <DescriptionDetails>
              {new Date(job.attempted_at).toLocaleString()}
            </DescriptionDetails>
          </>
        )}

        {job.finalized_at && (
          <>
            <DescriptionTerm>Finalized</DescriptionTerm>
            <DescriptionDetails>
              {new Date(job.finalized_at).toLocaleString()}
            </DescriptionDetails>
          </>
        )}

        {job.tags.length > 0 && (
          <>
            <DescriptionTerm>Tags</DescriptionTerm>
            <DescriptionDetails>
              <div className="flex flex-wrap gap-1">
                {job.tags.map((tag) => (
                  <Badge key={tag} intent="secondary">
                    {tag}
                  </Badge>
                ))}
              </div>
            </DescriptionDetails>
          </>
        )}
      </DescriptionList>

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
                    {new Date(job.callback_timeout_at).toLocaleString()}
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
    </div>
  );
}
