import { useManagedQuery } from "@/hooks/use-managed-query";
import { Time } from "@/components/Time";
import {
  useMutation,
  useQueryClient,
} from "@tanstack/react-query";
import { useParams, useNavigate } from "@tanstack/react-router";
import { useState } from "react";
import { fetchDlqJob, retryDlqJob, purgeDlqJob } from "@/lib/api";
import type { DlqRow } from "@/lib/api";
import { isNotFoundError, ResourceLoadError, ResourceNotFound } from "@/components/ResourceNotFound";
import { toast } from "@/components/ui/toast";
import { Page, PageHeader } from "@/components/Page";
import { KindLink, QueueLink, TagChip } from "@/components/links";
import { SECTION_LABELS } from "@/lib/sections";
import { Badge } from "@/components/ui/badge";
import { Card, CardHeader, CardContent } from "@/components/ui/card";
import {
  DescriptionList,
  DescriptionTerm,
  DescriptionDetails,
} from "@/components/ui/description-list";
import { JsonView } from "@/components/JsonView";
import { OutputCard } from "@/components/OutputCard";
import { ConfirmAction } from "@/components/ConfirmAction";
import { AdminButton } from "@/components/AdminAction";
import { jobStateMeta } from "@/lib/states";

export function DlqDetailPage() {
  const { id } = useParams({ strict: false });
  const jobId = Number(id);
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const [confirming, setConfirming] = useState<"retry" | "purge" | null>(null);
  const [lastConfirming, setLastConfirming] = useState<"retry" | "purge">("retry");
  const confirm = (action: "retry" | "purge") => {
    setLastConfirming(action);
    setConfirming(action);
  };

  const rowQuery = useManagedQuery<DlqRow | null>({
    queryKey: ["dlq-job", jobId],
    queryFn: () => fetchDlqJob(jobId),
    enabled: !isNaN(jobId),
  });

  const retryMutation = useMutation({
    mutationFn: () => retryDlqJob(jobId),
    onSuccess: (revived) => {
      void queryClient.invalidateQueries({ queryKey: ["dlq"] });
      void queryClient.invalidateQueries({ queryKey: ["dlq-depth"] });
      void queryClient.invalidateQueries({ queryKey: ["jobs"] });
      if (revived) {
        toast.success(`Dead letter #${jobId} retried; job now ${jobStateMeta(revived.state).label.toLowerCase()}`);
        void navigate({ to: "/jobs/$id", params: { id: String(revived.id) } });
      } else {
        toast.info(`Dead letter #${jobId} was already gone`);
        void navigate({ to: "/dlq" });
      }
    },
  });

  const purgeMutation = useMutation({
    mutationFn: () => purgeDlqJob(jobId),
    onSuccess: (res) => {
      void queryClient.invalidateQueries({ queryKey: ["dlq"] });
      void queryClient.invalidateQueries({ queryKey: ["dlq-depth"] });
      toast.success(res.count > 0 ? `Dead letter #${jobId} purged` : `Dead letter #${jobId} was already purged`);
      void navigate({ to: "/dlq" });
    },
  });

  if (rowQuery.isLoading) {
    return <p className="p-4 text-sm">Loading…</p>;
  }
  if (rowQuery.data === undefined && rowQuery.error && !isNotFoundError(rowQuery.error)) {
    return (
      <ResourceLoadError
        noun={`dead letter #${jobId}`}
        error={rowQuery.error}
        onRetry={() => void rowQuery.refetch()}
        backTo="/dlq"
        backLabel="Back to dead letters"
      />
    );
  }
  const row = rowQuery.data;
  if (!row) {
    return (
      <ResourceNotFound
        title={`Dead letter #${jobId} not found`}
        reason="This row may have been retried or purged."
        backTo="/dlq"
        backLabel="Back to dead letters"
      />
    );
  }

  return (
    <Page width="readable" className="space-y-4">
      <PageHeader
        breadcrumbs={[{ label: SECTION_LABELS["/dlq"], href: "/dlq" }]}
        crumbLabel={`#${row.id}`}
        title={`Dead letter #${row.id}`}
        actions={
          <div className="flex gap-2">
            <AdminButton size="sm" onPress={() => confirm("retry")} isDisabled={retryMutation.isPending}>
              Retry…
            </AdminButton>
            <AdminButton
              intent="outline"
              size="sm"
              className="text-danger"
              onPress={() => confirm("purge")}
              isDisabled={purgeMutation.isPending}
            >
              Purge…
            </AdminButton>
          </div>
        }
      />

      <Card>
        <CardHeader>
          <div className="flex gap-2">
            <Badge intent="danger">Dead-lettered</Badge>
          </div>
        </CardHeader>
        <CardContent>
          <DescriptionList>
            <DescriptionTerm>Kind</DescriptionTerm>
            <DescriptionDetails>
              <KindLink kind={row.kind} descriptor={row.kind_descriptor} />
            </DescriptionDetails>
            <DescriptionTerm>Queue</DescriptionTerm>
            <DescriptionDetails>
              <QueueLink queue={row.queue} descriptor={row.queue_descriptor} />
            </DescriptionDetails>
            {row.tags.length > 0 && (
              <>
                <DescriptionTerm>Tags</DescriptionTerm>
                <DescriptionDetails>
                  <div className="flex flex-wrap gap-1">
                    {row.tags.map((tag) => <TagChip key={tag} tag={tag} to="/dlq" />)}
                  </div>
                </DescriptionDetails>
              </>
            )}
            <DescriptionTerm>Reason</DescriptionTerm>
            <DescriptionDetails>{row.dlq_reason}</DescriptionDetails>
            <DescriptionTerm>DLQ'd</DescriptionTerm>
            <DescriptionDetails>
              <Time value={row.dlq_at} />
            </DescriptionDetails>
            <DescriptionTerm>Attempts</DescriptionTerm>
            <DescriptionDetails>
              {row.attempt} / {row.max_attempts}
            </DescriptionDetails>
            <DescriptionTerm>Original run lease</DescriptionTerm>
            <DescriptionDetails className="font-mono">
              {row.original_run_lease}
            </DescriptionDetails>
            <DescriptionTerm>Created</DescriptionTerm>
            <DescriptionDetails><Time value={row.created_at} /></DescriptionDetails>
            {row.finalized_at && (
              <>
                <DescriptionTerm>Finalized</DescriptionTerm>
                <DescriptionDetails>
                  <Time value={row.finalized_at} />
                </DescriptionDetails>
              </>
            )}
          </DescriptionList>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>Args</CardHeader>
        <CardContent>
          <JsonView data={row.args} />
        </CardContent>
      </Card>

      {/* The dead-lettered attempt's output, if a failed attempt recorded one (ADR-048) */}
      <OutputCard output={row.output} errors={row.errors} />

      {row.errors && row.errors.length > 0 && (
        <Card>
          <CardHeader>Error history ({row.errors.length})</CardHeader>
          <CardContent>
            <JsonView data={row.errors} />
          </CardContent>
        </Card>
      )}

      {row.progress != null && (
        <Card>
          <CardHeader>Last progress checkpoint</CardHeader>
          <CardContent>
            <JsonView data={row.progress} />
          </CardContent>
        </Card>
      )}

      {lastConfirming === "retry" ? (
        <ConfirmAction
          isOpen={confirming === "retry"}
          onOpenChange={(open) => !open && setConfirming(null)}
          tier={1}
          title={`Retry dead letter #${row.id}?`}
          description={
            <>
              It was dead-lettered with the reason <q className="font-medium text-fg">{row.dlq_reason}</q>. Retrying
              moves this {row.kind} job back to <code>{row.queue}</code> to run again, so its handler's side effects
              happen again. Retry only once whatever that reason describes is fixed.
            </>
          }
          confirmLabel={`Retry dead letter #${row.id}`}
          onConfirm={() => retryMutation.mutateAsync()}
        />
      ) : (
        <ConfirmAction
          isOpen={confirming === "purge"}
          onOpenChange={(open) => !open && setConfirming(null)}
          tier={1}
          intent="danger"
          irreversible
          title={`Purge dead letter #${row.id}?`}
          description={`Deletes this ${row.kind} job and its arguments, errors and output for good.`}
          confirmLabel={`Purge dead letter #${row.id}`}
          onConfirm={() => purgeMutation.mutateAsync()}
        />
      )}
    </Page>
  );
}
