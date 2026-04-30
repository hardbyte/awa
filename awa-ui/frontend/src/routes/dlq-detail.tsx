import {
  useQuery,
  useMutation,
  useQueryClient,
} from "@tanstack/react-query";
import { useParams, useNavigate, Link } from "@tanstack/react-router";
import { useState } from "react";
import { fetchDlqJob, retryDlqJob, purgeDlqJob } from "@/lib/api";
import type { DlqRow } from "@/lib/api";
import { useReadOnly } from "@/hooks/use-read-only";
import { toast } from "@/components/ui/toast";
import { Heading } from "@/components/ui/heading";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardHeader, CardContent } from "@/components/ui/card";
import {
  DescriptionList,
  DescriptionTerm,
  DescriptionDetails,
} from "@/components/ui/description-list";
import { JsonView } from "@/components/JsonView";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { timeAgo } from "@/lib/time";
import { POLL } from "@/lib/constants";

export function DlqDetailPage() {
  const { id } = useParams({ strict: false });
  const jobId = Number(id);
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const readOnly = useReadOnly();
  const [showPurge, setShowPurge] = useState(false);

  const rowQuery = useQuery<DlqRow | null>({
    queryKey: ["dlq-job", jobId],
    queryFn: () => fetchDlqJob(jobId),
    enabled: !isNaN(jobId),
    refetchInterval: POLL.SLOW,
  });

  const retryMutation = useMutation({
    mutationFn: () => retryDlqJob(jobId),
    onSuccess: (revived) => {
      void queryClient.invalidateQueries({ queryKey: ["dlq"] });
      void queryClient.invalidateQueries({ queryKey: ["dlq-depth"] });
      void queryClient.invalidateQueries({ queryKey: ["jobs"] });
      if (revived) {
        toast.success(`Retried — job now ${revived.state}`);
        void navigate({ to: "/jobs/$id", params: { id: String(revived.id) } });
      } else {
        toast.info("DLQ row was already gone");
        void navigate({ to: "/dlq" });
      }
    },
    onError: () => toast.error("Failed to retry from DLQ"),
  });

  const purgeMutation = useMutation({
    mutationFn: () => purgeDlqJob(jobId),
    onSuccess: (res) => {
      void queryClient.invalidateQueries({ queryKey: ["dlq"] });
      void queryClient.invalidateQueries({ queryKey: ["dlq-depth"] });
      toast.success(res.count > 0 ? "DLQ row purged" : "Already purged");
      void navigate({ to: "/dlq" });
    },
    onError: () => toast.error("Failed to purge DLQ row"),
  });

  if (rowQuery.isLoading) {
    return <p className="p-4 text-sm">Loading…</p>;
  }
  if (rowQuery.error) {
    return (
      <p className="p-4 text-sm text-danger">
        Error: {String(rowQuery.error)}
      </p>
    );
  }
  const row = rowQuery.data;
  if (!row) {
    return (
      <div className="p-4 space-y-3">
        <Heading>DLQ row #{jobId}</Heading>
        <p className="text-muted-fg">
          Not found — this row may have been retried or purged.
        </p>
        <Link to="/dlq" className="underline">
          Back to DLQ
        </Link>
      </div>
    );
  }

  return (
    <div className="space-y-4 p-4">
      <div className="flex items-center justify-between gap-2">
        <Heading>
          <Link to="/dlq" className="text-muted-fg hover:underline">
            DLQ
          </Link>{" "}
          / #{row.id}
        </Heading>
        {!readOnly && (
          <div className="flex gap-2">
            <Button
              size="xs"
              onPress={() => retryMutation.mutate()}
              isDisabled={retryMutation.isPending}
            >
              Retry
            </Button>
            <Button
              intent="outline"
              size="xs"
              className="text-danger"
              onPress={() => setShowPurge(true)}
              isDisabled={purgeMutation.isPending}
            >
              Purge
            </Button>
          </div>
        )}
      </div>

      <Card>
        <CardHeader>
          <div className="flex gap-2">
            <Badge intent="danger">dlq</Badge>
            <Badge intent="secondary">{row.kind}</Badge>
            <Badge intent="secondary">{row.queue}</Badge>
          </div>
        </CardHeader>
        <CardContent>
          <DescriptionList>
            <DescriptionTerm>Reason</DescriptionTerm>
            <DescriptionDetails>{row.dlq_reason}</DescriptionDetails>
            <DescriptionTerm>DLQ'd</DescriptionTerm>
            <DescriptionDetails>
              {new Date(row.dlq_at).toISOString()} ({timeAgo(row.dlq_at)})
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
            <DescriptionDetails>{timeAgo(row.created_at)}</DescriptionDetails>
            {row.finalized_at && (
              <>
                <DescriptionTerm>Finalized</DescriptionTerm>
                <DescriptionDetails>
                  {timeAgo(row.finalized_at)}
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

      <ConfirmDialog
        isOpen={showPurge}
        onOpenChange={setShowPurge}
        title="Purge DLQ row"
        description={`Permanently delete DLQ row #${row.id}.`}
        confirmLabel="Purge"
        confirmIntent="danger"
        onConfirm={() => purgeMutation.mutate()}
      />
    </div>
  );
}
