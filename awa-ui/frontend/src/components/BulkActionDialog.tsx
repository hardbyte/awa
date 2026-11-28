import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "@tanstack/react-router";
import {
  previewBatchOperation,
  submitBatchOperation,
  type BatchOperationPayload,
  type BatchOperationPreview,
} from "@/lib/api";
import {
  actsOnDlq,
  bulkConfirmLabel,
  bulkConfirmText,
  describeOperation,
  operationLabel,
  previewCount,
} from "@/lib/batch";
import { bulkTier } from "@/lib/confirm";
import { BULK_SAMPLE_ROWS } from "@/lib/constants";
import { ConfirmAction } from "@/components/ConfirmAction";
import { DlqLink, JobLink, KindLink, QueueLink } from "@/components/links";
import { StateBadge } from "@/components/StateBadge";
import { toast } from "@/components/ui/toast";

const IRREVERSIBLE = new Set(["discard", "purge_dlq"]);

/**
 * Previews a bulk operation over a filter, shows its exact count, its scope
 * in words and some of the jobs it would change, then submits it and opens
 * the operation's page. Discards, purges and very large cancels ask the user
 * to type the count.
 */
export function BulkActionDialog({
  isOpen,
  onOpenChange,
  payload,
  preview: givenPreview,
}: {
  isOpen: boolean;
  onOpenChange: (open: boolean) => void;
  payload: BatchOperationPayload;
  /** A preview the caller already holds for exactly this payload. */
  preview?: BatchOperationPreview;
}) {
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const previewQuery = useQuery({
    queryKey: ["bulk-preview", payload],
    queryFn: () => previewBatchOperation(payload),
    enabled: isOpen && !givenPreview,
    staleTime: 0,
    gcTime: 0,
    retry: false,
    refetchOnWindowFocus: false,
  });
  const preview = givenPreview ?? previewQuery.data;
  const count = preview ? previewCount(preview) : undefined;
  const tier = bulkTier(payload.op_kind, count);
  const dlq = actsOnDlq(payload.op_kind);
  const label = operationLabel(payload.op_kind);
  const sample = (preview?.sample ?? []).slice(0, BULK_SAMPLE_ROWS);
  const nothing = count?.kind === "exact" && count.value === 0;

  async function submit() {
    const operation = await submitBatchOperation(payload);
    void queryClient.invalidateQueries({ queryKey: ["batch-ops"] });
    toast.success(`Submitted ${label.toLowerCase()} operation ${operation.id.slice(0, 8)}`);
    void navigate({ to: "/batch-ops/$id", params: { id: operation.id } });
  }

  return (
    <ConfirmAction
      isOpen={isOpen}
      onOpenChange={onOpenChange}
      tier={tier}
      intent={IRREVERSIBLE.has(payload.op_kind) || payload.op_kind === "cancel" ? "danger" : "primary"}
      irreversible={IRREVERSIBLE.has(payload.op_kind)}
      title={count ? `${bulkConfirmLabel(payload.op_kind, count)}?` : `${label}: counting the matching ${dlq ? "dead letters" : "jobs"}…`}
      description={
        preview
          ? `${describeOperation(payload, preview)} It runs as a bulk operation that the maintenance leader applies a chunk at a time; you can follow and cancel it on its page.`
          : "Counting the jobs this would change…"
      }
      confirmLabel={count ? bulkConfirmLabel(payload.op_kind, count) : label}
      confirmText={count && tier === 3 ? bulkConfirmText(count) : undefined}
      confirmTextLabel={dlq ? "the number of dead letters" : "the number of jobs"}
      isConfirmDisabled={!preview || nothing}
      onConfirm={submit}
    >
      {previewQuery.isError && (
        <p role="alert" className="rounded-md bg-danger-subtle px-3 py-2 text-danger-subtle-fg" data-testid="bulk-preview-error">
          {previewQuery.error.message}
        </p>
      )}
      {nothing && <p data-testid="bulk-nothing">Nothing matches, so there is nothing to submit.</p>}
      {sample.length > 0 && (
        <div className="space-y-1">
          <p className="text-muted-fg">
            {count && count.value > sample.length ? `${sample.length} of them:` : "They are:"}
          </p>
          <ul className="divide-y rounded-md border" aria-label="Sample of matching jobs" data-testid="bulk-sample">
            {sample.map((job) => (
              <li key={job.id} className="flex items-center gap-3 px-3 py-1.5">
                {dlq ? <DlqLink id={job.id} /> : <JobLink id={job.id} />}
                <KindLink kind={job.kind} descriptor={job.kind_descriptor} className="min-w-0 truncate" />
                <QueueLink queue={job.queue} descriptor={job.queue_descriptor} className="min-w-0 truncate text-muted-fg" />
                <span className="ml-auto">
                  <StateBadge state={job.state} />
                </span>
              </li>
            ))}
          </ul>
        </div>
      )}
    </ConfirmAction>
  );
}
