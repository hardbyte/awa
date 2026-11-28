import { useState } from "react";
import { useParams } from "@tanstack/react-router";
import { useManagedQuery } from "@/hooks/use-managed-query";
import { useNow } from "@/hooks/use-now";
import { fetchBatchOperation, type BatchOperation } from "@/lib/api";
import {
  actsOnDlq,
  batchOutcome,
  describeFilter,
  describeSpec,
  filterToSearch,
  isBatchOperationId,
  isCancellable,
  KIND_OPTIONS,
  operationLabel,
} from "@/lib/batch";
import { durationBetween } from "@/lib/time";
import { formatNumber } from "@/lib/number";
import { SECTION_LABELS } from "@/lib/sections";
import { HttpError } from "@/lib/observation/read-control";
import { Page, PageHeader } from "@/components/Page";
import { BatchStateBadge } from "@/components/StateBadge";
import { FilterLink, InstanceLink } from "@/components/links";
import { Time } from "@/components/Time";
import { CancelOperationDialog } from "@/components/CancelOperationDialog";
import { AdminButton } from "@/components/AdminAction";
import { CopyButton } from "@/components/CopyButton";
import { isNotFoundError, ResourceLoadError, ResourceNotFound } from "@/components/ResourceNotFound";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import {
  DescriptionDetails,
  DescriptionList,
  DescriptionTerm,
} from "@/components/ui/description-list";
import { ProgressBar, ProgressBarHeader, ProgressBarTrack, ProgressBarValue } from "@/components/ui/progress-bar";
import { Label } from "@/components/ui/field";

/** The API answers an unknown or malformed id with 400 rather than 404. */
function isMissingOperation(error: unknown): boolean {
  if (isNotFoundError(error)) return true;
  return error instanceof HttpError && error.status === 400 && /not found|uuid|invalid url/i.test(error.message);
}

function Outcome({ operation }: { operation: BatchOperation }) {
  const eligible =
    operation.total_matched === null ? null : Math.max(0, operation.total_matched - operation.skipped);
  const done = operation.processed + operation.errored;
  const nothingChanged = operation.processed === 0 && operation.skipped > 0;
  return (
    <div className="space-y-3">
      <p
        className={`text-base font-medium tabular-nums ${nothingChanged ? "text-warning-subtle-fg" : ""}`}
        data-testid="batch-op-outcome"
      >
        {batchOutcome(operation)}
      </p>
      {nothingChanged && (
        <p className="text-sm text-warning-subtle-fg" data-testid="batch-op-changed-nothing">
          Every matching job was skipped: none was in a state this operation acts on when it was reached.
        </p>
      )}
      {eligible !== null && eligible > 0 && (
        <ProgressBar
          value={Math.min(done, eligible)}
          maxValue={eligible}
          valueLabel={`${formatNumber(done)} of ${formatNumber(eligible)}`}
          aria-label="Eligible jobs handled"
        >
          <ProgressBarHeader>
            <Label>Eligible jobs handled</Label>
            <ProgressBarValue />
          </ProgressBarHeader>
          <ProgressBarTrack />
        </ProgressBar>
      )}
      <dl className="grid grid-cols-2 gap-x-6 gap-y-1 text-sm sm:grid-cols-4">
        <div>
          <dt className="text-muted-fg">Matched</dt>
          <dd className="tabular-nums">
            {operation.total_matched !== null
              ? formatNumber(operation.total_matched)
              : operation.finalized_at
                ? "—"
                : "Counting…"}
          </dd>
        </div>
        <div>
          <dt className="text-muted-fg">Changed</dt>
          <dd className="tabular-nums">{formatNumber(operation.processed)}</dd>
        </div>
        <div>
          <dt className="text-muted-fg">Skipped</dt>
          <dd className="tabular-nums">{formatNumber(operation.skipped)}</dd>
        </div>
        <div>
          <dt className="text-muted-fg">Errored</dt>
          <dd className={`tabular-nums ${operation.errored > 0 ? "text-danger-subtle-fg" : ""}`}>
            {formatNumber(operation.errored)}
          </dd>
        </div>
      </dl>
    </div>
  );
}

export function BatchOpDetailPage() {
  const { id = "" } = useParams({ strict: false }) as { id?: string };
  const validId = isBatchOperationId(id);
  const now = useNow();
  const [confirmCancel, setConfirmCancel] = useState(false);

  const opQuery = useManagedQuery<BatchOperation>({
    queryKey: ["batch-op", id],
    queryFn: () => fetchBatchOperation(id),
    enabled: validId,
  });
  const operation = opQuery.data;

  const notFound = (
    <ResourceNotFound
      title={`Operation ${id.slice(0, 8)} not found`}
      reason="No batch operation has this id. Finished operations are deleted after their retention period."
      backTo="/batch-ops"
      backLabel="Back to bulk operations"
    />
  );
  if (!validId) return notFound;
  if (opQuery.isLoading) return <p className="text-sm text-muted-fg">Loading…</p>;
  if (!operation) {
    if (opQuery.error && !isMissingOperation(opQuery.error)) {
      return (
        <ResourceLoadError
          noun={`operation ${id.slice(0, 8)}`}
          error={opQuery.error}
          onRetry={() => void opQuery.refetch()}
          backTo="/batch-ops"
          backLabel="Back to bulk operations"
        />
      );
    }
    return notFound;
  }

  const option = KIND_OPTIONS.find((candidate) => candidate.kind === operation.op_kind);
  const filterWords = describeFilter(operation.filter, option?.scope);
  const scope =
    operation.total_matched === null
      ? filterWords.charAt(0).toUpperCase() + filterWords.slice(1)
      : `${formatNumber(operation.total_matched)} ${filterWords}`;
  const matching = filterToSearch(operation.filter, operation.op_kind);
  const spec = describeSpec(operation.op_kind, operation.spec);
  const active = !operation.finalized_at;

  return (
    <Page width="readable">
      <PageHeader
        breadcrumbs={[{ label: SECTION_LABELS["/batch-ops"], href: "/batch-ops" }]}
        crumbLabel={operation.id.slice(0, 8)}
        title={operationLabel(operation.op_kind)}
        status={<BatchStateBadge state={operation.state} tone={operation.state === "completed" && operation.processed === 0 && operation.skipped > 0 ? "retryable" : undefined} />}
        actions={
          isCancellable(operation.state) && (
            <AdminButton intent="outline" size="sm" className="text-danger" onPress={() => setConfirmCancel(true)}>
              Cancel operation…
            </AdminButton>
          )
        }
        description={
          <span className="inline-flex items-center gap-1 font-mono text-xs">
            {operation.id}
            <CopyButton value={operation.id} label="Copy operation id" />
          </span>
        }
      />

      <Card>
        <CardHeader title="Scope" />
        <CardContent className="space-y-2">
          <p className="text-sm" data-testid="batch-op-scope">
            {scope}.
          </p>
          {spec && <p className="text-sm" data-testid="batch-op-spec">{spec}</p>}
          <FilterLink
            to={actsOnDlq(operation.op_kind) ? "/dlq" : "/jobs"}
            filter={matching.filter}
            state={matching.state}
            liveOnly={matching.liveOnly}
            className="text-sm"
          >
            {actsOnDlq(operation.op_kind)
              ? "Open matching dead letters"
              : matching.wider
                ? `Open matching jobs in every state, not only ${matching.wider}`
                : "Open matching jobs"}
          </FilterLink>
        </CardContent>
      </Card>

      <Card>
        <CardHeader title="Progress" />
        <CardContent className="space-y-3">
          <Outcome operation={operation} />
          {operation.last_error && (
            <div
              className="rounded-md border border-danger/40 bg-danger-subtle px-3 py-2 text-sm text-danger-subtle-fg"
              role="alert"
              data-testid="batch-op-last-error"
            >
              <span className="font-medium">Last error: </span>
              <span className="break-words">{operation.last_error}</span>
            </div>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader title="Details" />
        <CardContent>
          <DescriptionList>
            <DescriptionTerm>Submitted by</DescriptionTerm>
            <DescriptionDetails>{operation.submitted_by ?? "—"}</DescriptionDetails>
            <DescriptionTerm>Submitted</DescriptionTerm>
            <DescriptionDetails><Time value={operation.submitted_at} /></DescriptionDetails>
            <DescriptionTerm>Started</DescriptionTerm>
            <DescriptionDetails><Time value={operation.started_at} fallback="Not started" /></DescriptionDetails>
            <DescriptionTerm>Finalized</DescriptionTerm>
            <DescriptionDetails><Time value={operation.finalized_at} fallback="—" /></DescriptionDetails>
            <DescriptionTerm>Duration</DescriptionTerm>
            <DescriptionDetails>
              {operation.started_at
                ? `${durationBetween(operation.started_at, operation.finalized_at ?? new Date(now).toISOString())}${active ? " so far" : ""}`
                : "—"}
            </DescriptionDetails>
            <DescriptionTerm>Runner</DescriptionTerm>
            <DescriptionDetails>
              {operation.runner_instance ? (
                <InstanceLink instanceId={operation.runner_instance} className="font-mono text-sm" />
              ) : (
                "—"
              )}
            </DescriptionDetails>
            <DescriptionTerm>Kept until</DescriptionTerm>
            <DescriptionDetails><Time value={operation.retention_until} fallback="—" /></DescriptionDetails>
          </DescriptionList>
        </CardContent>
      </Card>

      <CancelOperationDialog operation={operation} isOpen={confirmCancel} onOpenChange={setConfirmCancel} />
    </Page>
  );
}
