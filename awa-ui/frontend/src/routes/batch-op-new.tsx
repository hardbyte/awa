import { useEffect, useMemo, useState } from "react";
import { useNavigate, useSearch } from "@tanstack/react-router";
import { useMutation } from "@tanstack/react-query";
import {
  previewBatchOperation,
  type BatchOperationFilter,
  type BatchOperationKind,
  type BatchOperationPayload,
  type BatchOperationPreview,
} from "@/lib/api";
import { BulkActionDialog } from "@/components/BulkActionDialog";
import { AdminButton } from "@/components/AdminAction";
import {
  actsOnDlq,
  buildPayload,
  filterError,
  formatCount,
  formFromSearch,
  isNarrowing,
  isPayloadFormValid,
  KIND_OPTIONS,
  kindOption,
  previewCount,
  sameFilter,
  searchFromForm,
  type BatchForm,
} from "@/lib/batch";
import { toast } from "@/components/ui/toast";
import { Page, PageHeader } from "@/components/Page";
import { SECTION_LABELS } from "@/lib/sections";
import { Input } from "@/components/ui/input";
import { formatNumber } from "@/lib/number";

export function BatchOpNewPage() {
  const navigate = useNavigate();
  const search = useSearch({ strict: false }) as Record<string, unknown>;
  const [form, setForm] = useState<BatchForm>(() => formFromSearch(search));
  // The URL mirrors the form so it survives a reload and can be shared.
  useEffect(() => {
    void navigate({ to: "/batch-ops/new", search: searchFromForm(form), replace: true });
  }, [form, navigate]);
  // A preview holds the operation kind and filter it was taken with: it
  // stands for the form only while both are the same.
  const [preview, setPreview] = useState<{
    opKind: BatchOperationKind;
    filter: BatchOperationFilter;
    result: BatchOperationPreview;
  } | null>(null);
  const [confirming, setConfirming] = useState(false);
  const formFilterError = useMemo(() => filterError(form), [form]);
  const selectedKind = kindOption(form.opKind);
  const payload = useMemo(() => buildPayload(form), [form]);
  const narrowing = isNarrowing(payload.filter);
  const isPayloadValid = isPayloadFormValid(form) && formFilterError === null && narrowing;
  const currentPreview =
    preview && preview.opKind === payload.op_kind && sameFilter(preview.filter, payload.filter)
      ? preview.result
      : null;
  const currentCount = currentPreview ? previewCount(currentPreview) : null;
  const submitHint = !isPayloadValid || currentPreview
    ? null
    : preview
      ? "Operation or filter changed; preview again"
      : "Preview first";

  const previewMutation = useMutation({
    mutationFn: (previewed: BatchOperationPayload) => {
      if (!isPayloadValid) throw new Error("Invalid batch operation payload");
      return previewBatchOperation(previewed);
    },
    onSuccess: (result, previewed) => {
      setPreview({ opKind: previewed.op_kind, filter: previewed.filter, result });
      toast.success(`Preview matched ${formatCount(previewCount(result))} job(s)`);
    },
    onError: (error) => toast.error(error.message),
  });

  return (
    <Page>
      <PageHeader
        breadcrumbs={[{ label: SECTION_LABELS["/batch-ops"], href: "/batch-ops" }]}
        title="New operation"
        description="Preview how many jobs match, then submit. The maintenance leader applies the operation a chunk at a time."
      />

      <section className="rounded-xl border bg-bg p-4 shadow-sm">
        <div className="grid gap-3 md:grid-cols-3">
          <label className="space-y-1 text-sm">
            <span className="font-medium">Operation</span>
            <select
              className="w-full rounded-lg border border-input bg-bg px-3 py-2 text-sm"
              value={form.opKind}
              onChange={(event) => {
                const opKind = event.target.value as BatchOperationKind;
                setForm((current) => ({
                  ...current,
                  opKind,
                  state: kindOption(opKind).states.includes(current.state) ? current.state : "",
                }));
              }}
            >
              {KIND_OPTIONS.map((option) => (
                <option key={option.kind} value={option.kind}>{option.label}</option>
              ))}
            </select>
          </label>
          <label className="space-y-1 text-sm">
            <span className="font-medium">Queue filter</span>
            <Input value={form.queue} onChange={(event) => setForm((current) => ({ ...current, queue: event.target.value }))} placeholder="default" />
          </label>
          <label className="space-y-1 text-sm">
            <span className="font-medium">Kind filter</span>
            <Input value={form.kind} onChange={(event) => setForm((current) => ({ ...current, kind: event.target.value }))} placeholder="send_email" />
          </label>
          <label className="space-y-1 text-sm">
            <span className="font-medium">IDs filter</span>
            <Input value={form.ids} onChange={(event) => setForm((current) => ({ ...current, ids: event.target.value }))} placeholder="123, 456" />
          </label>
          <label className="space-y-1 text-sm">
            <span className="font-medium">Tags filter</span>
            <Input value={form.tags} onChange={(event) => setForm((current) => ({ ...current, tags: event.target.value }))} placeholder="order:123, tenant:acme" />
          </label>
          <label className="space-y-1 text-sm">
            <span className="font-medium">State filter</span>
            <select
              className="w-full rounded-lg border border-input bg-bg px-3 py-2 text-sm"
              value={form.state}
              onChange={(event) => setForm((current) => ({ ...current, state: event.target.value }))}
            >
              <option value="">{selectedKind.scope.charAt(0).toUpperCase() + selectedKind.scope.slice(1)}</option>
              {selectedKind.states.length > 1 &&
                selectedKind.states.map((state) => (
                  <option key={state} value={state}>{state.charAt(0).toUpperCase() + state.slice(1).replace(/_/g, " ")}</option>
                ))}
            </select>
          </label>
          <label className="space-y-1 text-sm">
            <span className="font-medium">Schedule filter</span>
            <Input value={form.cronName} onChange={(event) => setForm((current) => ({ ...current, cronName: event.target.value }))} placeholder="nightly-report" />
          </label>
          {(actsOnDlq(form.opKind) || form.dlqReason) && (
            <label className="space-y-1 text-sm">
              <span className="font-medium">Dead-letter reason filter</span>
              <Input value={form.dlqReason} onChange={(event) => setForm((current) => ({ ...current, dlqReason: event.target.value }))} placeholder="max_attempts_exhausted" />
            </label>
          )}
          <label className="space-y-1 text-sm">
            <span className="font-medium">Created from</span>
            <Input value={form.createdGte} onChange={(event) => setForm((current) => ({ ...current, createdGte: event.target.value }))} placeholder="2026-10-01" />
          </label>
          <label className="space-y-1 text-sm">
            <span className="font-medium">Created before</span>
            <Input value={form.createdLt} onChange={(event) => setForm((current) => ({ ...current, createdLt: event.target.value }))} placeholder="2026-10-02" />
          </label>
          {(form.opKind === "set_priority" || form.opKind === "move_queue") && (
            <label className="space-y-1 text-sm">
              <span className="font-medium">Priority</span>
              <Input value={form.priority} onChange={(event) => setForm((current) => ({ ...current, priority: event.target.value }))} placeholder="1" />
            </label>
          )}
          {form.opKind === "move_to_dlq" && (
            <label className="space-y-1 text-sm">
              <span className="font-medium">DLQ reason</span>
              <Input value={form.reason} onChange={(event) => setForm((current) => ({ ...current, reason: event.target.value }))} placeholder="batch operation" />
            </label>
          )}
          {form.opKind === "move_queue" && (
            <label className="space-y-1 text-sm md:col-span-3">
              <span className="font-medium">Destination queue</span>
              <Input value={form.destinationQueue} onChange={(event) => setForm((current) => ({ ...current, destinationQueue: event.target.value }))} placeholder="escalations" />
            </label>
          )}
        </div>
        <p className="mt-3 text-xs text-muted-fg">
          {selectedKind.label} acts only on {selectedKind.scope} jobs; other jobs
          in the same filter are left alone.
        </p>
        {formFilterError && (
          <p className="mt-2 text-sm text-danger" role="alert">
            {formFilterError}
          </p>
        )}
        {!formFilterError && !narrowing && (
          <p className="mt-2 text-sm text-muted-fg" data-testid="batch-narrowing">
            Add a queue, kind, tag, schedule, ID or created bound.
          </p>
        )}
        {currentCount && (
          <p className="mt-2 text-sm" data-testid="batch-preview-count">
            {currentCount.kind === "at_least" ? "At least " : ""}
            {formatNumber(currentCount.value)} eligible job(s) would change.
          </p>
        )}
        <div className="mt-4 flex items-center gap-2">
          <AdminButton onPress={() => previewMutation.mutate(payload)} isDisabled={previewMutation.isPending || !isPayloadValid}>Preview</AdminButton>
          <AdminButton intent="primary" onPress={() => setConfirming(true)} isDisabled={!isPayloadValid || !currentPreview}>Submit…</AdminButton>
          {submitHint && (
            <span className="text-xs text-muted-fg" data-testid="batch-submit-hint">{submitHint}</span>
          )}
        </div>
      </section>

      {currentPreview && (
        <BulkActionDialog
          isOpen={confirming}
          onOpenChange={setConfirming}
          payload={payload}
          preview={currentPreview}
        />
      )}
    </Page>
  );
}
