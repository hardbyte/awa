import { useState } from "react";
import { Outlet, useRouterState } from "@tanstack/react-router";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useQueueHub } from "@/hooks/use-queue-hub";
import { useReadOnly } from "@/hooks/use-read-only";
import { drainQueue, pauseQueue, resumeQueue } from "@/lib/api";
import { SECTION_LABELS } from "@/lib/sections";
import { formatNumber } from "@/lib/number";
import { Page, PageHeader } from "@/components/Page";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { JobsList } from "@/components/JobsList";
import { DlqList } from "@/components/DlqList";
import { isNotFoundError, ResourceLoadError, ResourceNotFound } from "@/components/ResourceNotFound";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Tab, TabList, TabPanel, TabPanels, Tabs } from "@/components/ui/tabs";
import { toast } from "@/components/ui/toast";

const TABS = [
  { id: "overview", label: "Overview", suffix: "" },
  { id: "jobs", label: "Jobs", suffix: "/jobs" },
  { id: "dlq", label: "Dead letters", suffix: "/dlq" },
  { id: "config", label: "Config", suffix: "/config" },
] as const;

/** Header, actions and tabs shared by every view of one queue. */
export function QueueHubLayout() {
  const { name, queueQuery, queue, runtime } = useQueueHub();
  const pathname = useRouterState({ select: (state) => state.location.pathname });
  const queryClient = useQueryClient();
  const readOnly = useReadOnly();
  const [showDrainConfirm, setShowDrainConfirm] = useState(false);

  const invalidateQueue = () => {
    void queryClient.invalidateQueries({ queryKey: ["queue", name] });
    void queryClient.invalidateQueries({ queryKey: ["queues"] });
  };
  const pauseMutation = useMutation({
    mutationFn: () => pauseQueue(name, "ui"),
    onSuccess: () => {
      invalidateQueue();
      toast.success(`Queue "${name}" paused`);
    },
    onError: (error) => toast.error(`Failed to pause queue: ${error.message}`),
  });
  const resumeMutation = useMutation({
    mutationFn: () => resumeQueue(name),
    onSuccess: () => {
      invalidateQueue();
      toast.success(`Queue "${name}" resumed`);
    },
    onError: (error) => toast.error(`Failed to resume queue: ${error.message}`),
  });
  const drainMutation = useMutation({
    mutationFn: () => drainQueue(name),
    onSuccess: (result) => {
      setShowDrainConfirm(false);
      invalidateQueue();
      void queryClient.invalidateQueries({ queryKey: ["jobs"] });
      toast.success(`Queue "${name}" drained: ${formatNumber(result.drained)} job(s) cancelled`);
    },
    onError: (error) => toast.error(`Failed to drain queue: ${error.message}`),
  });

  const notFound = (
    <ResourceNotFound
      title={`Queue "${name}" not found`}
      reason="No jobs or configuration exist for this queue name."
      backTo="/queues"
      backLabel="Back to queues"
    />
  );
  if (!name) return notFound;
  if (queueQuery.isLoading) return <p className="text-sm text-muted-fg">Loading…</p>;
  if (!queue) {
    if (queueQuery.error && !isNotFoundError(queueQuery.error)) {
      return (
        <ResourceLoadError
          noun={`queue "${name}"`}
          error={queueQuery.error}
          onRetry={() => void queueQuery.refetch()}
          backTo="/queues"
          backLabel="Back to queues"
        />
      );
    }
    return notFound;
  }

  const title = queue.display_name?.trim() ? queue.display_name : queue.queue;
  const base = `/queues/${encodeURIComponent(name)}`;
  const segments = pathname.replace(/\/+$/, "").split("/").filter(Boolean);
  const selected =
    (segments.length === 3 && TABS.find((tab) => tab.suffix === `/${segments[2]}`)?.id) || "overview";

  return (
    <Page className="space-y-4">
      <PageHeader
        breadcrumbs={[{ label: SECTION_LABELS["/queues"], href: "/queues" }]}
        title={title}
        status={
          <>
            {title !== queue.queue && (
              <code className="rounded bg-muted px-1.5 py-0.5 text-sm">{queue.queue}</code>
            )}
            {queue.paused && <Badge intent="warning" data-testid="queue-paused">Paused</Badge>}
            {runtime?.config?.dlq_enabled === true && <Badge intent="secondary">DLQ on</Badge>}
            {queue.descriptor_stale && <Badge intent="warning">Descriptor stale</Badge>}
            {queue.descriptor_mismatch && <Badge intent="danger">Descriptor drift</Badge>}
            {runtime?.config_mismatch && <Badge intent="warning">Config mismatch</Badge>}
          </>
        }
        actions={
          <>
            {queue.paused ? (
              <Button intent="outline" size="sm" onPress={() => resumeMutation.mutate()} isDisabled={readOnly || resumeMutation.isPending}>
                Resume
              </Button>
            ) : (
              <Button intent="outline" size="sm" onPress={() => pauseMutation.mutate()} isDisabled={readOnly || pauseMutation.isPending}>
                Pause
              </Button>
            )}
            <Button
              intent="outline"
              size="sm"
              className="text-danger"
              onPress={() => setShowDrainConfirm(true)}
              isDisabled={readOnly || drainMutation.isPending}
            >
              Drain
            </Button>
          </>
        }
        description={queue.description || undefined}
      />

      <Tabs selectedKey={selected}>
        <TabList aria-label={`${title} views`}>
          {TABS.map((tab) => (
            <Tab key={tab.id} id={tab.id} href={`${base}${tab.suffix}`}>
              {tab.label}
            </Tab>
          ))}
        </TabList>
        <TabPanels>
          {TABS.map((tab) => (
            <TabPanel key={tab.id} id={tab.id} className="pt-2">
              <Outlet />
            </TabPanel>
          ))}
        </TabPanels>
      </Tabs>

      <ConfirmDialog
        isOpen={showDrainConfirm}
        onOpenChange={setShowDrainConfirm}
        onConfirm={() => drainMutation.mutate()}
        title={`Drain queue "${queue.queue}"`}
        description={`Cancel all available, scheduled, retryable and waiting jobs in "${queue.queue}". Running jobs are not affected.`}
        confirmLabel="Drain queue"
        confirmIntent="danger"
        isPending={drainMutation.isPending}
      />
    </Page>
  );
}

export function QueueJobsTab() {
  const { name, queue } = useQueueHub();
  return (
    <JobsList
      lockedFilter={{ queue: name }}
      stateCounts={
        queue && {
          available: queue.available,
          running: queue.running,
          scheduled: queue.scheduled,
          retryable: queue.retryable,
          failed: queue.failed,
          waiting_external: queue.waiting_external,
        }
      }
    />
  );
}

export function QueueDlqTab() {
  const { name } = useQueueHub();
  return <DlqList lockedFilter={{ queue: name }} />;
}
