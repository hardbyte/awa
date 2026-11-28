import { useParams } from "@tanstack/react-router";
import { useManagedQuery } from "@/hooks/use-managed-query";
import {
  fetchQueue,
  fetchQueueRuntime,
  type QueueOverview,
  type QueueRuntimeSummary,
} from "@/lib/api";

/** The queue a hub page shows, read once and shared by its tabs. */
export function useQueueHub() {
  const { name = "" } = useParams({ strict: false }) as { name?: string };
  const queueQuery = useManagedQuery<QueueOverview>({
    queryKey: ["queue", name],
    queryFn: () => fetchQueue(name),
    enabled: !!name,
  });
  const runtimeQuery = useManagedQuery<QueueRuntimeSummary[]>({
    queryKey: ["queue-runtime"],
    queryFn: fetchQueueRuntime,
    enabled: !!name,
  });
  const runtime = runtimeQuery.data?.find((entry) => entry.queue === name);
  return { name, queueQuery, queue: queueQuery.data, runtimeQuery, runtime };
}
