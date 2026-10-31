import { useEffect, useRef, useState } from "react";
import { keepPreviousData, useQuery, useQueryClient, type QueryKey } from "@tanstack/react-query";
import { managedQueryId, refreshScheduler } from "@/lib/observation/coordinator";
import { HttpError, readWithFence, withReadSignal } from "@/lib/observation/read-control";
import { CapacityError } from "@/lib/observation/scheduler";

function policy(key: QueryKey) {
  const name = String(key[0]);
  if (!["jobs", "jobs-count", "job", "dlq", "dlq-job", "dlq-depth", "queues", "queue", "queue-runtime", "runtime", "cron", "cron-reconciliation", "batch-ops", "storage", "stats"].includes(name)) {
    throw new Error(`Unmapped managed query: ${name}`);
  }
  return {
    cached: ["queues", "queue-runtime", "runtime", "storage", "stats"].includes(name) && key[1] !== "timeseries",
    manualOnly: name === "jobs-count", stalesCounts: ["jobs", "job", "dlq", "dlq-job"].includes(name), stopAfterNull: name === "storage",
    intervalMs: ["job", "batch-ops"].includes(name) ? 2000 : name === "dlq-job" ? 10_000 : undefined,
  };
}
interface Options<T> { queryKey: QueryKey; queryFn: () => Promise<T>; enabled?: boolean; paused?: boolean; staleTime?: number; retry?: boolean }

export function useManagedQuery<T>(options: Options<T>) {
  const client = useQueryClient();
  const id = managedQueryId(options.queryKey);
  const [stale, setStale] = useState(true);
  const [admissionError, setAdmissionError] = useState<Error | null>(null);
  const paused = useRef(options.paused ?? false); paused.current = options.paused ?? false;
  const attachment = useRef<ReturnType<typeof refreshScheduler.attach> | null>(null);
  const attempt = useRef<(() => void) | null>(null);
  const query = useQuery<T>({ ...options, enabled: false, retry: false, staleTime: Infinity,
    placeholderData: options.enabled !== false && ["jobs", "dlq"].includes(String(options.queryKey[0])) ? keepPreviousData : undefined,
    refetchInterval: false, refetchOnMount: false, refetchOnWindowFocus: false, refetchOnReconnect: false });
  useEffect(() => {
    setAdmissionError(null);
    if (options.enabled === false) return;
    const current = { queryKey: options.queryKey, queryFn: options.queryFn };
    const limits = policy(current.queryKey);
    let disposed = false;
    let clearRejection: (() => void) | null = null;
    const register = () => {
      if (disposed || attachment.current) return;
      try {
        attachment.current = refreshScheduler.attach({ id, ...limits, paused: paused.current, stale: setStale,
          run: async signal => {
            const cancel = () => { void client.cancelQueries({ queryKey: current.queryKey, exact: true }); };
            signal.addEventListener("abort", cancel, { once: true });
            try { return await client.fetchQuery({ queryKey: current.queryKey, retry: false, staleTime: 0,
              queryFn: () => readWithFence(signal, () => withReadSignal(signal, current.queryFn)) }); }
            finally { signal.removeEventListener("abort", cancel); }
          },
        });
        clearRejection?.(); clearRejection = null;
        setAdmissionError(null);
      } catch (error) {
        if (error instanceof CapacityError && !clearRejection) clearRejection = refreshScheduler.recordRejection();
        setAdmissionError(error instanceof Error ? error : new Error("Observation registration failed"));
      }
    };
    const unsubscribe = refreshScheduler.onCapacity(register);
    attempt.current = register; register();
    return () => {
      disposed = true; unsubscribe(); attempt.current = null;
      clearRejection?.();
      attachment.current?.detach(); attachment.current = null;
      queueMicrotask(() => {
        const cached = client.getQueryCache().find({ queryKey: current.queryKey, exact: true });
        if (cached?.getObserversCount() === 0) client.removeQueries({ queryKey: current.queryKey, exact: true });
      });
    };
    // The captured function remains bound to this exact backend/scope/filter/page.
  }, [client, id, options.enabled]);
  useEffect(() => { attachment.current?.setPaused(options.paused ?? false); }, [id, options.paused]);
  return { ...query, data: options.enabled === false ? undefined : query.data, error: admissionError ?? query.error,
    isLoading: options.enabled !== false && query.data === undefined && !query.error && !admissionError,
    isRetainedNotFound: query.data !== undefined && query.error instanceof HttpError && query.error.status === 404,
    isError: !!admissionError || (query.isError && query.data === undefined), isObservationStale: stale || query.isPlaceholderData,
    refetch: () => { attempt.current?.(); refreshScheduler.mark(id, true); return Promise.resolve(query); } };
}
