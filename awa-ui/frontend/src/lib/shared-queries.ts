import { fetchBatchOperations, fetchDlqDepth } from "@/lib/api";

/**
 * Managed queries several pages read. Each page must use these exact keys
 * and functions so they share one observation key.
 */
export const dlqDepthQuery = { queryKey: ["dlq-depth"], queryFn: fetchDlqDepth } as const;

export const BATCH_OPS_LIST_LIMIT = 50;
export const batchOpsQuery = {
  queryKey: ["batch-ops"],
  queryFn: () => fetchBatchOperations({ limit: BATCH_OPS_LIST_LIMIT }),
} as const;

/** Operation states that still change jobs or are about to. */
export const ACTIVE_BATCH_STATES = new Set(["pending", "scanning", "running", "cancelling"]);
