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

/** A page of operations older than `cursor`, the last row of the page before. */
export function olderBatchOpsQuery(cursor: { before_submitted_at: string; before_id: string }) {
  return {
    queryKey: ["batch-ops", cursor],
    queryFn: () => fetchBatchOperations({ limit: BATCH_OPS_LIST_LIMIT, ...cursor }),
  } as const;
}

/** Operation states that still change jobs or are about to. */
export const ACTIVE_BATCH_STATES = new Set(["pending", "scanning", "running", "cancelling"]);
