/** List pages a detail page belongs to, and the name each goes by. */
export type ListRoute = "/jobs" | "/queues" | "/dlq" | "/runtime" | "/kinds" | "/cron" | "/batch-ops";

export const SECTION_LABELS: Record<ListRoute, string> = {
  "/jobs": "Jobs",
  "/queues": "Queues",
  "/dlq": "Dead letters",
  "/runtime": "Fleet",
  "/kinds": "Job kinds",
  "/cron": "Schedules",
  "/batch-ops": "Bulk operations",
};
