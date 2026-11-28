import { useNavigate, useSearch } from "@tanstack/react-router";
import { useManagedQuery } from "@/hooks/use-managed-query";
import { dlqDepthQuery } from "@/lib/shared-queries";
import { formatNumber } from "@/lib/number";
import { buildSearch, parseSearch } from "@/components/SearchBar";
import { DlqList } from "@/components/DlqList";
import { Page, PageHeader } from "@/components/Page";
import { Badge } from "@/components/ui/badge";

export function DlqPage() {
  const depthQuery = useManagedQuery(dlqDepthQuery);
  const navigate = useNavigate();
  const search = useSearch({ strict: false }) as Record<string, string | undefined>;
  const filters = parseSearch(search.q ?? "");

  // A queue chip narrows the list to that queue, or widens it again.
  const toggleQueue = (queue: string) => {
    const q = buildSearch({ ...filters, queue: filters.queue === queue ? undefined : queue });
    void navigate({ to: "/dlq", search: q ? { q } : {}, replace: true });
  };

  return (
    <Page className="space-y-4">
      <PageHeader
        title="Dead letters"
        status={depthQuery.data && (
          <div className="flex flex-wrap gap-2 items-center">
            <Badge intent="warning">
              {formatNumber(depthQuery.data.total)} total
            </Badge>
            {depthQuery.data.by_queue.slice(0, 5).map((q) => {
              const active = filters.queue === q.queue;
              return (
                <button
                  key={q.queue}
                  type="button"
                  aria-pressed={active}
                  data-testid="dlq-queue-chip"
                  onClick={() => toggleQueue(q.queue)}
                  className={[
                    "rounded-full border px-2 py-px text-xs font-medium transition-colors",
                    active
                      ? "border-primary bg-primary text-primary-fg"
                      : "border-border bg-secondary text-secondary-fg hover:bg-muted",
                  ].join(" ")}
                >
                  {q.queue}: {formatNumber(q.count)}
                </button>
              );
            })}
          </div>
        )}
      />

      <DlqList />
    </Page>
  );
}
