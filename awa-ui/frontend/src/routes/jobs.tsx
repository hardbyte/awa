import { useSearch } from "@tanstack/react-router";
import { useManagedQuery } from "@/hooks/use-managed-query";
import { fetchStats, type StateCounts } from "@/lib/api";
import { formatNumber } from "@/lib/number";
import { parseSearch } from "@/components/SearchBar";
import { JobsList } from "@/components/JobsList";
import { QueueLink } from "@/components/links";
import { Page, PageHeader } from "@/components/Page";

export function JobsPage() {
  const search = useSearch({ strict: false }) as Record<string, string | undefined>;
  const queue = parseSearch(search.q ?? "").queue;

  const statsQuery = useManagedQuery<StateCounts>({
    queryKey: ["stats"],
    queryFn: fetchStats,
  });
  const total = statsQuery.data
    ? Object.values(statsQuery.data).reduce((a, b) => a + b, 0)
    : undefined;

  return (
    <Page className="space-y-4">
      <PageHeader
        title="Jobs"
        status={
          total !== undefined && <span className="text-sm text-muted-fg">{formatNumber(total)} total</span>
        }
        actions={
          queue && (
            <QueueLink queue={queue} className="text-sm" data-testid="open-queue">
              Open queue {queue} &rarr;
            </QueueLink>
          )
        }
      />
      <JobsList />
    </Page>
  );
}
