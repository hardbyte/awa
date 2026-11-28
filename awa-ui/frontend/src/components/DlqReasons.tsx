import { Link } from "@tanstack/react-router";
import { useManagedQuery } from "@/hooks/use-managed-query";
import { fetchDlqReasons, type DlqReasonGroup } from "@/lib/api";
import { buildSearch, type SearchFilters } from "@/components/SearchBar";
import { formatNumber } from "@/lib/number";
import { Time } from "@/components/Time";

/** Groups shown above the dead-letter list. */
export const DLQ_REASON_GROUPS = 5;

/** The search a reason group links to: the current search, narrowed to the
 * group's reason, queue and kind. */
export function reasonSearch(filters: SearchFilters, group: DlqReasonGroup): string {
  return buildSearch({ ...filters, queue: group.queue, kind: group.kind, reason: group.reason });
}

/**
 * The commonest reasons dead letters matching the search's queue and kind
 * failed for, each linking to the list filtered to it. Hidden while the DLQ
 * is empty, and on a server without `/api/dlq/reasons`.
 */
export function DlqReasons({ filters }: { filters: SearchFilters }) {
  const scope = { queue: filters.queue, kind: filters.kind, limit: DLQ_REASON_GROUPS };
  const reasonsQuery = useManagedQuery<DlqReasonGroup[] | null>({
    queryKey: ["dlq-reasons", scope],
    queryFn: () => fetchDlqReasons(scope),
  });
  const groups = reasonsQuery.data;
  if (!groups || groups.length === 0) return null;
  return (
    <section aria-label="Top reasons" className="rounded-lg border px-3 py-2" data-testid="dlq-reasons">
      <h2 className="text-sm font-medium">Top reasons</h2>
      <ul className="mt-1 divide-y text-sm">
        {groups.map((group) => {
          // The router marks the link for the current search with aria-current.
          return (
            <li
              key={`${group.reason}\u0000${group.queue}\u0000${group.kind}`}
              className="flex flex-wrap items-baseline gap-x-3 gap-y-0.5 py-1"
            >
              <Link
                to="/dlq"
                search={{ q: reasonSearch(filters, group) }}
                className="min-w-0 max-w-full truncate font-medium text-primary no-underline hover:underline sm:max-w-[24rem]"
                title={group.reason}
                data-testid="dlq-reason-link"
              >
                {group.reason}
              </Link>
              <span className="min-w-0 truncate text-muted-fg">
                {group.kind} · {group.queue}
              </span>
              <span className="ml-auto tabular-nums" data-testid="dlq-reason-count">
                {formatNumber(group.count)}
              </span>
              <span className="text-xs text-muted-fg">
                latest <Time value={group.newest} />
              </span>
            </li>
          );
        })}
      </ul>
    </section>
  );
}
