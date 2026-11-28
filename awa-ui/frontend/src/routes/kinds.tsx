import { useQuery } from "@tanstack/react-query";
import { FilterLink, KindLink } from "@/components/links";
import { fetchKinds } from "@/lib/api";
import type { JobKindOverview } from "@/lib/api";
import { Page, PageHeader } from "@/components/Page";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableColumn,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { usePollInterval } from "@/hooks/use-poll-interval";
import { timeAgo } from "@/lib/time";
import { JsonView } from "@/components/JsonView";
import { RowCards } from "@/components/RowCards";
import { formatNumber, plural } from "@/lib/number";

// The output type the kind declares, with its JSON Schema behind a toggle
// when the runtime published one.
function OutputType({ kind }: { kind: JobKindOverview }) {
  if (!kind.output) return null;
  return (
    <div className="mt-1 text-xs font-normal text-muted-fg">
      Output <code className="rounded bg-muted px-1 py-0.5">{kind.output.type}</code>
      {kind.output.schema !== undefined && kind.output.schema !== null && (
        <details className="mt-1">
          <summary className="cursor-pointer">Schema</summary>
          <div className="mt-1">
            <JsonView data={kind.output.schema} />
          </div>
        </details>
      )}
    </div>
  );
}

// Only surfaces a line when the descriptor needs attention — drift, stale,
// or not declared. A fresh descriptor is implied by the row's presence.
function descriptorSyncLabel(kind: JobKindOverview): string | null {
  if (kind.descriptor_mismatch) return "Descriptor drift across live runtimes";
  if (!kind.descriptor_last_seen_at) return "Descriptor not declared";
  if (kind.descriptor_stale) {
    return `Descriptor stale · seen ${timeAgo(kind.descriptor_last_seen_at)}`;
  }
  return null;
}

/** Badges for the exceptions only: an undeclared, stale or drifting descriptor. */
function KindStatusBadges({ kind }: { kind: JobKindOverview }) {
  return (
    <div className="flex flex-wrap gap-1">
      {!kind.descriptor_last_seen_at ? (
        <Badge intent="secondary">Not declared</Badge>
      ) : kind.descriptor_stale ? (
        <Badge intent="secondary">Descriptor stale</Badge>
      ) : null}
      {kind.descriptor_mismatch && <Badge intent="warning">Descriptor drift</Badge>}
    </div>
  );
}

export function KindsPage() {
  const poll = usePollInterval();
  const kindsQuery = useQuery<JobKindOverview[]>({
    queryKey: ["kinds"],
    queryFn: fetchKinds,
    refetchInterval: poll.interval,
    staleTime: poll.staleTime,
  });

  const kinds = kindsQuery.data ?? [];

  return (
    <Page className="space-y-4">
      <PageHeader title="Job kinds" />

      <RowCards
        label="Job kinds"
        placeholder={
          kindsQuery.isLoading ? (
            "Loading job kinds…"
          ) : kindsQuery.isError ? (
            <span className="text-danger-subtle-fg">Failed to load job kinds.</span>
          ) : (
            "No job kinds found."
          )
        }
        items={kinds.map((kind) => ({
          key: kind.kind,
          title: kind.display_name?.trim() ? kind.display_name : kind.kind,
          link: { to: "/kinds/$kind", params: { kind: kind.kind } },
          status: <KindStatusBadges kind={kind} />,
          why: descriptorSyncLabel(kind) ?? undefined,
          meta: (
            <>
              <span>{plural(kind.job_count, "job")}</span>
              <span>{plural(kind.queue_count, "queue")}</span>
              <span>{formatNumber(kind.completed_last_hour)} completed/hr</span>
              {kind.owner && <span>{kind.owner}</span>}
            </>
          ),
        }))}
      />

      <Table aria-label="Job kinds" className="hidden sm:block">
        <TableHeader>
          <TableColumn isRowHeader>Kind</TableColumn>
          <TableColumn className="text-right">Jobs</TableColumn>
          <TableColumn className="text-right">Queues</TableColumn>
          <TableColumn className="text-right">Completed/hr</TableColumn>
          <TableColumn>Owner</TableColumn>
          <TableColumn>Status</TableColumn>
        </TableHeader>
        <TableBody
          renderEmptyState={() => (
            <div
              className={`p-6 text-center text-sm ${kindsQuery.isError ? "text-danger-subtle-fg" : "text-muted-fg"}`}
            >
              {kindsQuery.isLoading
                ? "Loading job kinds…"
                : kindsQuery.isError
                  ? "Failed to load job kinds."
                  : "No job kinds found."}
            </div>
          )}
        >
          {kinds.map((kind) => (
              <TableRow key={kind.kind} id={kind.kind}>
                <TableCell className="font-medium">
                  <div>
                    <KindLink kind={kind.kind} descriptor={kind} />
                    <FilterLink filter={{ kind: kind.kind }} className="ml-2 text-xs font-normal" aria-label={`Jobs of kind ${kind.kind}`}>
                      jobs
                    </FilterLink>
                    {kind.description && (
                      <div className="mt-1 text-xs font-normal text-muted-fg">
                        {kind.description}
                      </div>
                    )}
                    <OutputType kind={kind} />
                    {descriptorSyncLabel(kind) && (
                      <div className="mt-1 text-xs font-normal text-muted-fg">
                        {descriptorSyncLabel(kind)}
                      </div>
                    )}
                  </div>
                </TableCell>
                <TableCell className="text-right tabular-nums">
                  {kind.job_count.toLocaleString()}
                </TableCell>
                <TableCell className="text-right tabular-nums">
                  {kind.queue_count.toLocaleString()}
                </TableCell>
                <TableCell className="text-right tabular-nums">
                  {kind.completed_last_hour.toLocaleString()}
                </TableCell>
                <TableCell>{kind.owner ?? "—"}</TableCell>
                <TableCell>
                  <KindStatusBadges kind={kind} />
                </TableCell>
              </TableRow>
            ))}
        </TableBody>
      </Table>
    </Page>
  );
}
