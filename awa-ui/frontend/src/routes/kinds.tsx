import { useQuery } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import { fetchKinds } from "@/lib/api";
import type { JobKindOverview } from "@/lib/api";
import { Heading } from "@/components/ui/heading";
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

function kindLabel(kind: JobKindOverview): string {
  // Treat empty display_name the same as missing — the contract is
  // "display name if set, otherwise the raw key". `??` alone would let
  // `display_name: ""` render a blank label.
  return kind.display_name?.trim() ? kind.display_name : kind.kind;
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
    <div className="space-y-4">
      <Heading level={2}>Job Kinds</Heading>

      {kinds.length === 0 ? (
        <div
          className={`rounded-lg border p-6 text-center text-sm sm:hidden ${kindsQuery.isError ? "text-danger-fg" : "text-muted-fg"}`}
        >
          {kindsQuery.isLoading
            ? "Loading job kinds…"
            : kindsQuery.isError
              ? "Failed to load job kinds."
              : "No job kinds found."}
        </div>
      ) : (
        <div className="space-y-3 sm:hidden">
          {kinds.map((kind) => (
            <div key={kind.kind} className="rounded-lg border p-4">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <Link
                    to="/jobs"
                    search={{ q: `kind:${kind.kind}` }}
                    className="font-medium text-primary no-underline hover:underline"
                  >
                    {kindLabel(kind)}
                  </Link>
                  {kind.display_name && (
                    <div className="text-xs text-muted-fg">{kind.kind}</div>
                  )}
                  {descriptorSyncLabel(kind) && (
                    <div className="mt-1 text-xs text-muted-fg">
                      {descriptorSyncLabel(kind)}
                    </div>
                  )}
                </div>
                <div className="flex flex-wrap gap-1">
                  {!kind.descriptor_last_seen_at ? (
                    <Badge intent="secondary">Not declared</Badge>
                  ) : kind.descriptor_stale ? (
                    <Badge intent="warning">Descriptor stale</Badge>
                  ) : null}
                  {kind.descriptor_mismatch && (
                    <Badge intent="danger">Descriptor drift</Badge>
                  )}
                </div>
              </div>

              {kind.description && (
                <p className="mt-2 text-sm text-muted-fg">{kind.description}</p>
              )}

              <div className="mt-3 grid grid-cols-2 gap-x-4 gap-y-1 text-sm">
                <span className="text-muted-fg">Jobs</span>
                <span>{kind.job_count.toLocaleString()}</span>
                <span className="text-muted-fg">Queues</span>
                <span>{kind.queue_count.toLocaleString()}</span>
                <span className="text-muted-fg">Completed/hr</span>
                <span>{kind.completed_last_hour.toLocaleString()}</span>
                {kind.owner && (
                  <>
                    <span className="text-muted-fg">Owner</span>
                    <span>{kind.owner}</span>
                  </>
                )}
              </div>

              {kind.tags.length > 0 && (
                <div className="mt-3 flex flex-wrap gap-1">
                  {kind.tags.map((tag) => (
                    <Badge key={tag} intent="secondary">
                      {tag}
                    </Badge>
                  ))}
                </div>
              )}
            </div>
          ))}
        </div>
      )}

      <Table aria-label="Job kinds" className="hidden sm:table">
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
              className={`p-6 text-center text-sm ${kindsQuery.isError ? "text-danger-fg" : "text-muted-fg"}`}
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
                    <Link
                      to="/jobs"
                      search={{ q: `kind:${kind.kind}` }}
                      className="text-primary no-underline hover:underline"
                    >
                      {kindLabel(kind)}
                    </Link>
                    {kind.display_name?.trim() && (
                      <div className="text-xs font-normal text-muted-fg">{kind.kind}</div>
                    )}
                    {kind.description && (
                      <div className="mt-1 text-xs font-normal text-muted-fg">
                        {kind.description}
                      </div>
                    )}
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
                  <div className="flex flex-wrap gap-1">
                    {!kind.descriptor_last_seen_at ? (
                      <Badge intent="secondary">Not declared</Badge>
                    ) : kind.descriptor_stale ? (
                      <Badge intent="warning">Descriptor stale</Badge>
                    ) : (
                      <Badge intent="success">Live</Badge>
                    )}
                    {kind.descriptor_mismatch && (
                      <Badge intent="danger">Descriptor drift</Badge>
                    )}
                  </div>
                </TableCell>
              </TableRow>
            ))}
        </TableBody>
      </Table>
    </div>
  );
}
