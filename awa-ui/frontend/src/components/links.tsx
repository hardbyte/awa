import { Link } from "@tanstack/react-router";
import { buildSearch, type SearchFilters } from "@/components/SearchBar";

/** One look for every link to another object in the UI. */
export const entityLinkClass =
  "text-primary no-underline hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-ring rounded-xs";

interface Descriptor {
  display_name?: string | null;
}

function displayName(key: string, descriptor?: Descriptor | null): string {
  return descriptor?.display_name?.trim() ? descriptor.display_name : key;
}

interface EntityLinkProps {
  className?: string;
  children?: React.ReactNode;
}

export function QueueLink({
  queue,
  descriptor,
  className = "",
  children,
  "data-testid": testId,
}: EntityLinkProps & { queue: string; descriptor?: Descriptor | null; "data-testid"?: string }) {
  const label = displayName(queue, descriptor);
  return (
    <Link
      to="/queues/$name"
      params={{ name: queue }}
      className={`${entityLinkClass} ${className}`}
      title={label === queue ? undefined : queue}
      data-testid={testId}
    >
      {children ?? label}
    </Link>
  );
}

export function KindLink({
  kind,
  descriptor,
  className = "",
  children,
}: EntityLinkProps & { kind: string; descriptor?: Descriptor | null }) {
  const label = displayName(kind, descriptor);
  return (
    <Link
      to="/kinds/$kind"
      params={{ kind }}
      className={`${entityLinkClass} ${className}`}
      title={label === kind ? undefined : kind}
    >
      {children ?? label}
    </Link>
  );
}

export function ScheduleLink({ name, className = "", children }: EntityLinkProps & { name: string }) {
  return (
    <Link to="/cron/$name" params={{ name }} className={`${entityLinkClass} ${className}`}>
      {children ?? name}
    </Link>
  );
}

export function JobLink({ id, className = "", children }: EntityLinkProps & { id: number }) {
  return (
    <Link
      to="/jobs/$id"
      params={{ id: String(id) }}
      className={`${entityLinkClass} font-mono ${className}`}
    >
      {children ?? `#${id}`}
    </Link>
  );
}

export function DlqLink({ id, className = "", children }: EntityLinkProps & { id: number }) {
  return (
    <Link
      to="/dlq/$id"
      params={{ id: String(id) }}
      className={`${entityLinkClass} font-mono ${className}`}
    >
      {children ?? `#${id}`}
    </Link>
  );
}

export function InstanceLink({
  instanceId,
  className = "",
  children,
}: EntityLinkProps & { instanceId: string }) {
  return (
    <Link
      to="/runtime/$instanceId"
      params={{ instanceId }}
      className={`${entityLinkClass} ${className}`}
    >
      {children ?? instanceId}
    </Link>
  );
}

export function BatchOpLink({ id, className = "", children }: EntityLinkProps & { id: string }) {
  return (
    <Link
      to="/batch-ops/$id"
      params={{ id }}
      className={`${entityLinkClass} font-mono ${className}`}
    >
      {children ?? id.slice(0, 8)}
    </Link>
  );
}

/** A tag that opens the jobs carrying it. */
export function TagChip({ tag, to = "/jobs", className = "" }: { tag: string; to?: "/jobs" | "/dlq"; className?: string }) {
  return (
    <Link
      to={to}
      search={{ q: buildSearch({ tags: [tag] }) }}
      className={`inline-flex items-center rounded-sm bg-secondary px-1.5 py-px text-xs/5 font-medium text-secondary-fg no-underline hover:bg-muted-fg/20 focus-visible:outline-2 focus-visible:outline-ring ${className}`}
      data-testid="tag-chip"
    >
      {tag}
    </Link>
  );
}

/** A link to the Jobs or Dead letters list with a filter applied. */
export function FilterLink({
  to = "/jobs",
  filter,
  state,
  liveOnly = false,
  className = "",
  children,
  "aria-label": ariaLabel,
  "data-testid": testId,
}: EntityLinkProps & {
  to?: "/jobs" | "/dlq";
  filter: SearchFilters;
  state?: string;
  /** Leave dead letters out of the Jobs list. */
  liveOnly?: boolean;
  "aria-label"?: string;
  "data-testid"?: string;
}) {
  const q = buildSearch(filter);
  const search: Record<string, string | boolean> = {};
  if (state && to === "/jobs") search.state = state;
  if (liveOnly && to === "/jobs") search.dead_lettered = false;
  if (q) search.q = q;
  return (
    <Link to={to} search={search} className={`${entityLinkClass} ${className}`} aria-label={ariaLabel} data-testid={testId}>
      {children}
    </Link>
  );
}
