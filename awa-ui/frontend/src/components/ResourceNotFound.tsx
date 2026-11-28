import { Link } from "@tanstack/react-router";
import { Button } from "@/components/ui/button";
import { Page, PageHeader } from "@/components/Page";
import { HttpError } from "@/lib/observation/read-control";
import { SECTION_LABELS, type ListRoute } from "@/lib/sections";

export function isNotFoundError(error: unknown): boolean {
  return error instanceof HttpError && error.status === 404;
}

interface ResourceNotFoundProps {
  title: string;
  reason: string;
  backTo: ListRoute;
  backLabel: string;
}

/** Detail page for an id or name that does not (or no longer) exist. */
export function ResourceNotFound({ title, reason, backTo, backLabel }: ResourceNotFoundProps) {
  return (
    <Page width="readable">
      <div className="space-y-4" data-testid="resource-not-found">
        <PageHeader
          title={title}
          crumbLabel="Not found"
          breadcrumbs={[{ label: SECTION_LABELS[backTo], href: backTo }]}
        />
        <p className="text-sm text-muted-fg">{reason}</p>
        <Link to={backTo} className="text-sm text-primary no-underline hover:underline">
          &larr; {backLabel}
        </Link>
      </div>
    </Page>
  );
}

interface ResourceLoadErrorProps {
  /** What failed to load, e.g. "job #42". */
  noun: string;
  error: Error;
  onRetry: () => void;
  backTo: ListRoute;
  backLabel: string;
}

/** Detail page whose first read failed for a reason other than not-found. */
export function ResourceLoadError({ noun, error, onRetry, backTo, backLabel }: ResourceLoadErrorProps) {
  return (
    <Page width="readable">
      <PageHeader
        title={`Could not load ${noun}`}
        crumbLabel="Error"
        breadcrumbs={[{ label: SECTION_LABELS[backTo], href: backTo }]}
      />
      <div className="flex flex-wrap items-center gap-3" role="alert">
        <p className="text-sm text-danger-subtle-fg">
          Failed to load {noun}: {error.message}
        </p>
        <Button size="xs" intent="outline" onPress={onRetry}>
          Try again
        </Button>
      </div>
      <Link to={backTo} className="text-sm text-primary no-underline hover:underline">
        &larr; {backLabel}
      </Link>
    </Page>
  );
}
