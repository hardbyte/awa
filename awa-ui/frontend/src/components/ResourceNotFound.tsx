import { Link } from "@tanstack/react-router";
import { Heading } from "@/components/ui/heading";
import { Button } from "@/components/ui/button";
import { HttpError } from "@/lib/observation/read-control";

type ListRoute = "/jobs" | "/queues" | "/dlq" | "/runtime";

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
    <div className="space-y-4" data-testid="resource-not-found">
      <Link to={backTo} className="text-sm text-muted-fg no-underline hover:text-fg">
        &larr; {backLabel}
      </Link>
      <Heading level={2}>{title}</Heading>
      <p className="text-sm text-muted-fg">{reason}</p>
    </div>
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
    <div className="space-y-4">
      <Link to={backTo} className="text-sm text-muted-fg no-underline hover:text-fg">
        &larr; {backLabel}
      </Link>
      <div className="flex flex-wrap items-center gap-3" role="alert">
        <p className="text-sm text-danger-subtle-fg">
          Failed to load {noun}: {error.message}
        </p>
        <Button size="xs" intent="outline" onPress={onRetry}>
          Try again
        </Button>
      </div>
    </div>
  );
}
