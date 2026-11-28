import { Button } from "@/components/ui/button";

interface ListStateProps {
  isLoading: boolean;
  error: Error | null;
  onRetry?: () => void;
  /** What failed to load, e.g. "batch operations". */
  noun: string;
  empty: React.ReactNode;
  className?: string;
}

/**
 * The loading, error and empty states of a list, kept distinct so a failed
 * fetch never reads as "nothing here".
 */
export function ListState({ isLoading, error, onRetry, noun, empty, className = "" }: ListStateProps) {
  if (isLoading) {
    return (
      <div className={`p-6 text-center text-sm text-muted-fg ${className}`} role="status">
        Loading {noun}…
      </div>
    );
  }
  if (error) {
    return (
      <div className={`flex flex-col items-center gap-3 p-6 text-center text-sm ${className}`} role="alert">
        <p className="text-danger-subtle-fg">
          Failed to load {noun}: {error.message}
        </p>
        {onRetry && (
          <Button size="xs" intent="outline" onPress={onRetry}>
            Try again
          </Button>
        )}
      </div>
    );
  }
  return <div className={`p-6 text-center text-sm text-muted-fg ${className}`}>{empty}</div>;
}
