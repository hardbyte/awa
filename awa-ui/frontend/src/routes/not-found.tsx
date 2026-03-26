import { Link } from "@tanstack/react-router";
import { Heading } from "@/components/ui/heading";

export function NotFoundPage() {
  return (
    <div className="flex flex-col items-center justify-center py-24 text-center">
      <div className="text-6xl font-bold text-muted-fg/30">404</div>
      <Heading level={2} className="mt-4">
        Page not found
      </Heading>
      <p className="mt-2 text-sm text-muted-fg">
        The page you're looking for doesn't exist or has been moved.
      </p>
      <Link
        to="/"
        className="mt-6 inline-flex items-center gap-2 rounded-lg bg-primary px-4 py-2 text-sm font-medium text-primary-fg transition-colors hover:opacity-80"
      >
        Go to Dashboard
      </Link>
    </div>
  );
}
