import { useEffect } from "react";
import { useParams, useNavigate } from "@tanstack/react-router";

/**
 * Queue detail redirects to the Jobs page with a locked queue filter.
 * Queue-specific actions (pause/resume/drain) appear in a banner on
 * the Jobs page when a queue: filter is active.
 */
export function QueueDetailPage() {
  const { name } = useParams({ strict: false });
  const queueName = name ?? "";
  const navigate = useNavigate();

  useEffect(() => {
    if (queueName) {
      void navigate({
        to: "/jobs",
        search: { q: `queue:${queueName}` },
        replace: true,
      });
    }
  }, [queueName, navigate]);

  return (
    <div className="py-8 text-center text-sm text-muted-fg">
      Redirecting to jobs for queue &ldquo;{queueName}&rdquo;...
    </div>
  );
}
