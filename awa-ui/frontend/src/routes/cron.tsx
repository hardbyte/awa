import { useState } from "react";
import {
  useQuery,
  useMutation,
  useQueryClient,
} from "@tanstack/react-query";
import { fetchCronJobs, triggerCronJob } from "@/lib/api";
import { toast } from "@/components/ui/toast";
import type { CronJobRow } from "@/lib/api";
import { Heading } from "@/components/ui/heading";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { CronExpr } from "@/components/CronExpr";
import { JsonView } from "@/components/JsonView";
import { timeAgo } from "@/lib/time";

export function CronPage() {
  const queryClient = useQueryClient();
  const [expandedName, setExpandedName] = useState<string | null>(null);

  const cronQuery = useQuery<CronJobRow[]>({
    queryKey: ["cron"],
    queryFn: fetchCronJobs,
  });

  const triggerMutation = useMutation({
    mutationFn: (name: string) => triggerCronJob(name),
    onSuccess: (_data, name) => {
      void queryClient.invalidateQueries({ queryKey: ["jobs"] });
      toast.success(`Cron job "${name}" triggered`);
    },
    onError: () => {
      toast.error("Failed to trigger cron job");
    },
  });

  const cronJobs = cronQuery.data ?? [];

  return (
    <div className="space-y-4">
      <Heading level={2}>Cron Schedules</Heading>

      {cronJobs.length > 0 ? (
        <div className="space-y-3">
          {cronJobs.map((cj) => {
            const isExpanded = expandedName === cj.name;
            const hasArgs =
              cj.args != null &&
              typeof cj.args === "object" &&
              Object.keys(cj.args as Record<string, unknown>).length > 0;
            const hasTags = cj.tags.length > 0;
            const hasMetadata =
              cj.metadata != null &&
              typeof cj.metadata === "object" &&
              Object.keys(cj.metadata as Record<string, unknown>).length > 0;

            return (
              <div
                key={cj.name}
                className="rounded-lg border transition-colors"
              >
                {/* Summary row */}
                <div
                  className="flex flex-wrap items-center gap-3 px-4 py-3 cursor-pointer hover:bg-secondary/30"
                  onClick={() =>
                    setExpandedName(isExpanded ? null : cj.name)
                  }
                >
                  {/* Expand indicator */}
                  <svg
                    className={`size-4 shrink-0 text-muted-fg transition-transform ${
                      isExpanded ? "rotate-90" : ""
                    }`}
                    viewBox="0 0 20 20"
                    fill="currentColor"
                  >
                    <path
                      fillRule="evenodd"
                      d="M7.21 14.77a.75.75 0 0 1 .02-1.06L11.168 10 7.23 6.29a.75.75 0 1 1 1.04-1.08l4.5 4.25a.75.75 0 0 1 0 1.08l-4.5 4.25a.75.75 0 0 1-1.06-.02Z"
                      clipRule="evenodd"
                    />
                  </svg>

                  <span className="font-medium">{cj.name}</span>
                  <CronExpr expr={cj.cron_expr} />
                  {cj.timezone !== "UTC" && (
                    <span className="text-xs text-muted-fg">
                      {cj.timezone}
                    </span>
                  )}

                  <span className="text-sm text-muted-fg">{cj.kind}</span>
                  <span className="text-sm text-muted-fg">&rarr; {cj.queue}</span>

                  {cj.priority !== 2 && (
                    <Badge
                      intent={cj.priority === 1 ? "danger" : "secondary"}
                      className="text-[10px]"
                    >
                      P{cj.priority}
                    </Badge>
                  )}

                  <span className="ml-auto text-sm text-muted-fg">
                    {cj.last_enqueued_at ? (
                      <span title={new Date(cj.last_enqueued_at).toLocaleString()}>
                        {timeAgo(cj.last_enqueued_at)}
                      </span>
                    ) : (
                      "Never run"
                    )}
                  </span>

                  <Button
                    intent="outline"
                    size="xs"
                    onPress={(e) => {
                      e.continuePropagation(); // Don't toggle expand
                      triggerMutation.mutate(cj.name);
                    }}
                    isDisabled={triggerMutation.isPending}
                  >
                    Trigger now
                  </Button>
                </div>

                {/* Expanded detail */}
                {isExpanded && (
                  <div className="border-t px-4 py-3 space-y-3">
                    <div className="grid grid-cols-2 gap-x-8 gap-y-2 text-sm sm:grid-cols-4">
                      <div>
                        <dt className="text-muted-fg">Kind</dt>
                        <dd className="font-medium">{cj.kind}</dd>
                      </div>
                      <div>
                        <dt className="text-muted-fg">Queue</dt>
                        <dd className="font-medium">{cj.queue}</dd>
                      </div>
                      <div>
                        <dt className="text-muted-fg">Priority</dt>
                        <dd className="font-medium">{cj.priority}</dd>
                      </div>
                      <div>
                        <dt className="text-muted-fg">Max attempts</dt>
                        <dd className="font-medium">{cj.max_attempts}</dd>
                      </div>
                    </div>

                    {hasTags && (
                      <div>
                        <dt className="text-sm text-muted-fg mb-1">Tags</dt>
                        <div className="flex flex-wrap gap-1">
                          {cj.tags.map((tag) => (
                            <Badge key={tag} intent="secondary" className="text-[10px]">
                              {tag}
                            </Badge>
                          ))}
                        </div>
                      </div>
                    )}

                    {hasArgs && (
                      <div>
                        <dt className="text-sm text-muted-fg mb-1">Arguments</dt>
                        <JsonView data={cj.args} />
                      </div>
                    )}

                    {hasMetadata && (
                      <div>
                        <dt className="text-sm text-muted-fg mb-1">Metadata</dt>
                        <JsonView data={cj.metadata} />
                      </div>
                    )}

                    <div className="text-xs text-muted-fg">
                      Created {new Date(cj.created_at).toLocaleString()}
                      {" · "}
                      Updated {new Date(cj.updated_at).toLocaleString()}
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      ) : cronQuery.isLoading ? (
        <p className="text-sm text-muted-fg">Loading...</p>
      ) : (
        <p className="text-sm text-muted-fg">No cron schedules found.</p>
      )}
    </div>
  );
}
