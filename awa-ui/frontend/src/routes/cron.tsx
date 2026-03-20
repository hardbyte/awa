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
import {
  Table,
  TableHeader,
  TableBody,
  TableRow,
  TableCell,
  TableColumn,
} from "@/components/ui/table";
import { CronExpr } from "@/components/CronExpr";

function timeAgo(dateStr: string): string {
  const seconds = Math.floor(
    (Date.now() - new Date(dateStr).getTime()) / 1000
  );
  if (seconds < 0) return "just now";
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

export function CronPage() {
  const queryClient = useQueryClient();

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
        <Table aria-label="Cron schedules">
          <TableHeader>
            <TableColumn isRowHeader>Name</TableColumn>
            <TableColumn>Schedule</TableColumn>
            <TableColumn>Kind</TableColumn>
            <TableColumn>Queue</TableColumn>
            <TableColumn>Pri</TableColumn>
            <TableColumn>Last Enqueued</TableColumn>
            <TableColumn>Actions</TableColumn>
          </TableHeader>
          <TableBody>
            {cronJobs.map((cj) => (
              <TableRow key={cj.name} id={cj.name}>
                <TableCell className="font-medium">{cj.name}</TableCell>
                <TableCell>
                  <div className="space-y-0.5">
                    <CronExpr expr={cj.cron_expr} />
                    {cj.timezone !== "UTC" && (
                      <div className="text-xs text-muted-fg">{cj.timezone}</div>
                    )}
                  </div>
                </TableCell>
                <TableCell>{cj.kind}</TableCell>
                <TableCell className="text-muted-fg">{cj.queue}</TableCell>
                <TableCell>
                  {cj.priority !== 2 ? (
                    <Badge
                      intent={cj.priority === 1 ? "danger" : "secondary"}
                      className="text-[10px]"
                    >
                      {cj.priority}
                    </Badge>
                  ) : (
                    <span className="text-muted-fg/40">2</span>
                  )}
                </TableCell>
                <TableCell>
                  {cj.last_enqueued_at ? (
                    <span title={new Date(cj.last_enqueued_at).toLocaleString()}>
                      {timeAgo(cj.last_enqueued_at)}
                    </span>
                  ) : (
                    <span className="text-muted-fg">Never</span>
                  )}
                </TableCell>
                <TableCell>
                  <Button
                    intent="outline"
                    size="xs"
                    onPress={() => triggerMutation.mutate(cj.name)}
                    isDisabled={triggerMutation.isPending}
                  >
                    Trigger now
                  </Button>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      ) : cronQuery.isLoading ? (
        <p className="text-sm text-muted-fg">Loading...</p>
      ) : (
        <p className="text-sm text-muted-fg">No cron schedules found.</p>
      )}
    </div>
  );
}
