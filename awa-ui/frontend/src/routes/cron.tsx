import {
  useQuery,
  useMutation,
  useQueryClient,
} from "@tanstack/react-query";
import { fetchCronJobs, triggerCronJob } from "@/lib/api";
import { toast } from "@/components/ui/toast";
import type { CronJobRow } from "@/lib/api";
import { Heading } from "@/components/ui/heading";
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
            <TableColumn>Cron</TableColumn>
            <TableColumn>Timezone</TableColumn>
            <TableColumn>Kind</TableColumn>
            <TableColumn>Queue</TableColumn>
            <TableColumn>Last Enqueued</TableColumn>
            <TableColumn>Actions</TableColumn>
          </TableHeader>
          <TableBody>
            {cronJobs.map((cj) => (
              <TableRow key={cj.name} id={cj.name}>
                <TableCell className="font-medium">{cj.name}</TableCell>
                <TableCell>
                  <CronExpr expr={cj.cron_expr} />
                </TableCell>
                <TableCell>{cj.timezone}</TableCell>
                <TableCell>{cj.kind}</TableCell>
                <TableCell>{cj.queue}</TableCell>
                <TableCell>
                  {cj.last_enqueued_at
                    ? new Date(cj.last_enqueued_at).toLocaleString()
                    : "Never"}
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
