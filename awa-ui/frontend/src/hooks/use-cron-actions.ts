import { useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "@tanstack/react-router";
import {
  cronOwnerAction,
  pauseCronJob,
  resumeCronJob,
  triggerCronJob,
  type CronActionPlan,
  type CronOwnerAction,
} from "@/lib/api";
import { toast } from "@/components/ui/toast";

/** Pause, resume, trigger and ownership changes for schedules, shared by
 * the schedule list and a schedule's page. */
export function useCronActions() {
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const invalidateCron = () => queryClient.invalidateQueries({ queryKey: ["cron"] });

  const trigger = useMutation({
    mutationFn: (name: string) => triggerCronJob(name),
    onSuccess: (job, name) => {
      void queryClient.invalidateQueries({ queryKey: ["jobs"] });
      void invalidateCron();
      toast.success(`Triggered "${name}": job #${job.id}`, {
        action: {
          label: "Open job",
          onClick: () => void navigate({ to: "/jobs/$id", params: { id: String(job.id) } }),
        },
      });
    },
    onError: (error) => toast.error(`Failed to trigger: ${error.message}`),
  });

  const pause = useMutation({
    mutationFn: (name: string) => pauseCronJob(name, "ui"),
    onSuccess: (_data, name) => {
      void invalidateCron();
      toast.success(`Paused "${name}"`);
    },
    onError: (error) => toast.error(`Failed to pause: ${error.message}`),
  });

  const resume = useMutation({
    mutationFn: (name: string) => resumeCronJob(name),
    onSuccess: (_data, name) => {
      void invalidateCron();
      toast.success(`Resumed "${name}"`);
    },
    onError: (error) => toast.error(`Failed to resume: ${error.message}`),
  });

  // An owner action is previewed first; the plan it returns is applied on
  // confirmation.
  const [plan, setPlan] = useState<CronActionPlan | null>(null);
  const owner = useMutation({
    mutationFn: ({ action, apply }: { action: CronOwnerAction; apply: boolean }) => cronOwnerAction(action, apply),
    onSuccess: (result) => {
      if (result.applied) {
        setPlan(null);
        void invalidateCron();
        void queryClient.invalidateQueries({ queryKey: ["cron-reconciliation"] });
        toast.success("Schedule operation applied");
      } else {
        setPlan(result);
      }
    },
    onError: (error: Error) => toast.error(error.message),
  });

  return {
    trigger,
    pause,
    resume,
    owner,
    plan,
    clearPlan: () => setPlan(null),
    previewOwner: (action: CronOwnerAction) => owner.mutate({ action, apply: false }),
    applyPlan: () => {
      if (plan) owner.mutate({ action: plan.action, apply: true });
    },
  };
}
