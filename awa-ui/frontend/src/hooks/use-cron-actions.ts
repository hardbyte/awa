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
import { toastWithUndo } from "@/lib/undo-toast";

interface Target {
  name: string;
  /** Set when this undoes the other action, so its toast offers no Undo. */
  isUndo?: boolean;
}

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
  });

  const pause = useMutation({
    mutationFn: ({ name }: Target) => pauseCronJob(name, "ui"),
    onSuccess: (_data, { name, isUndo }) => {
      void invalidateCron();
      if (isUndo) toast.success(`Schedule "${name}" paused again`);
      else toastWithUndo(`Schedule "${name}" paused`, () => resume.mutate({ name, isUndo: true }));
    },
    onError: (error, { name }) => toast.error(`Couldn't pause schedule "${name}": ${error.message}`),
  });

  const resume = useMutation({
    mutationFn: ({ name }: Target) => resumeCronJob(name),
    onSuccess: (_data, { name, isUndo }) => {
      void invalidateCron();
      if (isUndo) toast.success(`Schedule "${name}" resumed again`);
      else toastWithUndo(`Schedule "${name}" resumed`, () => pause.mutate({ name, isUndo: true }));
    },
    onError: (error, { name }) => toast.error(`Couldn't resume schedule "${name}": ${error.message}`),
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
