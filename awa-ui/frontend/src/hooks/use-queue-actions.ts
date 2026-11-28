import { useMutation, useQueryClient } from "@tanstack/react-query";
import { drainQueue, pauseQueue, resumeQueue } from "@/lib/api";
import { toastWithUndo } from "@/lib/undo-toast";
import { toast } from "@/components/ui/toast";

interface Target {
  queue: string;
  /** Set when this undoes the other action, so its toast offers no Undo. */
  isUndo?: boolean;
}

/** Pause, resume and drain for queues, shared by the queue list and hub. */
export function useQueueActions() {
  const queryClient = useQueryClient();
  const invalidate = (queue: string) => {
    void queryClient.invalidateQueries({ queryKey: ["queue", queue] });
    void queryClient.invalidateQueries({ queryKey: ["queues"] });
  };

  const pause = useMutation({
    mutationFn: ({ queue }: Target) => pauseQueue(queue, "ui"),
    onSuccess: (_data, { queue, isUndo }) => {
      invalidate(queue);
      if (isUndo) toast.success(`Queue "${queue}" paused again`);
      else toastWithUndo(`Queue "${queue}" paused`, () => resume.mutate({ queue, isUndo: true }));
    },
    onError: (error, { queue }) => toast.error(`Couldn't pause queue "${queue}": ${error.message}`),
  });

  const resume = useMutation({
    mutationFn: ({ queue }: Target) => resumeQueue(queue),
    onSuccess: (_data, { queue, isUndo }) => {
      invalidate(queue);
      if (isUndo) toast.success(`Queue "${queue}" resumed again`);
      else toastWithUndo(`Queue "${queue}" resumed`, () => pause.mutate({ queue, isUndo: true }));
    },
    onError: (error, { queue }) => toast.error(`Couldn't resume queue "${queue}": ${error.message}`),
  });

  // Errors show in the confirmation dialog that runs it.
  const drain = useMutation({
    mutationFn: (queue: string) => drainQueue(queue),
    onSuccess: (result, queue) => {
      invalidate(queue);
      void queryClient.invalidateQueries({ queryKey: ["jobs"] });
      void queryClient.invalidateQueries({ queryKey: ["jobs-count"] });
      toast.success(`Drained queue "${queue}": ${result.drained.toLocaleString()} job(s) cancelled`);
    },
  });

  return { pause, resume, drain };
}
