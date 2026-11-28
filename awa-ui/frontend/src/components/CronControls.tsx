import { useState } from "react";
import type { CronJobRow } from "@/lib/api";
import type { useCronActions } from "@/hooks/use-cron-actions";
import { AdminButton } from "@/components/AdminAction";
import { ConfirmAction } from "@/components/ConfirmAction";
import { toast } from "@/components/ui/toast";

const RETIRED_REASON = "Retired schedules can't be paused or triggered; restore it first";

/** Pause or Resume (with Undo) and Trigger now for one schedule. Triggering a
 * paused schedule asks first, since pausing it was meant to stop its runs. */
export function CronControls({
  cron,
  actions,
  size,
}: {
  cron: CronJobRow;
  actions: ReturnType<typeof useCronActions>;
  size: "xs" | "sm";
}) {
  const [confirmTrigger, setConfirmTrigger] = useState(false);
  const isPaused = cron.paused_at != null;
  const isRetired = cron.retired_at != null;
  const toggling = actions.pause.isPending || actions.resume.isPending;
  const retiredReason = isRetired ? RETIRED_REASON : undefined;

  return (
    <>
      {isPaused ? (
        <AdminButton
          intent="outline"
          size={size}
          onPress={() => actions.resume.mutate({ name: cron.name })}
          isDisabled={toggling || isRetired}
          disabledReason={retiredReason}
        >
          Resume
        </AdminButton>
      ) : (
        <AdminButton
          intent="outline"
          size={size}
          onPress={() => actions.pause.mutate({ name: cron.name })}
          isDisabled={toggling || isRetired}
          disabledReason={retiredReason}
        >
          Pause
        </AdminButton>
      )}
      <AdminButton
        intent="outline"
        size={size}
        onPress={() =>
          isPaused
            ? setConfirmTrigger(true)
            : actions.trigger.mutate(cron.name, {
                onError: (error) => toast.error(`Couldn't trigger schedule "${cron.name}": ${error.message}`),
              })
        }
        isDisabled={actions.trigger.isPending || isRetired}
        disabledReason={retiredReason}
      >
        Trigger now
      </AdminButton>
      <ConfirmAction
        isOpen={confirmTrigger}
        onOpenChange={setConfirmTrigger}
        tier={1}
        title={`Trigger paused schedule "${cron.name}"?`}
        description={
          <>
            This schedule is paused{cron.paused_by ? ` by ${cron.paused_by}` : ""}. Triggering it enqueues one{" "}
            <code>{cron.kind}</code> job in <code>{cron.queue}</code> now, which runs like any other: its side effects
            happen. The schedule stays paused.
          </>
        }
        confirmLabel={`Trigger "${cron.name}"`}
        onConfirm={() => actions.trigger.mutateAsync(cron.name)}
      />
    </>
  );
}
