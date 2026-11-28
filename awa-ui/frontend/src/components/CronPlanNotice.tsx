import type { CronActionPlan } from "@/lib/api";
import { Button } from "@/components/ui/button";

/** A previewed ownership change with its Apply and Cancel buttons. */
export function CronPlanNotice({
  plan,
  onApply,
  onCancel,
  isDisabled,
}: {
  plan: CronActionPlan;
  onApply: () => void;
  onCancel: () => void;
  isDisabled: boolean;
}) {
  return (
    <div role="status" className="space-y-2 rounded-lg border p-4">
      <p>
        Apply {plan.action.action.replace(/_/g, " ")} to: {plan.schedules.join(", ") || "no schedules"}?
      </p>
      <p className="text-sm text-muted-fg">
        Retirement stops future scheduled runs. Restore starts from now. Already enqueued jobs continue.
      </p>
      <div className="flex gap-2">
        <Button isDisabled={isDisabled} onPress={onApply}>Apply</Button>
        <Button intent="outline" onPress={onCancel}>Cancel</Button>
      </div>
    </div>
  );
}
