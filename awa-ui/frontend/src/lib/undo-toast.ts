import { toast } from "@/components/ui/toast";

/** A success toast with an Undo button that runs the inverse action. */
export function toastWithUndo(message: string, undo: () => void) {
  toast.success(message, { action: { label: "Undo", onClick: undo } });
}
