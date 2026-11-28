import type { JobRow } from "@/lib/api";

export interface ActionCopy {
  title: string;
  description: string;
  confirmLabel: string;
  intent: "primary" | "danger";
}

const SIDE_EFFECTS = "Whatever its handler does (send an email, charge a card, call an API) happens again.";

/** What a one-job Retry or Cancel dialog says, by the job's state. */
export function jobActionCopy(job: Pick<JobRow, "id" | "kind" | "state">, action: "retry" | "cancel"): ActionCopy {
  const name = `job #${job.id}`;
  if (action === "retry") {
    if (job.state === "waiting_external") {
      return {
        title: `Restart ${name}?`,
        description: `This ${job.kind} job is waiting for an external callback. Restarting runs it again from the first attempt and discards the pending callback, so work the handler already started may happen twice. To finish it instead, resolve the callback; to stop it, cancel it.`,
        confirmLabel: `Restart ${name}`,
        intent: "primary",
      };
    }
    return {
      title: `Retry ${name}?`,
      description: `This ${job.state} ${job.kind} job runs again. ${SIDE_EFFECTS}`,
      confirmLabel: `Retry ${name}`,
      intent: "primary",
    };
  }
  const effect =
    job.state === "running"
      ? "Its handler is signalled to stop; work it has already done stays done, and a result it reports later is ignored."
      : job.state === "waiting_external"
        ? "The pending callback is discarded; a callback that arrives later is refused."
        : "It won't run.";
  return {
    title: `Cancel ${name}?`,
    description: `${effect} A cancelled job can be retried later.`,
    confirmLabel: `Cancel ${name}`,
    intent: "danger",
  };
}

/** What a Retry or Cancel on selected rows did, for its toast. */
export function selectionOutcome(
  action: "retry" | "cancel",
  selected: Pick<JobRow, "state">[],
  requested: number,
  changed: number
): { message: string; detail?: string } {
  const verb = action === "retry" ? "Retried" : "Cancelled";
  const message = `${verb} ${changed.toLocaleString()} of ${selected.length.toLocaleString()} selected job(s)`;
  const reasons: string[] = [];
  const autoRetrying = action === "retry" ? selected.filter((job) => job.state === "retryable").length : 0;
  const ineligible = selected.length - requested - autoRetrying;
  if (autoRetrying > 0) reasons.push(`${autoRetrying} retryable job(s) retry automatically`);
  if (ineligible > 0) {
    reasons.push(
      action === "retry"
        ? `${ineligible} weren't failed, cancelled or waiting`
        : `${ineligible} had already finished`
    );
  }
  const raced = requested - changed;
  if (raced > 0) reasons.push(`${raced} changed state before the ${action === "retry" ? "retry" : "cancel"} reached them`);
  return reasons.length > 0 ? { message, detail: `Skipped: ${reasons.join("; ")}.` } : { message };
}

/** What a Retry or Cancel dialog says for several selected jobs. */
export function selectionCopy(action: "retry" | "cancel", count: number, waiting: number): ActionCopy {
  const jobs = `${count.toLocaleString()} jobs`;
  if (action === "retry") {
    const callback =
      waiting > 0
        ? ` ${waiting} of them are waiting for an external callback: restarting discards the pending callback, so work their handlers already started may happen twice.`
        : "";
    return {
      title: `Retry ${jobs}?`,
      description: `These jobs run again. Whatever their handlers do (send an email, charge a card, call an API) happens again.${callback}`,
      confirmLabel: `Retry ${jobs}`,
      intent: "primary",
    };
  }
  return {
    title: `Cancel ${jobs}?`,
    description:
      "Jobs that haven't started won't run. Running jobs' handlers are signalled to stop; work already done stays done. Cancelled jobs can be retried later.",
    confirmLabel: `Cancel ${jobs}`,
    intent: "danger",
  };
}
