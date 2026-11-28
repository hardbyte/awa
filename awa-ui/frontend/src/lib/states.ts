import type { ComponentType, SVGProps } from "react";
import {
  ArrowPathIcon,
  CheckCircleIcon,
  ClockIcon,
  InboxStackIcon,
  NoSymbolIcon,
  PlayCircleIcon,
  XCircleIcon,
} from "@heroicons/react/16/solid";
import { HourglassIcon } from "@/components/HourglassIcon";

/** A colour family from app.css (`--color-state-<tone>`). */
export type StateTone =
  | "available"
  | "running"
  | "completed"
  | "retryable"
  | "failed"
  | "cancelled"
  | "scheduled"
  | "waiting";

export interface StateMeta {
  label: string;
  /** For tight spaces such as filter pills. */
  shortLabel: string;
  Icon: ComponentType<SVGProps<SVGSVGElement>>;
  tone: StateTone;
}

/** Job states in the order lists and tiles show them. */
export const JOB_STATES = [
  "available",
  "running",
  "scheduled",
  "retryable",
  "waiting_external",
  "failed",
  "completed",
  "cancelled",
] as const;

const JOB_STATE_META: Record<string, StateMeta> = {
  available: { label: "Available", shortLabel: "Available", Icon: InboxStackIcon, tone: "available" },
  running: { label: "Running", shortLabel: "Running", Icon: PlayCircleIcon, tone: "running" },
  scheduled: { label: "Scheduled", shortLabel: "Scheduled", Icon: ClockIcon, tone: "scheduled" },
  retryable: { label: "Retryable", shortLabel: "Retryable", Icon: ArrowPathIcon, tone: "retryable" },
  waiting_external: { label: "Waiting for callback", shortLabel: "Waiting", Icon: HourglassIcon, tone: "waiting" },
  failed: { label: "Failed", shortLabel: "Failed", Icon: XCircleIcon, tone: "failed" },
  completed: { label: "Completed", shortLabel: "Completed", Icon: CheckCircleIcon, tone: "completed" },
  cancelled: { label: "Cancelled", shortLabel: "Cancelled", Icon: NoSymbolIcon, tone: "cancelled" },
};

function sentenceCase(token: string): string {
  const text = token.replace(/_/g, " ");
  return text.charAt(0).toUpperCase() + text.slice(1);
}

/** Label, icon and tone for a job state; unknown states read as neutral. */
export function jobStateMeta(state: string): StateMeta {
  return (
    JOB_STATE_META[state] ?? {
      label: sentenceCase(state),
      shortLabel: sentenceCase(state),
      Icon: ClockIcon,
      tone: "scheduled",
    }
  );
}

const BATCH_STATE_META: Record<string, StateMeta> = {
  pending: { label: "Pending", shortLabel: "Pending", Icon: ClockIcon, tone: "scheduled" },
  scanning: { label: "Scanning", shortLabel: "Scanning", Icon: PlayCircleIcon, tone: "running" },
  running: { label: "Running", shortLabel: "Running", Icon: PlayCircleIcon, tone: "running" },
  cancelling: { label: "Cancelling", shortLabel: "Cancelling", Icon: NoSymbolIcon, tone: "cancelled" },
  completed: { label: "Completed", shortLabel: "Completed", Icon: CheckCircleIcon, tone: "completed" },
  cancelled: { label: "Cancelled", shortLabel: "Cancelled", Icon: NoSymbolIcon, tone: "cancelled" },
  failed: { label: "Failed", shortLabel: "Failed", Icon: XCircleIcon, tone: "failed" },
};

/** Label, icon and tone for a batch operation state. */
export function batchStateMeta(state: string): StateMeta {
  return BATCH_STATE_META[state] ?? jobStateMeta(state);
}

/** Badge colours per tone; spelled out so the class names are static. */
export const TONE_BADGE: Record<StateTone, string> = {
  available: "[--badge-bg:var(--color-state-available-subtle)] [--badge-fg:var(--color-state-available-fg)]",
  running: "[--badge-bg:var(--color-state-running-subtle)] [--badge-fg:var(--color-state-running-fg)]",
  completed: "[--badge-bg:var(--color-state-completed-subtle)] [--badge-fg:var(--color-state-completed-fg)]",
  retryable: "[--badge-bg:var(--color-state-retryable-subtle)] [--badge-fg:var(--color-state-retryable-fg)]",
  failed: "[--badge-bg:var(--color-state-failed-subtle)] [--badge-fg:var(--color-state-failed-fg)]",
  cancelled: "[--badge-bg:var(--color-state-cancelled-subtle)] [--badge-fg:var(--color-state-cancelled-fg)]",
  scheduled: "[--badge-bg:var(--color-state-scheduled-subtle)] [--badge-fg:var(--color-state-scheduled-fg)]",
  waiting: "[--badge-bg:var(--color-state-waiting-subtle)] [--badge-fg:var(--color-state-waiting-fg)]",
};

/** Solid dot per tone, for timelines. */
export const TONE_DOT: Record<StateTone, string> = {
  available: "bg-state-available",
  running: "bg-state-running",
  completed: "bg-state-completed",
  retryable: "bg-state-retryable",
  failed: "bg-state-failed",
  cancelled: "bg-state-cancelled",
  scheduled: "bg-state-scheduled",
  waiting: "bg-state-waiting",
};

/** Tinted surface per tone, for count tiles. */
export const TONE_SURFACE: Record<StateTone, string> = {
  available: "bg-state-available-subtle",
  running: "bg-state-running-subtle",
  completed: "bg-state-completed-subtle",
  retryable: "bg-state-retryable-subtle",
  failed: "bg-state-failed-subtle",
  cancelled: "bg-state-cancelled-subtle",
  scheduled: "bg-state-scheduled-subtle",
  waiting: "bg-state-waiting-subtle",
};

/** Text colour per tone. */
export const TONE_TEXT: Record<StateTone, string> = {
  available: "text-state-available-fg",
  running: "text-state-running-fg",
  completed: "text-state-completed-fg",
  retryable: "text-state-retryable-fg",
  failed: "text-state-failed-fg",
  cancelled: "text-state-cancelled-fg",
  scheduled: "text-state-scheduled-fg",
  waiting: "text-state-waiting-fg",
};
