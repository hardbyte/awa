/** Heroicons has no hourglass; drawn to match the 16px solid set. */
export function HourglassIcon(props: React.ComponentProps<"svg">) {
  return (
    <svg viewBox="0 0 16 16" fill="currentColor" aria-hidden="true" {...props}>
      <path d="M3.5 1.5h9a.75.75 0 0 1 0 1.5H12v1.6a2 2 0 0 1-.6 1.4L9.4 8l2 2a2 2 0 0 1 .6 1.4V13h.5a.75.75 0 0 1 0 1.5h-9a.75.75 0 0 1 0-1.5H4v-1.6a2 2 0 0 1 .6-1.4l2-2-2-2A2 2 0 0 1 4 4.6V3h-.5a.75.75 0 0 1 0-1.5Z" />
    </svg>
  );
}
