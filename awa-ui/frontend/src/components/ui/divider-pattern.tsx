import { twMerge } from "tailwind-merge"

export interface DividerPatternProps extends React.ComponentProps<"div"> {
  direction?: "left" | "right"
  mask?:
    | "top"
    | "top-right"
    | "right"
    | "bottom-right"
    | "bottom"
    | "bottom-left"
    | "left"
    | "top-left"
    | "y"
    | "-y"
    | "center"
    | "-center"
    | "none"
}

const angleMap = {
  left: "-315deg",
  right: "315deg",
}

const maskMap: Record<Exclude<DividerPatternProps["mask"], undefined>, string> = {
  top: "mask-[radial-gradient(100%_100%_at_top,white,transparent)]",
  "top-right": "mask-[radial-gradient(100%_100%_at_top_right,white,transparent)]",
  right: "mask-[radial-gradient(100%_100%_at_right,white,transparent)]",
  "bottom-right": "mask-[radial-gradient(100%_100%_at_bottom_right,white,transparent)]",
  bottom: "mask-[radial-gradient(100%_100%_at_bottom,white,transparent)]",
  "bottom-left": "mask-[radial-gradient(100%_100%_at_bottom_left,white,transparent)]",
  left: "mask-[radial-gradient(100%_100%_at_left,white,transparent)]",
  "top-left": "mask-[radial-gradient(100%_100%_at_top_left,white,transparent)]",
  y: "mask-[linear-gradient(to_bottom,white_0,transparent_22%,transparent_78%,white_100%)]",
  "-y": "mask-[linear-gradient(to_bottom,transparent_0,white_22%,white_78%,transparent_100%)]",
  center: "mask-[radial-gradient(120%_120%_at_50%_50%,transparent_0_35%,white_70%)]",
  "-center": "mask-[radial-gradient(120%_120%_at_50%_50%,transparent_0_35%,white_70%)]",
  none: "",
}

export function DividerPattern({
  className,
  direction = "right",
  mask = "none",
  style,
  ...props
}: DividerPatternProps) {
  const angle = angleMap[direction]

  return (
    <div
      className={twMerge(
        "absolute inset-0 -z-10 border-[--pattern-fg] bg-size-[10px_10px] bg-fixed [--pattern-fg:var(--color-border)]",
        maskMap[mask],
        className,
      )}
      style={{
        backgroundImage: `repeating-linear-gradient(${angle}, var(--pattern-fg) 0, var(--pattern-fg) 1px, transparent 0, transparent 50%)`,
        ...style,
      }}
      {...props}
    />
  )
}
