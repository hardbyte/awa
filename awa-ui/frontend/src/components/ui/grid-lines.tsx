import { twMerge } from "tailwind-merge"

interface GridLinesProps extends Omit<React.ComponentProps<"svg">, "width" | "height"> {
  width?: number
  height?: number
  mask?:
    | "top"
    | "top_right"
    | "right"
    | "bottom_right"
    | "bottom"
    | "bottom_left"
    | "left"
    | "top_left"
    | "none"
}

const maskMap: Record<Exclude<GridLinesProps["mask"], undefined>, string> = {
  top: "mask-[radial-gradient(100%_100%_at_top,white,transparent)]",
  top_right: "mask-[radial-gradient(100%_100%_at_top_right,white,transparent)]",
  right: "mask-[radial-gradient(100%_100%_at_right,white,transparent)]",
  bottom_right: "mask-[radial-gradient(100%_100%_at_bottom_right,white,transparent)]",
  bottom: "mask-[radial-gradient(100%_100%_at_bottom,white,transparent)]",
  bottom_left: "mask-[radial-gradient(100%_100%_at_bottom_left,white,transparent)]",
  left: "mask-[radial-gradient(100%_100%_at_left,white,transparent)]",
  top_left: "mask-[radial-gradient(100%_100%_at_top_left,white,transparent)]",
  none: "",
}

const GridLines = ({
  className,
  width = 100,
  height = 100,
  mask = "top_right",
  ...props
}: GridLinesProps) => {
  return (
    <svg
      className={twMerge(
        "absolute inset-0 -z-10 h-full w-full stroke-fg/10",
        maskMap[mask],
        className,
      )}
      aria-hidden="true"
      {...props}
    >
      <defs>
        <pattern
          id="0787a7c5-978c-4f66-83c7-11c213f99cb7"
          width={width}
          height={height}
          x="50%"
          y={-1}
          patternUnits="userSpaceOnUse"
        >
          <path d="M.5 200V.5H200" fill="none" />
        </pattern>
      </defs>
      <rect
        width="100%"
        height="100%"
        strokeWidth={0}
        fill="url(#0787a7c5-978c-4f66-83c7-11c213f99cb7)"
      />
    </svg>
  )
}

export { GridLines }
