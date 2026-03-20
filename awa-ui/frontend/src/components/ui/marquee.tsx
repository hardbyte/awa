"use client"

import { useMemo } from "react"
import { twMerge } from "tailwind-merge"

interface MarqueeProps extends React.ComponentProps<"div"> {
  reverse?: boolean
  pauseOnHover?: boolean
  vertical?: boolean
  /**
   * @default 4
   */
  repeat?: number
  autoFill?: boolean
  ariaLive?: "off" | "polite" | "assertive"
}

const Marquee = ({
  className,
  reverse = false,
  pauseOnHover = false,
  children,
  vertical = false,
  repeat = 4,
  ariaLive = "off",
  ref,
  ...props
}: MarqueeProps) => {
  return (
    <div
      {...props}
      ref={ref}
      data-slot="marquee"
      className={twMerge(
        "group flex gap-(--gap) overflow-hidden p-2 outline-hidden [--duration:40s] [--gap:--spacing(4)]",
        vertical ? "flex-col" : "flex-row",
        className,
      )}
      aria-live={ariaLive}
      role={"role" in props ? props.role : "marquee"}
      tabIndex={0}
    >
      {useMemo(
        () => (
          <>
            {Array.from({ length: repeat }, (_, i) => (
              <div
                key={i}
                className={twMerge(
                  "flex shrink-0 justify-around gap-(--gap)",
                  vertical ? "animate-marquee-vertical flex-col" : "animate-marquee flex-row",
                  pauseOnHover && "group-hover:paused",
                  reverse && "direction-[reverse]",
                )}
              >
                {children}
              </div>
            ))}
          </>
        ),
        [repeat, children, vertical, pauseOnHover, reverse],
      )}
    </div>
  )
}

type MarqueeFadeProps = React.HTMLAttributes<HTMLDivElement> & {
  side: "left" | "right"
}

const MarqueeFade = ({ className, side, ...props }: MarqueeFadeProps) => (
  <div
    className={twMerge(
      "absolute top-0 bottom-0 z-10 h-full w-24 from-bg to-transparent",
      side === "left" ? "left-0 bg-linear-to-r" : "right-0 bg-linear-to-l",
      className,
    )}
    {...props}
  />
)

export type { MarqueeProps, MarqueeFadeProps }
export { Marquee, MarqueeFade }
