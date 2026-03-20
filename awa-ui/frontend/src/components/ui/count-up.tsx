"use client"

import { useInView, useMotionValue, useSpring } from "motion/react"
import { useEffect, useRef } from "react"

export interface CountUpProps {
  end: number
  start?: number
  direction?: "up" | "down"
  delay?: number
  duration?: number
  className?: string
  startWhen?: boolean
  separator?: string
  onStart?: () => void
  onEnd?: () => void
  format?: (value: number) => string
  precision?: number
}

export function CountUp({
  end,
  start = 0,
  direction = "up",
  delay = 0,
  duration = 0.1,
  className = "",
  startWhen = true,
  separator = "",
  onStart,
  onEnd,
  format,
  precision,
}: CountUpProps) {
  const ref = useRef<HTMLSpanElement>(null)
  const motionValue = useMotionValue(direction === "down" ? end : start)

  const getDecimalPlaces = (num: number): number => {
    const s = String(num)
    const i = s.indexOf(".")
    return i === -1 ? 0 : s.length - i - 1
  }

  const decimals =
    typeof precision === "number"
      ? Math.max(0, precision)
      : Number.isInteger(start) && Number.isInteger(end)
        ? 0
        : Math.max(getDecimalPlaces(start), getDecimalPlaces(end))

  const damping = 30 + 20 * (1 / Math.max(0.05, duration))
  const stiffness = 140 * (1 / Math.max(0.05, duration))

  const springValue = useSpring(motionValue, {
    damping,
    stiffness,
    restDelta: 0.001,
    restSpeed: 0.001,
  })

  const isInView = useInView(ref, { once: true, margin: "0px" })

  const formatNumber = (n: number) => {
    if (format) return format(n)
    const rounded = Number(n.toFixed(decimals))
    const formatted = new Intl.NumberFormat("en-US", {
      useGrouping: !!separator,
      minimumFractionDigits: decimals,
      maximumFractionDigits: decimals,
    }).format(rounded)
    return separator ? formatted.replace(/,/g, separator) : formatted
  }

  useEffect(() => {
    if (ref.current) {
      ref.current.textContent = formatNumber(direction === "down" ? end : start)
    }
  }, [start, end, direction, decimals, separator, format])

  useEffect(() => {
    if (isInView && startWhen) {
      if (typeof onStart === "function") onStart()
      const t1 = setTimeout(() => {
        motionValue.set(direction === "down" ? start : end)
      }, delay * 1000)
      const t2 = setTimeout(
        () => {
          if (typeof onEnd === "function") onEnd()
        },
        (delay + duration) * 1000,
      )
      return () => {
        clearTimeout(t1)
        clearTimeout(t2)
      }
    }
  }, [isInView, startWhen, motionValue, direction, start, end, delay, onStart, onEnd, duration])

  useEffect(() => {
    const unsub = springValue.on("change", (latest) => {
      if (!ref.current) return
      ref.current.textContent = formatNumber(latest)
    })
    return () => unsub()
  }, [springValue, decimals, separator, format])

  return <span className={className} ref={ref} />
}
