import { twMerge } from "tailwind-merge"

interface NoiseProps extends React.ComponentProps<"div"> {
  className?: string
  opacity?: number
  grainSize?: number
  color?: string
}

const Noise = ({
  className,
  opacity = 0.1,
  grainSize = 200,
  color = "var(--color-bg)",
  ...props
}: NoiseProps) => {
  const svg = encodeURIComponent(`
    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 600 600">
      <filter id="a">
        <feTurbulence type="fractalNoise" baseFrequency=".65" numOctaves="3" stitchTiles="stitch" />
      </filter>
      <rect width="100%" height="100%" filter="url(#a)" />
    </svg>
  `)

  return (
    <div className={twMerge("pointer-events-none absolute inset-0", className)} {...props}>
      <div
        className="absolute inset-0"
        style={{
          backgroundImage: `url("data:image/svg+xml,${svg}")`,
          backgroundRepeat: "repeat",
          backgroundSize: `${grainSize}px`,
          opacity,
        }}
      />
      <div
        className="absolute inset-0 mix-blend-overlay"
        style={{
          backgroundColor: color,
          opacity,
        }}
      />
    </div>
  )
}

export type { NoiseProps }
export { Noise }
