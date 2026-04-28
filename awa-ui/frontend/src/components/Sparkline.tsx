/**
 * Minimal SVG sparkline. Renders a polyline with an optional baseline;
 * no axes, no labels. Designed for inline use inside stat tiles, rows,
 * or badges.
 *
 * Degrades gracefully: when there are fewer than two numeric samples
 * the component renders an empty slot of the requested size so layout
 * doesn't jump when data lags or isn't yet available (important on
 * pre-v10 deployments where the timeseries endpoint may return an
 * empty series for recently-seeded databases).
 */
export interface SparklineProps {
  data: ReadonlyArray<number>;
  width?: number;
  height?: number;
  /** Stroke colour. Defaults to currentColor. */
  color?: string;
  /** Stroke width in px. */
  strokeWidth?: number;
  /** ARIA label for screen readers. */
  label?: string;
  className?: string;
}

export function Sparkline({
  data,
  width = 64,
  height = 20,
  color = "currentColor",
  strokeWidth = 1.5,
  label,
  className,
}: SparklineProps) {
  const finite = data.filter((n) => Number.isFinite(n));
  if (finite.length < 2) {
    return (
      <svg
        width={width}
        height={height}
        className={className}
        role="img"
        aria-label={label ?? "sparkline (no data)"}
      />
    );
  }

  const min = Math.min(...finite);
  const max = Math.max(...finite);
  const range = max - min || 1;
  const step = finite.length > 1 ? width / (finite.length - 1) : width;

  const points = finite
    .map((value, index) => {
      const x = index * step;
      const y = height - ((value - min) / range) * height;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");

  return (
    <svg
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      className={className}
      role="img"
      aria-label={label ?? `sparkline with ${finite.length} points`}
    >
      <polyline
        points={points}
        fill="none"
        stroke={color}
        strokeWidth={strokeWidth}
        strokeLinecap="round"
        strokeLinejoin="round"
        vectorEffect="non-scaling-stroke"
      />
    </svg>
  );
}
