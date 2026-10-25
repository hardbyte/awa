/**
 * Sparkline for sampled queue history. Unlike `Sparkline`, points keep their
 * bucket position (so a gap in sampling shows as a gap, not a shorter line)
 * and the y axis starts at zero: a large, steady backlog stays flat instead
 * of min-max scaling turning a 1% wobble into a cliff.
 */
export function HistorySpark({
  data,
  width = 120,
  height = 24,
  label,
  className,
}: {
  data: ReadonlyArray<number | null>;
  width?: number;
  height?: number;
  label: string;
  className?: string;
}) {
  const values = data.filter((value): value is number => value !== null);
  if (values.length === 0) {
    return (
      <svg
        width={width}
        height={height}
        className={className}
        role="img"
        aria-label={`${label}: no samples yet`}
      />
    );
  }
  const max = Math.max(...values) || 1;
  const step = data.length > 1 ? width / (data.length - 1) : 0;
  const pad = 1.5;
  const y = (value: number) => pad + (1 - value / max) * (height - pad * 2);

  const segments: string[][] = [];
  let current: string[] = [];
  data.forEach((value, index) => {
    if (value === null) {
      if (current.length) segments.push(current);
      current = [];
      return;
    }
    current.push(`${(index * step).toFixed(1)},${y(value).toFixed(1)}`);
  });
  if (current.length) segments.push(current);

  return (
    <svg
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      className={className}
      role="img"
      aria-label={label}
    >
      <line
        x1={0}
        x2={width}
        y1={height - pad}
        y2={height - pad}
        stroke="currentColor"
        strokeOpacity={0.15}
        vectorEffect="non-scaling-stroke"
      />
      {segments.map((points, index) =>
        points.length === 1 ? (
          <circle
            key={index}
            cx={points[0]!.split(",")[0]}
            cy={points[0]!.split(",")[1]}
            r={1.5}
            fill="currentColor"
          />
        ) : (
          <polyline
            key={index}
            points={points.join(" ")}
            fill="none"
            stroke="currentColor"
            strokeWidth={1.5}
            strokeLinecap="round"
            strokeLinejoin="round"
            vectorEffect="non-scaling-stroke"
          />
        ),
      )}
    </svg>
  );
}
