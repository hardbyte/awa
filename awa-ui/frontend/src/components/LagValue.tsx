/** Colour-coded lag display: green <10s, amber 10-60s, red >60s */
export function LagValue({ seconds }: { seconds: number | null }) {
  if (seconds == null) return <span className="text-muted-fg">-</span>;

  const color =
    seconds < 10
      ? "text-success"
      : seconds < 60
        ? "text-warning"
        : "text-danger";

  return <span className={color}>{seconds.toFixed(1)}</span>;
}
