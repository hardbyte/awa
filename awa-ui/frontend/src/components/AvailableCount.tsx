/** Available count; prefixed with "≈" when the backlog exceeds the exact-count limit. */
export function AvailableCount({
  count,
  isEstimate,
}: {
  count: number;
  isEstimate?: boolean;
}) {
  if (!isEstimate) return <>{count.toLocaleString()}</>;
  return (
    <span title="estimated: backlog exceeds exact-count limit; lag is a lower bound">
      ≈{count.toLocaleString()}
    </span>
  );
}
