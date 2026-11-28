/** A count with the browser's digit grouping, e.g. "3,000". */
export function formatNumber(value: number): string {
  return value.toLocaleString();
}

/** "1 job", "3,000 jobs". */
export function plural(count: number, singular: string, pluralForm = `${singular}s`): string {
  return `${formatNumber(count)} ${count === 1 ? singular : pluralForm}`;
}

/** A share as a percentage: one decimal below 10%, e.g. "0.4%", "2.5%", "38%". */
export function formatPercent(share: number): string {
  const percent = share * 100;
  if (percent > 0 && percent < 0.1) return "<0.1%";
  return `${percent < 10 ? percent.toFixed(1).replace(/\.0$/, "") : Math.round(percent)}%`;
}
