/** A count with the browser's digit grouping, e.g. "3,000". */
export function formatNumber(value: number): string {
  return value.toLocaleString();
}

/** "1 job", "3,000 jobs". */
export function plural(count: number, singular: string, pluralForm = `${singular}s`): string {
  return `${formatNumber(count)} ${count === 1 ? singular : pluralForm}`;
}
