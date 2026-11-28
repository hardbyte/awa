import { Link, type LinkProps } from "@tanstack/react-router";

export interface RowCardItem {
  key: string | number;
  /** The row's name; it links to the row's page, and the whole card follows it. */
  title: React.ReactNode;
  link: LinkProps;
  /** A quiet line beside the title, e.g. a job id. */
  titleSuffix?: React.ReactNode;
  /** Badges or a state, after the title. */
  status?: React.ReactNode;
  /** The one fact that says why the row matters, e.g. its last error. */
  why?: React.ReactNode;
  /** Secondary facts and the row's time. */
  meta?: React.ReactNode;
  /** A labelled checkbox for list selection. */
  selection?: { checked: boolean; onChange: (checked: boolean) => void; label: string; disabled?: boolean };
  /** Buttons that act on the row. */
  actions?: React.ReactNode;
}

interface RowCardsProps {
  items: RowCardItem[];
  /** The list's accessible name. */
  label: string;
  /** Shown instead of the list when there are no items: loading, error or empty. */
  placeholder?: React.ReactNode;
  className?: string;
}

/**
 * A list of cards for phone widths, standing in for a table. Each card's
 * title is a link, so Tab reaches it and Enter opens the row; a click
 * anywhere on the card does the same, except on its checkbox and buttons.
 */
export function RowCards({ items, label, placeholder, className = "sm:hidden" }: RowCardsProps) {
  if (items.length === 0) {
    return placeholder ? (
      <div className={`rounded-lg border p-6 text-center text-sm text-muted-fg ${className}`}>{placeholder}</div>
    ) : null;
  }
  return (
    <ul aria-label={label} className={`space-y-2 ${className}`} data-testid="row-cards">
      {items.map((item) => (
        <li
          key={item.key}
          className="relative rounded-lg border p-3 transition-colors focus-within:ring-2 focus-within:ring-ring has-[input:checked]:border-primary has-[input:checked]:bg-primary-subtle/30 hover:bg-muted/40"
          data-testid="row-card"
        >
          <div className="flex items-start gap-3">
            {item.selection && (
              <input
                type="checkbox"
                className="relative z-10 mt-1 size-4 shrink-0"
                aria-label={item.selection.label}
                checked={item.selection.checked}
                disabled={item.selection.disabled}
                onChange={(event) => item.selection!.onChange(event.target.checked)}
              />
            )}
            <div className="min-w-0 flex-1">
              <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
                <Link
                  {...item.link}
                  className="font-medium text-fg no-underline outline-hidden after:absolute after:inset-0 after:content-['']"
                >
                  {item.title}
                </Link>
                {item.titleSuffix && <span className="text-xs text-muted-fg">{item.titleSuffix}</span>}
                {item.status && <span className="ml-auto flex flex-wrap items-center gap-1">{item.status}</span>}
              </div>
              {item.why && <div className="mt-1 line-clamp-2 text-sm text-fg">{item.why}</div>}
              {item.meta && (
                <div className="mt-1 flex flex-wrap gap-x-3 gap-y-0.5 text-xs text-muted-fg">{item.meta}</div>
              )}
              {item.actions && <div className="relative z-10 mt-2 flex flex-wrap gap-2">{item.actions}</div>}
            </div>
          </div>
        </li>
      ))}
    </ul>
  );
}
