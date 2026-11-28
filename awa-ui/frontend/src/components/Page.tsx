import { Breadcrumbs, BreadcrumbsItem } from "@/components/ui/breadcrumbs";
import { Heading } from "@/components/ui/heading";

export interface Crumb {
  label: string;
  /** A path inside the app; omitted for the current page. */
  href?: string;
}

interface PageProps {
  /** "full" for lists and tables, "readable" for detail pages. */
  width?: "full" | "readable";
  className?: string;
  children: React.ReactNode;
}

export function Page({ width = "full", className = "", children }: PageProps) {
  return (
    <div
      className={`space-y-6 ${width === "readable" ? "w-full max-w-5xl" : "w-full"} ${className}`}
      data-slot="page"
    >
      {children}
    </div>
  );
}

interface PageHeaderProps {
  title: React.ReactNode;
  /** Trail to this page; the page itself is added as the last crumb. */
  breadcrumbs?: Crumb[];
  /** Plain-text name for the last crumb when the title is not a string. */
  crumbLabel?: string;
  /** Badges beside the title. */
  status?: React.ReactNode;
  /** Buttons at the end of the header row. */
  actions?: React.ReactNode;
  /** A line under the title. */
  description?: React.ReactNode;
  children?: React.ReactNode;
}

export function PageHeader({
  title,
  breadcrumbs,
  crumbLabel,
  status,
  actions,
  description,
  children,
}: PageHeaderProps) {
  const current = crumbLabel ?? (typeof title === "string" ? title : undefined);
  return (
    <div className="space-y-2" data-slot="page-header">
      {breadcrumbs && breadcrumbs.length > 0 && (
        <nav aria-label="Breadcrumbs">
        <Breadcrumbs className="flex-wrap text-muted-fg">
          {breadcrumbs.map((crumb) => (
            <BreadcrumbsItem key={`${crumb.label}-${crumb.href ?? ""}`} href={crumb.href}>
              {crumb.label}
            </BreadcrumbsItem>
          ))}
          {current && <BreadcrumbsItem>{current}</BreadcrumbsItem>}
        </Breadcrumbs>
        </nav>
      )}
      <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
        <Heading level={1} className="min-w-0 break-words">
          {title}
        </Heading>
        {status && <div className="flex flex-wrap items-center gap-2">{status}</div>}
        {actions && <div className="ml-auto flex flex-wrap items-center gap-2">{actions}</div>}
      </div>
      {description && <div className="max-w-3xl text-sm text-muted-fg">{description}</div>}
      {children}
    </div>
  );
}
