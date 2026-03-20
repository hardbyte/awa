import { twMerge } from "tailwind-merge"
import { Text } from "@/components/ui/text"

export function Empty({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="empty"
      className={twMerge(
        "flex min-w-0 flex-1 flex-col items-center justify-center gap-6 text-balance rounded-lg border-dashed p-6 text-center md:p-12",
        className,
      )}
      {...props}
    />
  )
}

export function EmptyHeader({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="empty-header"
      className={twMerge("flex max-w-sm flex-col items-center gap-2 text-center", className)}
      {...props}
    />
  )
}

export function EmptyMedia({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="empty-media"
      className={twMerge(
        "mb-2 flex shrink-0 items-center justify-center [&_svg]:pointer-events-none [&_svg]:shrink-0",
        className,
      )}
      {...props}
    />
  )
}

export function EmptyTitle({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="empty-title"
      className={twMerge("font-medium text-lg tracking-tight", className)}
      {...props}
    />
  )
}

export function EmptyDescription({ className, ...props }: React.ComponentProps<"p">) {
  return (
    <Text
      data-slot="empty-description"
      className={twMerge(
        "[&>a:hover]:text-primary-subtle-fg [&>a]:underline [&>a]:underline-offset-4",
        className,
      )}
      {...props}
    />
  )
}

export function EmptyContent({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="empty-content"
      className={twMerge(
        "flex w-full min-w-0 max-w-sm flex-col items-center gap-4 text-balance text-sm",
        className,
      )}
      {...props}
    />
  )
}
