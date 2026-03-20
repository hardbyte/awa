import { twMerge } from "tailwind-merge"

export function Details({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      className={twMerge(
        "*:data-[slot=text]:mt-1",
        "[&>*+[data-slot=details-list]]:mt-3",
        className,
      )}
      {...props}
    />
  )
}

export function DetailsHeader({ className, ...props }: React.ComponentProps<"h2">) {
  return <h2 className={twMerge("font-medium text-base/6", className)} {...props} />
}

export function DetailsList({ className, ...props }: React.ComponentProps<"dl">) {
  return (
    <dl
      data-slot="details-list"
      className={twMerge("text-base/6 sm:text-sm/6", className)}
      {...props}
    />
  )
}

export function DetailsTerm({ className, ...props }: React.ComponentProps<"dt">) {
  return <dt className={twMerge("inline whitespace-nowrap text-muted-fg", className)} {...props} />
}

export function DetailsDescription({ className, ...props }: React.ComponentProps<"dd">) {
  return (
    <dd
      className={twMerge(
        "block sm:inline",
        "mt-1 sm:ms-2 sm:mt-0",
        "after:mt-4 after:block after:content-[''] sm:after:mt-3",
        "*:data-[slot=text]:mt-1",
        "*:data-[slot=icon]:me-2 *:data-[slot=icon]:inline *:data-[slot=icon]:h-lh *:data-[slot=icon]:w-4.5 *:data-[slot=icon]:text-muted-fg sm:*:data-[slot=icon]:w-4",
        className,
      )}
      {...props}
    />
  )
}

export function DetailsFooter({ className, ...props }: React.ComponentProps<"div">) {
  return <div data-slot="details-footer" className={twMerge("mt-6", className)} {...props} />
}
