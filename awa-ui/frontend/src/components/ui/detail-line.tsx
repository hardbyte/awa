import { Children, isValidElement } from "react"
import { twMerge } from "tailwind-merge"

interface DetailLineProps extends React.ComponentProps<"div"> {
  gap?: number
}

const DetailLine = ({ gap = 4, className, ...props }: DetailLineProps) => {
  const _gap =
    gap === 1
      ? "gap-y-1"
      : gap === 2
        ? "gap-y-2"
        : gap === 4
          ? "gap-y-4"
          : gap === 6
            ? "gap-y-6"
            : gap === 8
              ? "gap-y-8"
              : "gap-y-12"
  return <div className={twMerge("flex flex-col", _gap, className)} {...props} />
}

interface ItemProps extends React.ComponentProps<"div"> {
  label?: string
  description?: string
}

const DetailLineItem = ({ className, label, description, children, ...props }: ItemProps) => {
  const labelChild = Children.toArray(children).find(
    (child) => isValidElement(child) && child.type === DetailLineLabel,
  )
  const descriptionChild = Children.toArray(children).find(
    (child) => isValidElement(child) && child.type === DetailLineDescription,
  )

  return (
    <div className={twMerge("grid grid-cols-[auto_1fr_auto] items-center", className)} {...props}>
      {label && !labelChild && <DetailLineLabel>{label}</DetailLineLabel>}
      {labelChild && labelChild}
      <span className="mx-2 after:block after:h-[1.5px] after:grow after:bg-[repeating-linear-gradient(to_right,theme(--color-muted-fg/50%)_0,theme(--color-muted-fg/50%)_1.5px,transparent_1.5px,transparent_6px)] after:bg-repeat-x after:content-['']" />
      {description && !descriptionChild && (
        <DetailLineDescription>{description}</DetailLineDescription>
      )}
      {descriptionChild && descriptionChild}
    </div>
  )
}

const DetailLineLabel = ({ className, ...props }: React.ComponentProps<"div">) => {
  return <div {...props} slot="label" className={twMerge("text-muted-fg text-sm", className)} />
}

const DetailLineDescription = ({ className, ...props }: React.HTMLAttributes<HTMLDivElement>) => {
  return (
    <div
      {...props}
      slot="description"
      className={twMerge(
        "text-sm *:data-[slot=icon]:size-3.5 *:data-[slot=icon]:shrink-0 *:data-[slot=icon]:text-muted-fg",
        className,
      )}
    />
  )
}

export { DetailLine, DetailLineItem, DetailLineLabel, DetailLineDescription }
