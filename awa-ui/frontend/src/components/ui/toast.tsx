import { Toaster as Sonner, toast } from "sonner";

function Toaster(props: React.ComponentProps<typeof Sonner>) {
  return (
    <Sonner
      className="toaster group"
      toastOptions={{
        classNames: {
          toast:
            "group toast group-[.toaster]:bg-overlay group-[.toaster]:text-overlay-fg group-[.toaster]:border-border group-[.toaster]:shadow-lg group-[.toaster]:rounded-lg",
          description: "group-[.toast]:text-muted-fg",
          actionButton: "group-[.toast]:bg-primary group-[.toast]:text-primary-fg",
          cancelButton: "group-[.toast]:bg-muted group-[.toast]:text-muted-fg",
          success: "group-[.toaster]:!bg-success-subtle group-[.toaster]:!text-success-subtle-fg group-[.toaster]:!border-success/20",
          error: "group-[.toaster]:!bg-danger-subtle group-[.toaster]:!text-danger-subtle-fg group-[.toaster]:!border-danger/20",
        },
      }}
      {...props}
    />
  );
}

export { Toaster, toast };
