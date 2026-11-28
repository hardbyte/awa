import { useId } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchCapabilities } from "@/lib/api";
import { READ_ONLY_REASON } from "@/lib/confirm";
import { Button, type ButtonProps } from "@/components/ui/button";

/**
 * Whether admin actions are off, and the reason to show once it's known.
 * Actions stay off without a reason while the server's capabilities load.
 */
export function useAdminGate(): { disabled: boolean; reason?: string } {
  const capabilities = useQuery({
    queryKey: ["capabilities"],
    queryFn: fetchCapabilities,
    enabled: false, refetchOnWindowFocus: false, refetchOnReconnect: false, retry: false,
    staleTime: 60_000,
  });
  if (!capabilities.isSuccess) return { disabled: true };
  return capabilities.data.read_only ? { disabled: true, reason: READ_ONLY_REASON } : { disabled: false };
}

/** Shows a disabled control's reason on hover and to assistive technology. */
export function DisabledReason({
  reason,
  children,
}: {
  reason: string | undefined;
  children: (describedBy: string | undefined) => React.ReactNode;
}) {
  const id = useId();
  if (!reason) return <>{children(undefined)}</>;
  return (
    <span title={reason} className="inline-flex" data-disabled-reason={reason}>
      {children(id)}
      <span id={id} hidden>
        {reason}
      </span>
    </span>
  );
}

/** A button for an admin action: off on a read-only server, saying why. */
export function AdminButton({
  isDisabled,
  disabledReason,
  ...props
}: ButtonProps & { disabledReason?: string }) {
  const gate = useAdminGate();
  const reason = gate.reason ?? (isDisabled ? disabledReason : undefined);
  return (
    <DisabledReason reason={reason}>
      {(describedBy) => <Button {...props} isDisabled={gate.disabled || isDisabled} aria-describedby={describedBy} />}
    </DisabledReason>
  );
}
