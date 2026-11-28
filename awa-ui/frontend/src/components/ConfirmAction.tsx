import { useEffect, useId, useState } from "react";
import { TextField } from "react-aria-components";
import { Modal, ModalBody, ModalContent, ModalDescription, ModalFooter, ModalHeader, ModalTitle } from "@/components/ui/modal";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/field";
import type { ConfirmTier } from "@/lib/confirm";

export interface ConfirmActionProps {
  isOpen: boolean;
  onOpenChange: (open: boolean) => void;
  /** 1: one object with side effects; 2: many jobs; 3: many or irreversible, typed to confirm. */
  tier: ConfirmTier;
  /** Names the object, e.g. "Cancel job #918?". */
  title: string;
  /** What happens and what could go wrong. */
  description: React.ReactNode;
  /** Scope details, e.g. a count and sample rows. */
  children?: React.ReactNode;
  /** Says the action can't be undone; set only when that is true. */
  irreversible?: boolean;
  /** Names the action, e.g. "Cancel job #918". */
  confirmLabel: string;
  dismissLabel?: string;
  intent?: "primary" | "danger";
  /** Runs the action. The dialog stays open until it settles and shows its error. */
  onConfirm: () => Promise<unknown>;
  /** Tier 3: the text to type, e.g. the queue name or the count. */
  confirmText?: string;
  /** Tier 3: what the text is, e.g. "the queue name". */
  confirmTextLabel?: string;
  /** Holds the confirm button, e.g. while the scope is still loading. */
  isConfirmDisabled?: boolean;
}

export function ConfirmAction({
  isOpen,
  onOpenChange,
  tier,
  title,
  description,
  children,
  irreversible = false,
  confirmLabel,
  dismissLabel = "Keep",
  intent = "primary",
  onConfirm,
  confirmText,
  confirmTextLabel = "the text above",
  isConfirmDisabled = false,
}: ConfirmActionProps) {
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [typed, setTyped] = useState("");
  const errorId = useId();

  useEffect(() => {
    if (isOpen) {
      setError(null);
      setTyped("");
    }
  }, [isOpen]);

  const needsTyping = tier === 3 && confirmText !== undefined;
  const typedMatches = !needsTyping || typed.trim() === confirmText;

  async function confirm() {
    setPending(true);
    setError(null);
    try {
      await onConfirm();
      onOpenChange(false);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setPending(false);
    }
  }

  return (
    <Modal isOpen={isOpen} onOpenChange={(open) => !pending && onOpenChange(open)}>
      <ModalContent
        role="alertdialog"
        size={tier === 1 ? "md" : "lg"}
        closeButton={false}
        isKeyboardDismissDisabled={pending}
        // A dialog closed while its open animation still runs can stay stuck on screen.
        className="animate-none!"
        overlay={{ className: "animate-none!" }}
        data-tier={tier}
      >
        <ModalHeader>
          <ModalTitle>{title}</ModalTitle>
          <ModalDescription>{description}</ModalDescription>
        </ModalHeader>
        <ModalBody className="space-y-3 text-sm">
          {children}
          {irreversible && <p className="font-medium text-fg">This can't be undone.</p>}
          {needsTyping && (
            <TextField value={typed} onChange={setTyped} isDisabled={pending} className="space-y-1" autoFocus>
              <Label>
                Type <code className="rounded bg-muted px-1 py-0.5 font-mono">{confirmText}</code> ({confirmTextLabel}) to confirm
              </Label>
              <Input autoComplete="off" spellCheck={false} aria-describedby={error ? errorId : undefined} />
            </TextField>
          )}
          {error && (
            <p id={errorId} role="alert" className="rounded-md bg-danger-subtle px-3 py-2 text-danger-subtle-fg" data-testid="confirm-error">
              {error}
            </p>
          )}
        </ModalBody>
        <ModalFooter>
          <Button intent="outline" slot="close" isDisabled={pending}>
            {dismissLabel}
          </Button>
          <Button
            intent={intent}
            onPress={() => void confirm()}
            isDisabled={isConfirmDisabled || !typedMatches}
            isPending={pending}
          >
            {confirmLabel}
          </Button>
        </ModalFooter>
      </ModalContent>
    </Modal>
  );
}
