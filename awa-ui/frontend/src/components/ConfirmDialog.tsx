/**
 * Reusable confirmation dialog using IntentUI Modal.
 * For destructive actions like draining a queue.
 */

import {
  Modal,
  ModalContent,
  ModalHeader,
  ModalBody,
  ModalFooter,
  ModalClose,
} from "@/components/ui/modal";
import { Button } from "@/components/ui/button";

interface ConfirmDialogProps {
  isOpen: boolean;
  onOpenChange: (open: boolean) => void;
  title: string;
  description: string;
  confirmLabel?: string;
  confirmIntent?: "primary" | "danger";
  onConfirm: () => void;
  isPending?: boolean;
}

export function ConfirmDialog({
  isOpen,
  onOpenChange,
  title,
  description,
  confirmLabel = "Confirm",
  confirmIntent = "danger",
  onConfirm,
  isPending,
}: ConfirmDialogProps) {
  return (
    <Modal isOpen={isOpen} onOpenChange={onOpenChange}>
      <ModalContent role="alertdialog" size="sm" closeButton={false}>
        <ModalHeader title={title} description={description} />
        <ModalBody>
          <p className="text-sm text-muted-fg">This action cannot be undone.</p>
        </ModalBody>
        <ModalFooter>
          <ModalClose intent="outline">Cancel</ModalClose>
          <Button
            intent={confirmIntent}
            onPress={() => {
              onConfirm();
              onOpenChange(false);
            }}
            isDisabled={isPending}
          >
            {confirmLabel}
          </Button>
        </ModalFooter>
      </ModalContent>
    </Modal>
  );
}
