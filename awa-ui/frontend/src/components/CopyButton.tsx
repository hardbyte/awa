import { useState } from "react";
import { Button } from "@/components/ui/button";

interface CopyButtonProps {
  value: string;
  label?: string;
}

export function CopyButton({ value, label }: CopyButtonProps) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    await navigator.clipboard.writeText(value);
    setCopied(true);
    setTimeout(() => setCopied(false), 1500);
  };

  return (
    <Button
      intent="plain"
      size="sq-xs"
      aria-label={label ?? "Copy to clipboard"}
      onPress={() => void handleCopy()}
      className="inline-flex opacity-0 group-hover:opacity-100 transition-opacity"
    >
      {copied ? (
        <svg data-slot="icon" viewBox="0 0 20 20" fill="currentColor" className="text-success">
          <path
            fillRule="evenodd"
            d="M16.704 4.153a.75.75 0 0 1 .143 1.052l-8 10.5a.75.75 0 0 1-1.127.075l-4.5-4.5a.75.75 0 0 1 1.06-1.06l3.894 3.893 7.48-9.817a.75.75 0 0 1 1.05-.143Z"
            clipRule="evenodd"
          />
        </svg>
      ) : (
        <svg data-slot="icon" viewBox="0 0 20 20" fill="currentColor">
          <path d="M7 3.5A1.5 1.5 0 0 1 8.5 2h3.879a1.5 1.5 0 0 1 1.06.44l3.122 3.12A1.5 1.5 0 0 1 17 6.622V12.5a1.5 1.5 0 0 1-1.5 1.5h-1v-3.379a3 3 0 0 0-.879-2.121L10.5 5.379A3 3 0 0 0 8.379 4.5H7v-1Z" />
          <path d="M4.5 6A1.5 1.5 0 0 0 3 7.5v9A1.5 1.5 0 0 0 4.5 18h7a1.5 1.5 0 0 0 1.5-1.5v-5.879a1.5 1.5 0 0 0-.44-1.06L9.44 6.439A1.5 1.5 0 0 0 8.378 6H4.5Z" />
        </svg>
      )}
    </Button>
  );
}

/** Inline text with a copy button that appears on hover. */
export function Copyable({
  value,
  children,
}: {
  value: string;
  children: React.ReactNode;
}) {
  return (
    <span className="group inline-flex items-center gap-1">
      {children}
      <CopyButton value={value} />
    </span>
  );
}
