import { type ReactNode } from "react";
import { cn } from "@/lib/cn";

type Tab = {
    key: string;
    label: ReactNode;
};

type Props = {
    tabs: Tab[];
    active: string;
    onChange: (key: string) => void;
};

/** Pestañas mínimas controladas. */
export function Tabs({ tabs, active, onChange }: Props) {
    return (
        <div className="flex border-b border-slate-200">
            {tabs.map((t) => (
                <button
                    key={t.key}
                    type="button"
                    onClick={() => onChange(t.key)}
                    className={cn(
                        "border-b-2 px-3 py-2 text-sm",
                        t.key === active
                            ? "border-slate-700 font-medium text-slate-800"
                            : "border-transparent text-slate-500 hover:text-slate-700",
                    )}
                >
                    {t.label}
                </button>
            ))}
        </div>
    );
}
