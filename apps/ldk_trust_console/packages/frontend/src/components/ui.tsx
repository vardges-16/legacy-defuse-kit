//-----------------------------------------------------------------------
// Small presentational building blocks shared by every view.
//-----------------------------------------------------------------------

import type { ReactNode } from 'react';
import { AlertTriangle, Inbox } from 'lucide-react';

import { cn } from '@/lib/utils';
import type { Tone } from '@/lib/format';
import { toneDot, toneSoft, toneText } from '@/lib/tone';


export function Card({
  title,
  eyebrow,
  action,
  children,
  className,
}: {
  title?: ReactNode;
  eyebrow?: ReactNode;
  action?: ReactNode;
  children: ReactNode;
  className?: string;
}) {
  return (
    <section className={cn('rounded-2xl border border-border bg-card p-500 shadow-card', className)}>
      {(title || eyebrow || action) && (
        <header className="mb-400 flex items-start justify-between gap-300">
          <div>
            {eyebrow && <p className="font-monospace text-200 uppercase tracking-widest text-muted-foreground">{eyebrow}</p>}
            {title && <h2 className="font-heading text-500 leading-500 font-semibold">{title}</h2>}
          </div>
          {action}
        </header>
      )}
      {children}
    </section>
  );
}

export function Pill({ tone, children }: { tone: Tone; children: ReactNode }) {
  return (
    <span className={cn('inline-flex items-center gap-100 rounded-full px-200 py-100-nudge font-monospace text-200 font-medium', toneSoft[tone])}>
      <span className={cn('size-[6px] rounded-full', toneDot[tone])} aria-hidden />
      {children}
    </span>
  );
}

export function Dot({ tone, className }: { tone: Tone; className?: string }) {
  return <span className={cn('inline-block size-[8px] rounded-full', toneDot[tone], className)} aria-hidden />;
}

export function Kpi({ label, value, sub, tone }: { label: string; value: ReactNode; sub?: ReactNode; tone?: Tone }) {
  return (
    <div className="flex flex-col gap-100">
      <span className="font-monospace text-200 uppercase tracking-widest text-muted-foreground">{label}</span>
      <span className={cn('font-numeric text-hero-800 leading-hero-800 font-semibold tabular-nums', tone && toneText[tone])}>{value}</span>
      {sub && <span className="text-200 text-muted-foreground">{sub}</span>}
    </div>
  );
}

export function Skeleton({ className }: { className?: string }) {
  return <div className={cn('animate-pulse rounded-xl bg-muted', className)} />;
}

export function ErrorState({ error }: { error: Error }) {
  const msg = error.message.length > 220 ? `${error.message.slice(0, 220)}…` : error.message;
  return (
    <div role="alert" className="flex items-start gap-300 rounded-xl border border-bad/40 bg-bad-soft p-400 text-300">
      <AlertTriangle className="icon-size-300 shrink-0 text-bad" aria-hidden />
      <div>
        <p className="font-semibold text-bad">Couldn't read the Trust Ledger</p>
        <p className="mt-100 text-foreground">{msg}</p>
        <p className="mt-100 text-200 text-muted-foreground">
          The app reads lh_trust with your own Fabric permissions. Check that you can open lh_trust in the workspace.
        </p>
      </div>
    </div>
  );
}

export function Empty({ children }: { children: ReactNode }) {
  return (
    <div className="flex flex-col items-center justify-center gap-200 py-800 text-300 text-muted-foreground">
      <Inbox className="icon-size-400" aria-hidden />
      <p className="text-center">{children}</p>
    </div>
  );
}

/** Renders loading / error / empty around a data block. */
export function Loader<T>({
  state,
  skeleton,
  isEmpty,
  empty,
  children,
}: {
  state: { data: T | undefined; error: Error | null; loading: boolean };
  skeleton: ReactNode;
  isEmpty?: (d: T) => boolean;
  empty?: ReactNode;
  children: (d: T) => ReactNode;
}) {
  if (state.error) return <ErrorState error={state.error} />;
  if (state.loading || state.data === undefined) return <>{skeleton}</>;
  if (isEmpty?.(state.data)) return <Empty>{empty ?? 'No data yet.'}</Empty>;
  return <>{children(state.data)}</>;
}
