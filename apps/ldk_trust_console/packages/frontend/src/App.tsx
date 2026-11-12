//-----------------------------------------------------------------------
// Trust Ledger console — Legacy Defuse Kit (team de_dust2).
// Read-only tracking of one_pipeline_to_rule_them_all and the Trust Ledger in lh_trust.
//-----------------------------------------------------------------------

import { useState, type ReactNode } from 'react';
import { Activity, Fingerprint, ListChecks, Moon, RefreshCw, ScanSearch, Sun } from 'lucide-react';

import { Dot } from '@/components/ui';
import { useTheme } from '@/hooks/theme.context';
import { useQuery } from '@/hooks/use-query';
import { fmtAgo, toneOf } from '@/lib/format';
import { fetchRuns } from '@/lib/trust-api';
import { cn } from '@/lib/utils';
import { Overview } from '@/views/Overview';
import { Quality } from '@/views/Quality';
import { Reconciliation } from '@/views/Reconciliation';
import { Runs } from '@/views/Runs';

const TABS = [
  { id: 'overview', label: 'Overview', icon: Fingerprint },
  { id: 'runs', label: 'Runs', icon: Activity },
  { id: 'reconciliation', label: 'Reconciliation', icon: ListChecks },
  { id: 'quality', label: 'Quality', icon: ScanSearch },
] as const;
type TabId = (typeof TABS)[number]['id'];

function App() {
  const [tab, setTab] = useState<TabId>('overview');
  const [refreshKey, setRefreshKey] = useState(0);
  const { isDark, toggleTheme } = useTheme();
  const latest = useQuery(() => fetchRuns(1), [refreshKey]);
  const run = latest.data?.[0];

  return (
    <div className="min-h-full bg-background">
      <header className="sticky top-0 z-10 border-b border-border bg-background/85 backdrop-blur">
        <div className="mx-auto flex max-w-[1320px] flex-wrap items-center gap-400 px-500 py-300">
          <div className="flex items-center gap-300">
            <div className="grid size-[36px] place-items-center rounded-xl bg-primary text-primary-foreground">
              <Fingerprint className="icon-size-300" aria-hidden />
            </div>
            <div className="leading-tight">
              <p className="font-heading text-400 font-semibold">Trust Ledger</p>
              <p className="font-monospace text-100 uppercase tracking-widest text-muted-foreground">Legacy Defuse Kit · de_dust2</p>
            </div>
          </div>

          <nav aria-label="Sections" className="order-3 flex w-full gap-100 overflow-x-auto md:order-none md:ml-600 md:w-auto">
            {TABS.map(({ id, label, icon: Icon }) => (
              <button
                key={id}
                type="button"
                onClick={() => setTab(id)}
                aria-current={tab === id ? 'page' : undefined}
                className={cn(
                  'flex items-center gap-200 rounded-full px-400 py-200 text-300 font-medium transition-colors',
                  'focus-visible:outline-2 focus-visible:outline-ring',
                  tab === id ? 'bg-foreground text-background' : 'text-muted-foreground hover:bg-accent hover:text-foreground'
                )}
              >
                <Icon className="icon-size-200" aria-hidden />
                {label}
              </button>
            ))}
          </nav>

          <div className="ml-auto flex items-center gap-300">
            {run && (
              <span className="hidden items-center gap-200 rounded-full border border-border px-300 py-100 text-200 sm:flex">
                <Dot tone={toneOf(run.status)} className="animate-pulse" />
                <span className="font-medium">{run.status}</span>
                <span className="text-muted-foreground">{fmtAgo(run.startedAt)}</span>
              </span>
            )}
            <IconButton label="Refresh data" onClick={() => setRefreshKey((k) => k + 1)}>
              <RefreshCw className={cn('icon-size-200', latest.loading && 'animate-spin')} aria-hidden />
            </IconButton>
            <IconButton label={isDark ? 'Switch to light mode' : 'Switch to dark mode'} onClick={toggleTheme}>
              {isDark ? <Sun className="icon-size-200" aria-hidden /> : <Moon className="icon-size-200" aria-hidden />}
            </IconButton>
          </div>
        </div>
      </header>

      <main className="mx-auto max-w-[1320px] px-500 py-600">
        {tab === 'overview' && <Overview refreshKey={refreshKey} />}
        {tab === 'runs' && <Runs refreshKey={refreshKey} />}
        {tab === 'reconciliation' && <Reconciliation refreshKey={refreshKey} />}
        {tab === 'quality' && <Quality refreshKey={refreshKey} />}
        <footer className="mt-800 flex flex-wrap justify-between gap-200 font-monospace text-200 text-muted-foreground">
          <span>Source: lh_trust · read-only · your own Fabric permissions</span>
          <span>Bronze = Silver + quarantined + merged</span>
        </footer>
      </main>
    </div>
  );
}

function IconButton({ label, onClick, children }: { label: string; onClick: () => void; children: ReactNode }) {
  return (
    <button
      type="button"
      aria-label={label}
      title={label}
      onClick={onClick}
      className="grid size-[36px] place-items-center rounded-full border border-border text-foreground transition-colors hover:bg-accent focus-visible:outline-2 focus-visible:outline-ring"
    >
      {children}
    </button>
  );
}

export default App;
