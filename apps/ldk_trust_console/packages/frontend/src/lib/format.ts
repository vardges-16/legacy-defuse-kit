//-----------------------------------------------------------------------
// Display helpers: numbers, money, durations, times and status tones.
//-----------------------------------------------------------------------

const intFmt = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 });
const usdFmt = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });

export const fmtInt = (n: number | null | undefined) => (n === null || n === undefined ? '—' : intFmt.format(n));
export const fmtUsd = (n: number | null | undefined) => (n === null || n === undefined ? '—' : usdFmt.format(n));
export const fmtPct = (n: number | null | undefined) => (n === null || n === undefined ? '—' : `${Math.round(n * 100)}%`);

export function fmtDuration(seconds: number | null | undefined): string {
  if (seconds === null || seconds === undefined) return '—';
  const s = Math.round(seconds);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${String(s % 60).padStart(2, '0')}s`;
  return `${Math.floor(m / 60)}h ${String(m % 60).padStart(2, '0')}m`;
}

export function fmtTime(d: Date | null | undefined): string {
  if (!d) return '—';
  return d.toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
}

export function fmtAgo(d: Date | null | undefined): string {
  if (!d) return '—';
  const s = Math.max(0, (Date.now() - d.getTime()) / 1000);
  if (s < 90) return 'just now';
  if (s < 3600) return `${Math.round(s / 60)} min ago`;
  if (s < 86400) return `${Math.round(s / 3600)} h ago`;
  return `${Math.round(s / 86400)} d ago`;
}

export const shortId = (id: string) => (id.length > 12 ? `${id.slice(0, 8)}…` : id);

export type Tone = 'ok' | 'warn' | 'bad' | 'idle';

/** Maps ledger and pipeline statuses (PASS, Succeeded, FAIL, Failed, PENDING, Skipped …) to a tone. */
export function toneOf(status: string): Tone {
  const s = status.toLowerCase();
  if (s === 'pass' || s === 'succeeded') return 'ok';
  if (s === 'fail' || s === 'failed' || s === 'cancelled') return 'bad';
  if (s === 'pending' || s === 'skipped' || s === 'inprogress' || s === 'queued') return 'warn';
  return 'idle';
}

/** Friendly entity labels for the nine reconciled entities. */
export function entityLabel(entity: string): string {
  const map: Record<string, string> = {
    cost: 'Cloud cost (FOCUS)',
    budget: 'Budget',
    cmdb_assets: 'CMDB assets',
    cmdb_asset_groups: 'CMDB asset groups',
    vm_backup_jobs: 'VM backup jobs',
    vm_backup_storage: 'VM backup storage',
    license_products: 'License products',
    license_entitlements: 'License entitlements',
    license_installations: 'License installations',
  };
  return map[entity] ?? entity.replace(/_/g, ' ');
}
