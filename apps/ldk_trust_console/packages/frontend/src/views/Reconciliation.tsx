//-----------------------------------------------------------------------
// Reconciliation: every check of the latest run, grouped by entity.
//-----------------------------------------------------------------------

import { useState } from 'react';

import { Card, Loader, Pill, Skeleton } from '@/components/ui';
import { useQuery } from '@/hooks/use-query';
import { entityLabel, fmtInt, fmtTime, shortId, toneOf } from '@/lib/format';
import { fetchChecks, fetchLatestSummary, type Check } from '@/lib/trust-api';
import { cn } from '@/lib/utils';

type Filter = 'ALL' | 'PASS' | 'FAIL' | 'PENDING';

async function load() {
  const summary = await fetchLatestSummary();
  if (!summary.length) return { runId: '', checkedAt: null, checks: [] as Check[] };
  return { runId: summary[0].runId, checkedAt: summary[0].checkedAt, checks: await fetchChecks(summary[0].runId) };
}

export function Reconciliation({ refreshKey }: { refreshKey: number }) {
  const state = useQuery(load, [refreshKey]);
  const [filter, setFilter] = useState<Filter>('ALL');

  return (
    <Loader state={state} skeleton={<Skeleton className="h-[520px]" />} isEmpty={(d) => d.checks.length === 0} empty="No reconciliation checks yet.">
      {(d) => {
        const counts = { ALL: d.checks.length, PASS: 0, FAIL: 0, PENDING: 0 } as Record<Filter, number>;
        for (const c of d.checks) if (c.status in counts) counts[c.status as Filter] += 1;
        const shown = filter === 'ALL' ? d.checks : d.checks.filter((c) => c.status === filter);
        const groups = new Map<string, Check[]>();
        for (const c of shown) groups.set(c.entity, [...(groups.get(c.entity) ?? []), c]);

        return (
          <div className="flex flex-col gap-500">
            <div className="flex flex-wrap items-end justify-between gap-400">
              <div>
                <p className="font-monospace text-200 uppercase tracking-widest text-muted-foreground">
                  Run {shortId(d.runId)} · checked {fmtTime(d.checkedAt)}
                </p>
                <h2 className="font-heading text-hero-700 leading-hero-700 font-semibold">
                  {counts.FAIL === 0 ? 'Every number ties out' : `${counts.FAIL} check${counts.FAIL > 1 ? 's' : ''} failing`}
                </h2>
              </div>
              <div role="tablist" aria-label="Filter checks" className="flex gap-100 rounded-full border border-border bg-card p-100">
                {(['ALL', 'PASS', 'FAIL', 'PENDING'] as Filter[]).map((f) => (
                  <button
                    key={f}
                    type="button"
                    role="tab"
                    aria-selected={filter === f}
                    onClick={() => setFilter(f)}
                    className={cn(
                      'rounded-full px-300 py-100 font-monospace text-200 font-medium transition-colors',
                      'focus-visible:outline-2 focus-visible:outline-ring',
                      filter === f ? 'bg-primary text-primary-foreground' : 'text-muted-foreground hover:bg-accent'
                    )}
                  >
                    {f} {counts[f]}
                  </button>
                ))}
              </div>
            </div>

            <div className="grid grid-cols-1 gap-500 xl:grid-cols-2">
              {[...groups.entries()].map(([entity, checks]) => (
                <Card key={entity} title={entityLabel(entity)} eyebrow={entity}>
                  <table className="w-full text-300">
                    <thead>
                      <tr className="text-left font-monospace text-200 uppercase tracking-widest text-muted-foreground">
                        <th className="pb-200 font-medium">Check</th>
                        <th className="pb-200 text-right font-medium">Left</th>
                        <th className="pb-200 text-right font-medium">Right</th>
                        <th className="pb-200 text-right font-medium">Status</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-border">
                      {checks.map((c) => (
                        <tr key={c.checkName} className="align-top">
                          <td className="py-200 pr-300">
                            <span className="font-medium">{c.checkName.replace(/^\d+\s*/, '')}</span>
                            {c.note && <span className="block text-200 text-muted-foreground">{c.note}</span>}
                          </td>
                          <td className="py-200 text-right font-numeric tabular-nums">
                            {fmtNum(c.leftValue)}
                            <span className="block font-monospace text-100 uppercase text-muted-foreground">{c.leftLayer}</span>
                          </td>
                          <td className="py-200 text-right font-numeric tabular-nums">
                            {fmtNum(c.rightValue)}
                            <span className="block font-monospace text-100 uppercase text-muted-foreground">{c.rightLayer}</span>
                          </td>
                          <td className="py-200 text-right">
                            <Pill tone={toneOf(c.status)}>{c.status}</Pill>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </Card>
              ))}
            </div>
          </div>
        );
      }}
    </Loader>
  );
}

/** Row counts as integers, amounts with two decimals. */
function fmtNum(n: number | null): string {
  if (n === null) return '—';
  return Number.isInteger(n) ? fmtInt(n) : n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}
