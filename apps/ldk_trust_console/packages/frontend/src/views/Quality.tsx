//-----------------------------------------------------------------------
// Quality: freshness of every table, quarantine reasons, gaps, AI scorecard.
//-----------------------------------------------------------------------

import { Card, Loader, Pill, Skeleton } from '@/components/ui';
import { useQuery } from '@/hooks/use-query';
import { entityLabel, fmtAgo, fmtInt, fmtPct, toneOf } from '@/lib/format';
import { fetchGaps, fetchLatestLoads, fetchQuarantine, fetchScorecard, type Score } from '@/lib/trust-api';
import { cn } from '@/lib/utils';

export function Quality({ refreshKey }: { refreshKey: number }) {
  const loads = useQuery(fetchLatestLoads, [refreshKey]);
  const quarantine = useQuery(fetchQuarantine, [refreshKey]);
  const gaps = useQuery(fetchGaps, [refreshKey]);
  const score = useQuery(fetchScorecard, [refreshKey]);

  return (
    <div className="grid grid-cols-1 gap-500 lg:grid-cols-5">
      <Card className="lg:col-span-3" eyebrow="Freshness" title="Last load of every table">
        <Loader state={loads} skeleton={<Skeleton className="h-[360px]" />} isEmpty={(d) => d.length === 0} empty="No loads logged yet.">
          {(d) => (
            <div className="overflow-x-auto">
              <table className="w-full text-300">
                <thead>
                  <tr className="text-left font-monospace text-200 uppercase tracking-widest text-muted-foreground">
                    <th className="pb-200 font-medium">Table</th>
                    <th className="pb-200 text-right font-medium">In</th>
                    <th className="pb-200 text-right font-medium">Out</th>
                    <th className="pb-200 text-right font-medium">Held</th>
                    <th className="pb-200 text-right font-medium">Loaded</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border">
                  {[...d]
                    .sort((a, b) => a.layer.localeCompare(b.layer) || a.entity.localeCompare(b.entity))
                    .map((r) => {
                      const ok = r.rowsIn === null || r.rowsIn === (r.rowsOut ?? 0) + (r.quarantined ?? 0) + (r.merged ?? 0);
                      return (
                        <tr key={`${r.layer}|${r.entity}`}>
                          <td className="py-200 pr-300">
                            <span className="mr-200 rounded-md bg-muted px-100 py-100-nudge font-monospace text-100 uppercase text-muted-foreground">
                              {r.layer}
                            </span>
                            <span className="font-medium">{r.entity}</span>
                          </td>
                          <td className="py-200 text-right font-numeric tabular-nums">{fmtInt(r.rowsIn)}</td>
                          <td className={cn('py-200 text-right font-numeric tabular-nums', !ok && 'text-bad')}>{fmtInt(r.rowsOut)}</td>
                          <td className="py-200 text-right font-numeric tabular-nums text-muted-foreground">
                            {fmtInt((r.quarantined ?? 0) + (r.merged ?? 0))}
                          </td>
                          <td className="py-200 text-right text-200 text-muted-foreground" title={r.loggedAt?.toLocaleString()}>
                            {fmtAgo(r.loggedAt)}
                          </td>
                        </tr>
                      );
                    })}
                </tbody>
              </table>
            </div>
          )}
        </Loader>
      </Card>

      <div className="flex flex-col gap-500 lg:col-span-2">
        <Card eyebrow="Quarantine" title="Why rows were held back">
          <Loader state={quarantine} skeleton={<Skeleton className="h-[200px]" />} isEmpty={(d) => d.length === 0} empty="Nothing quarantined.">
            {(d) => {
              const max = Math.max(...d.map((x) => x.rows));
              return (
                <ul className="flex flex-col gap-300">
                  {d.map((x) => (
                    <li key={`${x.entity}|${x.reasonCode}`}>
                      <div className="mb-100 flex justify-between gap-300 text-300">
                        <span>
                          <span className="font-monospace font-medium">{x.reasonCode}</span>
                          <span className="text-muted-foreground"> · {entityLabel(x.entity)}</span>
                        </span>
                        <span className="font-numeric tabular-nums">{fmtInt(x.rows)}</span>
                      </div>
                      <div className="h-[6px] rounded-full bg-muted">
                        <div className="h-full rounded-full bg-warn" style={{ width: `${(x.rows / max) * 100}%` }} />
                      </div>
                    </li>
                  ))}
                </ul>
              );
            }}
          </Loader>
        </Card>

        <Card eyebrow="Completeness" title="Known gaps in delivered data">
          <Loader state={gaps} skeleton={<Skeleton className="h-[120px]" />} isEmpty={(d) => d.length === 0} empty="No gaps detected.">
            {(d) => (
              <ul className="flex flex-wrap gap-200">
                {d.map((g) => (
                  <li
                    key={`${g.entity}|${g.expectedKey}`}
                    title={g.detail}
                    className="rounded-lg border border-warn/40 bg-warn-soft px-200 py-100 font-monospace text-200 text-warn"
                  >
                    {g.expectedKey} <span className="opacity-70">· {g.entity}</span>
                  </li>
                ))}
              </ul>
            )}
          </Loader>
        </Card>
      </div>

      <Card className="lg:col-span-5" eyebrow="AI cleansing" title="Scorecard against the answer keys">
        <Loader state={score} skeleton={<Skeleton className="h-[220px]" />} isEmpty={(d) => d.length === 0} empty="No scoring run yet.">
          {(d) => <Scorecard rows={d} />}
        </Loader>
      </Card>
    </div>
  );
}

function Scorecard({ rows }: { rows: Score[] }) {
  const totals = rows.filter((r) => r.method === 'ALL');
  const split = (r: Score) => {
    const parts = rows.filter((x) => x.system === r.system && x.defectId === r.defectId && x.method !== 'ALL');
    return parts.map((p) => `${p.method.split(' ')[0]} ${fmtInt(p.cases)}`).join(' · ');
  };
  return (
    <div className="grid grid-cols-1 gap-400 sm:grid-cols-2 xl:grid-cols-5">
      {(totals.length ? totals : rows).map((r) => {
        const f1 = r.f1 ?? 0;
        return (
          <div key={`${r.system}|${r.defectId}|${r.method}`} className="rounded-xl border border-border p-400">
            <div className="flex items-center justify-between gap-200">
              <span className="font-monospace text-300 font-semibold">{r.defectId}</span>
              <Pill tone={toneOf(f1 >= 0.95 ? 'pass' : f1 >= 0.8 ? 'pending' : 'fail')}>{r.system}</Pill>
            </div>
            <p className="mt-300 font-numeric text-hero-800 leading-hero-800 font-semibold tabular-nums">{fmtPct(r.f1)}</p>
            <p className="text-200 text-muted-foreground">F1 · {fmtInt(r.correct)}/{fmtInt(r.cases)} correct</p>
            <div className="mt-300 h-[6px] rounded-full bg-muted">
              <div className="h-full rounded-full bg-ok" style={{ width: `${f1 * 100}%` }} />
            </div>
            <p className="mt-200 font-monospace text-200 text-muted-foreground">
              P {fmtPct(r.precision)} · R {fmtPct(r.recall)}
            </p>
            {split(r) && <p className="font-monospace text-200 text-muted-foreground">{split(r)}</p>}
          </div>
        );
      })}
    </div>
  );
}
