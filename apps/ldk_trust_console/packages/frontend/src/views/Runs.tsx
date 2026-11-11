//-----------------------------------------------------------------------
// Runs: every audited pipeline run, with a step timeline and errors.
//-----------------------------------------------------------------------

import { useState } from 'react';

import { Card, Dot, Empty, Loader, Pill, Skeleton } from '@/components/ui';
import { toneDot } from '@/lib/tone';
import { useQuery } from '@/hooks/use-query';
import { fmtAgo, fmtDuration, fmtInt, fmtTime, shortId, toneOf } from '@/lib/format';
import { fetchActivities, fetchRuns, type ActivityRun, type PipelineRun } from '@/lib/trust-api';
import { cn } from '@/lib/utils';

export function Runs({ refreshKey }: { refreshKey: number }) {
  const runs = useQuery(() => fetchRuns(30), [refreshKey]);
  const [selected, setSelected] = useState<string | null>(null);

  return (
    <Loader
      state={runs}
      skeleton={<Skeleton className="h-[480px]" />}
      isEmpty={(d) => d.length === 0}
      empty="No audited pipeline runs yet."
    >
      {(d) => {
        const current = d.find((r) => r.runId === selected) ?? d[0];
        return (
          <div className="grid grid-cols-1 gap-500 lg:grid-cols-[minmax(260px,1fr)_2.4fr]">
            <Card eyebrow={`${d.length} runs`} title="History" className="p-300 lg:p-400">
              <ul className="flex flex-col gap-100">
                {d.map((r) => (
                  <li key={r.runId}>
                    <button
                      type="button"
                      onClick={() => setSelected(r.runId)}
                      aria-current={r.runId === current.runId}
                      className={cn(
                        'flex w-full items-center gap-300 rounded-xl px-300 py-200 text-left transition-colors',
                        'hover:bg-accent focus-visible:outline-2 focus-visible:outline-ring',
                        r.runId === current.runId && 'bg-accent'
                      )}
                    >
                      <Dot tone={toneOf(r.status)} />
                      <span className="flex-1">
                        <span className="block text-300 font-semibold">{fmtTime(r.startedAt)}</span>
                        <span className="block font-monospace text-200 text-muted-foreground">{shortId(r.runId)}</span>
                      </span>
                      <span className="font-numeric text-300 tabular-nums text-muted-foreground">{fmtDuration(r.durationS)}</span>
                    </button>
                  </li>
                ))}
              </ul>
            </Card>
            <RunDetail run={current} refreshKey={refreshKey} />
          </div>
        );
      }}
    </Loader>
  );
}

function RunDetail({ run, refreshKey }: { run: PipelineRun; refreshKey: number }) {
  const acts = useQuery(() => fetchActivities(run.runId), [run.runId, refreshKey]);
  const tone = toneOf(run.status);

  return (
    <Card
      eyebrow={`Run ${run.runId}`}
      title={
        <span className="flex flex-wrap items-center gap-300">
          {fmtTime(run.startedAt)}
          <Pill tone={tone}>{run.status}</Pill>
        </span>
      }
      action={<span className="text-200 text-muted-foreground">{fmtAgo(run.startedAt)}</span>}
    >
      <dl className="mb-600 grid grid-cols-2 gap-400 sm:grid-cols-4">
        <Fact label="Duration" value={fmtDuration(run.durationS)} />
        <Fact label="Ledger rows" value={fmtInt(run.ledgerRows)} />
        <Fact label="Recon checks" value={fmtInt(run.reconcileChecks)} />
        <Fact label="Recon failed" value={fmtInt(run.reconcileFail)} bad={(run.reconcileFail ?? 0) > 0} />
      </dl>

      <Loader state={acts} skeleton={<Skeleton className="h-[260px]" />} isEmpty={(d) => d.length === 0} empty="No activity rows for this run.">
        {(d) => <Timeline acts={d} />}
      </Loader>
    </Card>
  );
}

function Fact({ label, value, bad }: { label: string; value: string; bad?: boolean }) {
  return (
    <div>
      <dt className="font-monospace text-200 uppercase tracking-widest text-muted-foreground">{label}</dt>
      <dd className={cn('mt-100 font-numeric text-500 font-semibold tabular-nums', bad && 'text-bad')}>{value}</dd>
    </div>
  );
}

/** Gantt-style timeline of the pipeline steps, positioned on the run's own clock. */
function Timeline({ acts }: { acts: ActivityRun[] }) {
  const ran = acts.filter((a) => a.startedAt);
  if (!ran.length) return <Empty>No step ran in this run.</Empty>;
  const t0 = Math.min(...ran.map((a) => a.startedAt!.getTime()));
  const t1 = Math.max(...ran.map((a) => (a.endedAt ?? a.startedAt!).getTime()));
  const span = Math.max(1, t1 - t0);
  const errors = acts.filter((a) => a.errorMessage);

  return (
    <div className="flex flex-col gap-500">
      <ol className="flex flex-col gap-300">
        {acts.map((a) => {
          const tone = toneOf(a.status);
          const start = a.startedAt ? ((a.startedAt.getTime() - t0) / span) * 100 : 0;
          const width = a.startedAt
            ? Math.max(1.5, (((a.endedAt ?? a.startedAt).getTime() - a.startedAt.getTime()) / span) * 100)
            : 0;
          return (
            <li key={a.activityName} className="grid grid-cols-[minmax(120px,170px)_1fr_auto] items-center gap-300">
              <span className="truncate font-monospace text-300" title={a.activityType}>
                {a.activityName}
              </span>
              <div className="relative h-[22px] rounded-md bg-muted">
                {a.startedAt ? (
                  <div
                    className={cn('absolute top-0 h-full rounded-md', toneDot[tone])}
                    style={{ left: `${start}%`, width: `${Math.min(width, 100 - start)}%` }}
                    title={`${a.status} · ${fmtDuration(a.durationS)}`}
                  />
                ) : (
                  <span className="absolute inset-0 grid place-items-center font-monospace text-200 text-muted-foreground">
                    did not run
                  </span>
                )}
              </div>
              <span className="w-[72px] text-right font-numeric text-300 tabular-nums">
                {a.startedAt ? fmtDuration(a.durationS) : '—'}
              </span>
            </li>
          );
        })}
      </ol>

      {errors.length > 0 && (
        <div className="flex flex-col gap-300">
          <p className="font-monospace text-200 uppercase tracking-widest text-muted-foreground">Errors</p>
          {errors.map((a) => (
            <div key={a.activityName} className="rounded-xl border border-bad/40 bg-bad-soft p-400">
              <p className="font-monospace text-300 font-semibold text-bad">
                {a.activityName}
                {a.errorCode && ` · ${a.errorCode}`}
              </p>
              <p className="mt-100 line-clamp-4 text-300 break-words">{a.errorMessage}</p>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
