//-----------------------------------------------------------------------
// Overview: is the last run green, and did every row arrive?
//-----------------------------------------------------------------------

import { ShieldAlert, ShieldCheck, ShieldQuestion } from 'lucide-react';

import { Card, Kpi, Loader, Pill, Skeleton } from '@/components/ui';
import { toneDot, toneSoft, toneText } from '@/lib/tone';
import { useQuery } from '@/hooks/use-query';
import {
  entityLabel,
  fmtAgo,
  fmtDuration,
  fmtInt,
  fmtTime,
  fmtUsd,
  shortId,
  toneOf,
  type Tone,
} from '@/lib/format';
import { fetchLatestSummary, fetchRuns, type EntitySummary, type PipelineRun } from '@/lib/trust-api';
import { cn } from '@/lib/utils';

const ENTITY_ORDER = [
  'cost', 'budget', 'cmdb_assets', 'cmdb_asset_groups', 'license_products', 'license_entitlements',
  'license_installations', 'vm_backup_jobs', 'vm_backup_storage',
];

export function Overview({ refreshKey }: { refreshKey: number }) {
  const runs = useQuery(() => fetchRuns(20), [refreshKey]);
  const summary = useQuery(fetchLatestSummary, [refreshKey]);

  return (
    <div className="grid grid-cols-1 gap-500 lg:grid-cols-3">
      <Card className="lg:col-span-2" eyebrow="Latest pipeline run" title="one_pipeline_to_rule_them_all">
        <Loader
          state={runs}
          skeleton={<Skeleton className="h-[220px]" />}
          isEmpty={(d) => d.length === 0}
          empty="No audited pipeline runs yet. Run one_pipeline_to_rule_them_all once and refresh."
        >
          {(d) => <LatestRun runs={d} />}
        </Loader>
      </Card>

      <Card eyebrow="Latest reconciliation" title="Nothing lost?">
        <Loader state={summary} skeleton={<Skeleton className="h-[220px]" />} isEmpty={(d) => d.length === 0} empty="No reconciliation yet.">
          {(d) => <Totals rows={d} />}
        </Loader>
      </Card>

      <Card
        className="lg:col-span-3"
        eyebrow="Chain of custody"
        title="Source → Bronze → Silver, per entity"
        action={<Legend />}
      >
        <Loader state={summary} skeleton={<Skeleton className="h-[360px]" />} isEmpty={(d) => d.length === 0} empty="No reconciliation yet.">
          {(d) => <Custody rows={d} />}
        </Loader>
      </Card>
    </div>
  );
}

function LatestRun({ runs }: { runs: PipelineRun[] }) {
  const run = runs[0];
  const tone = toneOf(run.status);
  const Icon = tone === 'ok' ? ShieldCheck : tone === 'bad' ? ShieldAlert : ShieldQuestion;
  const problems = [run.failedSteps && `failed: ${run.failedSteps}`, run.skippedSteps && `skipped: ${run.skippedSteps}`]
    .filter(Boolean)
    .join(' · ');
  const checks = run.reconcileChecks ?? 0;
  const failChecks = run.reconcileFail ?? 0;

  return (
    <div className="flex flex-col gap-600">
      <div className="flex items-center gap-500">
        <div className={cn('grid size-[72px] shrink-0 place-items-center rounded-3xl', toneSoft[tone])}>
          <Icon className={cn('icon-size-700', toneText[tone])} aria-hidden />
        </div>
        <div className="min-w-0">
          <p className="font-heading text-hero-700 leading-hero-700 font-semibold">
            {tone === 'ok' ? 'All green' : tone === 'bad' ? 'Run failed' : 'Run incomplete'}
          </p>
          <p className="text-300 text-muted-foreground">
            {tone === 'ok'
              ? `Every step succeeded · ${fmtInt(checks)} reconciliation checks, ${fmtInt(failChecks)} failed`
              : problems || `${fmtInt(failChecks)} of ${fmtInt(checks)} reconciliation checks failed`}
          </p>
        </div>
      </div>

      <dl className="grid grid-cols-2 gap-400 sm:grid-cols-4">
        <Meta label="Started" value={fmtTime(run.startedAt)} sub={fmtAgo(run.startedAt)} />
        <Meta label="Duration" value={fmtDuration(run.durationS)} />
        <Meta label="Ledger rows" value={fmtInt(run.ledgerRows)} sub="written by this run" />
        <Meta label="Run id" value={shortId(run.runId)} sub={run.triggerType || 'manual'} mono />
      </dl>

      <History runs={runs} />
    </div>
  );
}

function Meta({ label, value, sub, mono }: { label: string; value: string; sub?: string; mono?: boolean }) {
  return (
    <div>
      <dt className="font-monospace text-200 uppercase tracking-widest text-muted-foreground">{label}</dt>
      <dd className={cn('mt-100 text-400 font-semibold', mono ? 'font-monospace' : 'font-numeric')}>{value}</dd>
      {sub && <dd className="text-200 text-muted-foreground">{sub}</dd>}
    </div>
  );
}

/** Flight-recorder strip: one bar per run, height = duration, colour = status. Oldest left. */
function History({ runs }: { runs: PipelineRun[] }) {
  const ordered = [...runs].reverse();
  const max = Math.max(1, ...ordered.map((r) => r.durationS ?? 0));
  return (
    <div>
      <p className="mb-200 font-monospace text-200 uppercase tracking-widest text-muted-foreground">
        Last {ordered.length} runs
      </p>
      <div className="flex h-[56px] items-end gap-100" role="list">
        {ordered.map((r) => {
          const tone = toneOf(r.status);
          return (
            <div
              key={r.runId}
              role="listitem"
              title={`${fmtTime(r.startedAt)} · ${r.status} · ${fmtDuration(r.durationS)}`}
              className={cn('min-w-[10px] flex-1 rounded-md transition-opacity hover:opacity-70', toneDot[tone])}
              style={{ height: `${Math.max(12, ((r.durationS ?? 0) / max) * 100)}%` }}
            />
          );
        })}
      </div>
    </div>
  );
}

function Totals({ rows }: { rows: EntitySummary[] }) {
  const sum = (f: (r: EntitySummary) => number | null) => rows.reduce((a, r) => a + (f(r) ?? 0), 0);
  const passed = rows.filter((r) => toneOf(r.status) === 'ok').length;
  const lost = sum((r) => r.lostRows);
  const fail = sum((r) => r.checksFail);
  const pending = sum((r) => r.checksPending);
  return (
    <div className="flex h-full flex-col gap-500">
      <Kpi
        label="Entities reconciled"
        value={`${passed}/${rows.length}`}
        tone={passed === rows.length ? 'ok' : 'warn'}
        sub={`checked ${fmtAgo(rows[0].checkedAt)}`}
      />
      <Kpi label="Rows lost" value={fmtInt(lost)} tone={lost === 0 ? 'ok' : 'bad'} sub="Bronze = Silver + quarantined + merged" />
      <div className="grid grid-cols-2 gap-400">
        <Kpi label="Quarantined" value={fmtInt(sum((r) => r.quarantinedRows))} sub="kept, with a reason" />
        <Kpi label="Merged" value={fmtInt(sum((r) => r.mergedRows))} sub="duplicates, logged" />
      </div>
      <p className="text-200 text-muted-foreground">
        {fmtInt(sum((r) => r.checksPass))} checks passed · {fmtInt(fail)} failed · {fmtInt(pending)} pending
      </p>
    </div>
  );
}

function Legend() {
  const item = (cls: string, label: string) => (
    <span className="inline-flex items-center gap-100">
      <span className={cn('size-[10px] rounded-sm', cls)} aria-hidden />
      {label}
    </span>
  );
  return (
    <div className="hidden flex-wrap gap-300 text-200 text-muted-foreground md:flex">
      {item('bg-ok', 'in Silver')}
      {item('bg-info', 'merged duplicate')}
      {item('bg-warn', 'quarantined')}
      {item('bg-bad', 'lost')}
    </div>
  );
}

function Custody({ rows }: { rows: EntitySummary[] }) {
  const sorted = [...rows].sort((a, b) => ENTITY_ORDER.indexOf(a.entity) - ENTITY_ORDER.indexOf(b.entity));
  return (
    <ul className="divide-y divide-border">
      {sorted.map((r) => (
        <CustodyRow key={r.entity} r={r} />
      ))}
    </ul>
  );
}

function CustodyRow({ r }: { r: EntitySummary }) {
  const bronze = r.bronzeRows ?? 0;
  const pct = (n: number | null) => (bronze > 0 ? ((n ?? 0) / bronze) * 100 : 0);
  const lost = r.lostRows ?? 0;
  const amountOk =
    r.silverAmount !== null && r.goldAmount !== null ? Math.abs(r.silverAmount - r.goldAmount) < 0.01 : null;
  const sealTone: Tone = lost === 0 ? 'ok' : 'bad';

  return (
    <li className="grid grid-cols-1 items-center gap-300 py-400 md:grid-cols-[minmax(180px,1.1fr)_minmax(220px,1.3fr)_2fr_auto]">
      <div className="flex items-center gap-200">
        <span className="font-semibold">{entityLabel(r.entity)}</span>
        <Pill tone={toneOf(r.status)}>{r.status}</Pill>
      </div>

      <div className="flex items-baseline gap-200 font-numeric text-300 tabular-nums">
        <Step label="source" value={r.sourceRows} />
        <span className="text-muted-foreground">→</span>
        <Step label="bronze" value={r.bronzeRows} />
        <span className="text-muted-foreground">→</span>
        <Step label="silver" value={r.silverRows} strong />
      </div>

      <div>
        <div className="flex h-[12px] w-full overflow-hidden rounded-full bg-muted" aria-label="Where every Bronze row went">
          <div className="bg-ok" style={{ width: `${pct(r.silverRows)}%` }} />
          <div className="bg-info" style={{ width: `${pct(r.mergedRows)}%` }} />
          <div className="bg-warn" style={{ width: `${pct(r.quarantinedRows)}%` }} />
          <div className="bg-bad" style={{ width: `${pct(lost)}%` }} />
        </div>
        <p className="mt-100 text-200 text-muted-foreground">
          {fmtInt(r.mergedRows)} merged · {fmtInt(r.quarantinedRows)} quarantined
          {amountOk !== null && (
            <>
              {' · '}
              <span className={amountOk ? 'text-ok' : 'text-bad'}>
                {fmtUsd(r.silverAmount)} {amountOk ? '= Gold' : `≠ Gold ${fmtUsd(r.goldAmount)}`}
              </span>
            </>
          )}
        </p>
      </div>

      <div
        className={cn(
          'justify-self-start rounded-full border px-300 py-100 font-monospace text-200 font-semibold md:justify-self-end',
          sealTone === 'ok' ? 'border-ok/40 text-ok' : 'border-bad/40 text-bad'
        )}
      >
        {fmtInt(lost)} lost
      </div>
    </li>
  );
}

function Step({ label, value, strong }: { label: string; value: number | null; strong?: boolean }) {
  return (
    <span className="flex flex-col">
      <span className={cn(strong && 'font-semibold')}>{fmtInt(value)}</span>
      <span className="font-monospace text-100 uppercase tracking-widest text-muted-foreground">{label}</span>
    </span>
  );
}
