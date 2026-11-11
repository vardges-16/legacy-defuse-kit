//-----------------------------------------------------------------------
// Read-only queries against the Trust Ledger (lh_trust). Every query names its
// columns and bounds its row count; rows are mapped into plain view models so
// the UI never depends on raw connector shapes.
//-----------------------------------------------------------------------

import { getRayfinClient } from './rayfin-client';

type Rec = Record<string, unknown>;
const rec = (r: unknown) => r as Rec;

function str(v: unknown): string {
  return v === null || v === undefined ? '' : String(v);
}
function num(v: unknown): number | null {
  if (v === null || v === undefined || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}
function date(v: unknown): Date | null {
  if (v === null || v === undefined || v === '') return null;
  const d = v instanceof Date ? v : new Date(String(v));
  return Number.isNaN(d.getTime()) ? null : d;
}

async function trust() {
  const client = await getRayfinClient();
  return client.connectors.trust;
}

/** Keeps, per key, only the rows of the most recent run (rows must be sorted newest first). */
function latestRunPerKey<T extends { runId: string }>(rows: T[], key: (r: T) => string): T[] {
  const latest = new Map<string, string>();
  for (const r of rows) if (!latest.has(key(r))) latest.set(key(r), r.runId);
  return rows.filter((r) => latest.get(key(r)) === r.runId);
}

// ---------------------------------------------------------------- pipeline runs

export interface PipelineRun {
  runId: string;
  pipelineName: string;
  triggerType: string;
  startedAt: Date | null;
  endedAt: Date | null;
  durationS: number | null;
  status: string;
  failedSteps: string;
  skippedSteps: string;
  ledgerRows: number | null;
  reconcileChecks: number | null;
  reconcileFail: number | null;
}

export async function fetchRuns(limit = 30): Promise<PipelineRun[]> {
  const t = await trust();
  const rows = await t.MetaPipelineRuns.select([
    'runId', 'pipelineName', 'triggerType', 'startedAt', 'endedAt', 'durationS', 'status',
    'failedSteps', 'skippedSteps', 'ledgerRows', 'reconcileChecks', 'reconcileFail',
  ])
    .orderBy({ startedAt: 'desc' })
    .first(limit)
    .execute();
  return rows.map(rec).map((r) => ({
    runId: str(r.runId),
    pipelineName: str(r.pipelineName),
    triggerType: str(r.triggerType),
    startedAt: date(r.startedAt),
    endedAt: date(r.endedAt),
    durationS: num(r.durationS),
    status: str(r.status),
    failedSteps: str(r.failedSteps),
    skippedSteps: str(r.skippedSteps),
    ledgerRows: num(r.ledgerRows),
    reconcileChecks: num(r.reconcileChecks),
    reconcileFail: num(r.reconcileFail),
  }));
}

export interface ActivityRun {
  activityName: string;
  activityType: string;
  status: string;
  startedAt: Date | null;
  endedAt: Date | null;
  durationS: number | null;
  errorCode: string;
  errorMessage: string;
}

export async function fetchActivities(runId: string): Promise<ActivityRun[]> {
  const t = await trust();
  const rows = await t.MetaPipelineActivityRuns.select([
    'activityName', 'activityType', 'status', 'startedAt', 'endedAt', 'durationS', 'errorCode', 'errorMessage',
  ])
    .where({ runId: { eq: runId } })
    .orderBy({ startedAt: 'asc' })
    .first(200)
    .execute();
  return rows.map(rec).map((r) => ({
    activityName: str(r.activityName),
    activityType: str(r.activityType),
    status: str(r.status),
    startedAt: date(r.startedAt),
    endedAt: date(r.endedAt),
    durationS: num(r.durationS),
    errorCode: str(r.errorCode),
    errorMessage: str(r.errorMessage),
  }));
}

// ---------------------------------------------------------------- reconciliation

export interface EntitySummary {
  runId: string;
  checkedAt: Date | null;
  entity: string;
  status: string;
  sourceRows: number | null;
  bronzeRows: number | null;
  silverRows: number | null;
  quarantinedRows: number | null;
  mergedRows: number | null;
  lostRows: number | null;
  bronzeAmount: number | null;
  silverAmount: number | null;
  goldAmount: number | null;
  checksPass: number;
  checksFail: number;
  checksPending: number;
}

/** Entity rows of the most recent reconciliation run. */
export async function fetchLatestSummary(): Promise<EntitySummary[]> {
  const t = await trust();
  const head = await t.MetaTrustSummary.select(['runId', 'checkedAt'])
    .orderBy({ checkedAt: 'desc' })
    .first(1)
    .execute();
  if (!head.length) return [];
  const runId = str(rec(head[0]).runId);
  const rows = await t.MetaTrustSummary.select([
    'runId', 'checkedAt', 'entity', 'status', 'sourceRows', 'bronzeRows', 'silverRows', 'quarantinedRows',
    'mergedRows', 'lostRows', 'bronzeAmount', 'silverAmount', 'goldAmount', 'checksPass', 'checksFail',
    'checksPending',
  ])
    .where({ runId: { eq: runId } })
    .first(100)
    .execute();
  return rows.map(rec).map((r) => ({
    runId: str(r.runId),
    checkedAt: date(r.checkedAt),
    entity: str(r.entity),
    status: str(r.status),
    sourceRows: num(r.sourceRows),
    bronzeRows: num(r.bronzeRows),
    silverRows: num(r.silverRows),
    quarantinedRows: num(r.quarantinedRows),
    mergedRows: num(r.mergedRows),
    lostRows: num(r.lostRows),
    bronzeAmount: num(r.bronzeAmount),
    silverAmount: num(r.silverAmount),
    goldAmount: num(r.goldAmount),
    checksPass: num(r.checksPass) ?? 0,
    checksFail: num(r.checksFail) ?? 0,
    checksPending: num(r.checksPending) ?? 0,
  }));
}

export interface Check {
  entity: string;
  checkName: string;
  leftLayer: string;
  leftValue: number | null;
  rightLayer: string;
  rightValue: number | null;
  difference: number | null;
  status: string;
  note: string;
}

export async function fetchChecks(runId: string): Promise<Check[]> {
  const t = await trust();
  const rows = await t.MetaReconciliation.select([
    'entity', 'checkName', 'leftLayer', 'leftValue', 'rightLayer', 'rightValue', 'difference', 'status', 'note',
  ])
    .where({ runId: { eq: runId } })
    .orderBy({ entity: 'asc' })
    .first(500)
    .execute();
  return rows.map(rec).map((r) => ({
    entity: str(r.entity),
    checkName: str(r.checkName),
    leftLayer: str(r.leftLayer),
    leftValue: num(r.leftValue),
    rightLayer: str(r.rightLayer),
    rightValue: num(r.rightValue),
    difference: num(r.difference),
    status: str(r.status),
    note: str(r.note),
  }));
}

// ---------------------------------------------------------------- data quality

export interface LoadStep {
  runId: string;
  layer: string;
  entity: string;
  targetTable: string;
  rowsIn: number | null;
  rowsOut: number | null;
  quarantined: number | null;
  merged: number | null;
  notebook: string;
  loggedAt: Date | null;
}

/** Latest load per layer + entity (freshness of every table the pipeline writes). */
export async function fetchLatestLoads(): Promise<LoadStep[]> {
  const t = await trust();
  const rows = await t.MetaLoadLedger.select([
    'runId', 'layer', 'entity', 'targetTable', 'rowsIn', 'rowCount', 'rowsQuarantined', 'rowsMerged',
    'notebook', 'loggedAt',
  ])
    .orderBy({ loggedAt: 'desc' })
    .first(1000)
    .execute();
  const seen = new Set<string>();
  const out: LoadStep[] = [];
  for (const r of rows.map(rec)) {
    const k = `${str(r.layer)}|${str(r.entity)}`;
    if (seen.has(k)) continue;
    seen.add(k);
    out.push({
      runId: str(r.runId),
      layer: str(r.layer),
      entity: str(r.entity),
      targetTable: str(r.targetTable),
      rowsIn: num(r.rowsIn),
      rowsOut: num(r.rowCount),
      quarantined: num(r.rowsQuarantined),
      merged: num(r.rowsMerged),
      notebook: str(r.notebook),
      loggedAt: date(r.loggedAt),
    });
  }
  return out;
}

export interface ReasonCount {
  entity: string;
  reasonCode: string;
  rows: number;
}

/** Quarantined rows by reason code, for each entity's most recent run. */
export async function fetchQuarantine(): Promise<ReasonCount[]> {
  const t = await trust();
  const rows = (
    await t.MetaQuarantine.select(['runId', 'entity', 'reasonCode', 'loggedAt'])
      .orderBy({ loggedAt: 'desc' })
      .first(5000)
      .execute()
  )
    .map(rec)
    .map((r) => ({ runId: str(r.runId), entity: str(r.entity), reasonCode: str(r.reasonCode) }));
  const counts = new Map<string, ReasonCount>();
  for (const r of latestRunPerKey(rows, (x) => x.entity)) {
    const k = `${r.entity}|${r.reasonCode}`;
    const c = counts.get(k) ?? { entity: r.entity, reasonCode: r.reasonCode, rows: 0 };
    c.rows += 1;
    counts.set(k, c);
  }
  return [...counts.values()].sort((a, b) => b.rows - a.rows);
}

export interface Gap {
  entity: string;
  layer: string;
  gapType: string;
  expectedKey: string;
  detail: string;
}

/** Completeness gaps (missing days, months or files) of each entity's most recent run. */
export async function fetchGaps(): Promise<Gap[]> {
  const t = await trust();
  const rows = (
    await t.MetaCompleteness.select(['runId', 'layer', 'entity', 'gapType', 'expectedKey', 'detail', 'loggedAt'])
      .orderBy({ loggedAt: 'desc' })
      .first(1000)
      .execute()
  )
    .map(rec)
    .map((r) => ({
      runId: str(r.runId),
      layer: str(r.layer),
      entity: str(r.entity),
      gapType: str(r.gapType),
      expectedKey: str(r.expectedKey),
      detail: str(r.detail),
    }));
  return latestRunPerKey(rows, (x) => x.entity).sort((a, b) => a.expectedKey.localeCompare(b.expectedKey));
}

export interface Score {
  system: string;
  defectId: string;
  method: string;
  cases: number;
  correct: number;
  precision: number | null;
  recall: number | null;
  f1: number | null;
}

/** Cleansing scorecard of the most recent scoring run. */
export async function fetchScorecard(): Promise<Score[]> {
  const t = await trust();
  const rows = (
    await t.MetaAiScorecard.select([
      'runId', 'system', 'defectId', 'method', 'cases', 'correct', 'precision', 'recall', 'f1', 'scoredAt',
    ])
      .orderBy({ scoredAt: 'desc' })
      .first(500)
      .execute()
  ).map(rec);
  if (!rows.length) return [];
  const runId = str(rows[0].runId);
  return rows
    .filter((r) => str(r.runId) === runId)
    .map((r) => ({
      system: str(r.system),
      defectId: str(r.defectId),
      method: str(r.method),
      cases: num(r.cases) ?? 0,
      correct: num(r.correct) ?? 0,
      precision: num(r.precision),
      recall: num(r.recall),
      f1: num(r.f1),
    }));
}
