// Generated from rayfin/connectors/trust/metadata.json (meta_pipeline_runs). Read-only Trust Ledger table.
import { entity, date, decimal, int, text, role } from '@microsoft/rayfin-core';
import { Source } from '@microsoft/rayfin-connectors';

@role('authenticated', ['read'])
@entity()
export class MetaPipelineRuns extends Source({ schema: 'dbo', table: 'meta_pipeline_runs', primaryKey: [] }) {
  @text({ optional: true, column: 'run_id', max: 8000 }) runId?: string;
  @text({ optional: true, column: 'pipeline_name', max: 8000 }) pipelineName?: string;
  @text({ optional: true, column: 'trigger_type', max: 8000 }) triggerType?: string;
  @date({ optional: true, column: 'started_at' }) startedAt?: Date;
  @date({ optional: true, column: 'ended_at' }) endedAt?: Date;
  @decimal({ optional: true, column: 'duration_s' }) durationS?: number;
  @text({ optional: true, max: 8000 }) status?: string;
  @text({ optional: true, column: 'failed_steps', max: 8000 }) failedSteps?: string;
  @text({ optional: true, column: 'skipped_steps', max: 8000 }) skippedSteps?: string;
  @int({ optional: true, column: 'ledger_rows' }) ledgerRows?: number;
  @int({ optional: true, column: 'reconcile_checks' }) reconcileChecks?: number;
  @int({ optional: true, column: 'reconcile_fail' }) reconcileFail?: number;
  @date({ optional: true, column: 'audited_at' }) auditedAt?: Date;
}
