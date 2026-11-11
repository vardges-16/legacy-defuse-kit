// Generated from rayfin/connectors/trust/metadata.json (meta_pipeline_activity_runs). Read-only Trust Ledger table.
import { entity, date, decimal, text, role } from '@microsoft/rayfin-core';
import { Source } from '@microsoft/rayfin-connectors';

@role('authenticated', ['read'])
@entity()
export class MetaPipelineActivityRuns extends Source({ schema: 'dbo', table: 'meta_pipeline_activity_runs', primaryKey: [] }) {
  @text({ optional: true, column: 'run_id', max: 8000 }) runId?: string;
  @text({ optional: true, column: 'activity_name', max: 8000 }) activityName?: string;
  @text({ optional: true, column: 'activity_type', max: 8000 }) activityType?: string;
  @text({ optional: true, max: 8000 }) status?: string;
  @date({ optional: true, column: 'started_at' }) startedAt?: Date;
  @date({ optional: true, column: 'ended_at' }) endedAt?: Date;
  @decimal({ optional: true, column: 'duration_s' }) durationS?: number;
  @text({ optional: true, column: 'error_code', max: 8000 }) errorCode?: string;
  @text({ optional: true, column: 'error_message', max: 8000 }) errorMessage?: string;
  @text({ optional: true, column: 'activity_run_id', max: 8000 }) activityRunId?: string;
  @date({ optional: true, column: 'audited_at' }) auditedAt?: Date;
}
