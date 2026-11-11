// Generated from rayfin/connectors/trust/metadata.json (meta_trust_summary). Read-only Trust Ledger table.
import { entity, date, decimal, int, text, role } from '@microsoft/rayfin-core';
import { Source } from '@microsoft/rayfin-connectors';

@role('authenticated', ['read'])
@entity()
export class MetaTrustSummary extends Source({ schema: 'dbo', table: 'meta_trust_summary', primaryKey: [] }) {
  @text({ optional: true, column: 'run_id', max: 8000 }) runId?: string;
  @date({ optional: true, column: 'checked_at' }) checkedAt?: Date;
  @text({ optional: true, max: 8000 }) entity?: string;
  @text({ optional: true, max: 8000 }) status?: string;
  @int({ optional: true, column: 'source_rows' }) sourceRows?: number;
  @int({ optional: true, column: 'bronze_rows' }) bronzeRows?: number;
  @int({ optional: true, column: 'silver_rows' }) silverRows?: number;
  @int({ optional: true, column: 'quarantined_rows' }) quarantinedRows?: number;
  @int({ optional: true, column: 'merged_rows' }) mergedRows?: number;
  @int({ optional: true, column: 'lost_rows' }) lostRows?: number;
  @decimal({ optional: true, column: 'bronze_amount' }) bronzeAmount?: number;
  @decimal({ optional: true, column: 'silver_amount' }) silverAmount?: number;
  @decimal({ optional: true, column: 'gold_amount' }) goldAmount?: number;
  @int({ optional: true, column: 'checks_pass' }) checksPass?: number;
  @int({ optional: true, column: 'checks_fail' }) checksFail?: number;
  @int({ optional: true, column: 'checks_pending' }) checksPending?: number;
  @text({ optional: true, column: 'silver_run_id', max: 8000 }) silverRunId?: string;
}
