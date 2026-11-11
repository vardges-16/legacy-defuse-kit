// Generated from rayfin/connectors/trust/metadata.json (meta_reconciliation). Read-only Trust Ledger table.
import { entity, date, decimal, text, role } from '@microsoft/rayfin-core';
import { Source } from '@microsoft/rayfin-connectors';

@role('authenticated', ['read'])
@entity()
export class MetaReconciliation extends Source({ schema: 'dbo', table: 'meta_reconciliation', primaryKey: [] }) {
  @text({ optional: true, column: 'run_id', max: 8000 }) runId?: string;
  @text({ optional: true, max: 8000 }) entity?: string;
  @text({ optional: true, column: 'check_name', max: 8000 }) checkName?: string;
  @text({ optional: true, column: 'left_layer', max: 8000 }) leftLayer?: string;
  @decimal({ optional: true, column: 'left_value' }) leftValue?: number;
  @text({ optional: true, column: 'right_layer', max: 8000 }) rightLayer?: string;
  @decimal({ optional: true, column: 'right_value' }) rightValue?: number;
  @decimal({ optional: true }) difference?: number;
  @text({ optional: true, max: 8000 }) status?: string;
  @text({ optional: true, max: 8000 }) note?: string;
  @date({ optional: true, column: 'checked_at' }) checkedAt?: Date;
}
