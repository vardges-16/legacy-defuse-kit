// Generated from rayfin/connectors/trust/metadata.json (meta_quarantine). Read-only Trust Ledger table.
import { entity, date, text, role } from '@microsoft/rayfin-core';
import { Source } from '@microsoft/rayfin-connectors';

@role('authenticated', ['read'])
@entity()
export class MetaQuarantine extends Source({ schema: 'dbo', table: 'meta_quarantine', primaryKey: [] }) {
  @text({ optional: true, column: 'run_id', max: 8000 }) runId?: string;
  @text({ optional: true, max: 8000 }) layer?: string;
  @text({ optional: true, max: 8000 }) entity?: string;
  @text({ optional: true, column: '_row_hash', max: 8000 }) rowHash?: string;
  @text({ optional: true, column: 'reason_code', max: 8000 }) reasonCode?: string;
  @text({ optional: true, column: 'record_key', max: 8000 }) recordKey?: string;
  @text({ optional: true, max: 8000 }) detail?: string;
  @date({ optional: true, column: 'logged_at' }) loggedAt?: Date;
}
