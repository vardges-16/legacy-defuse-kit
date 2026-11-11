// Generated from rayfin/connectors/trust/metadata.json (meta_completeness). Read-only Trust Ledger table.
import { entity, date, text, role } from '@microsoft/rayfin-core';
import { Source } from '@microsoft/rayfin-connectors';

@role('authenticated', ['read'])
@entity()
export class MetaCompleteness extends Source({ schema: 'dbo', table: 'meta_completeness', primaryKey: [] }) {
  @text({ optional: true, column: 'run_id', max: 8000 }) runId?: string;
  @text({ optional: true, max: 8000 }) layer?: string;
  @text({ optional: true, max: 8000 }) entity?: string;
  @text({ optional: true, column: 'gap_type', max: 8000 }) gapType?: string;
  @text({ optional: true, column: 'expected_key', max: 8000 }) expectedKey?: string;
  @text({ optional: true, max: 8000 }) detail?: string;
  @date({ optional: true, column: 'logged_at' }) loggedAt?: Date;
}
