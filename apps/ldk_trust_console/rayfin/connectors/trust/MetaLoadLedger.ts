// Generated from rayfin/connectors/trust/metadata.json (meta_load_ledger). Read-only Trust Ledger table.
import { entity, date, decimal, int, text, role } from '@microsoft/rayfin-core';
import { Source } from '@microsoft/rayfin-connectors';

@role('authenticated', ['read'])
@entity()
export class MetaLoadLedger extends Source({ schema: 'dbo', table: 'meta_load_ledger', primaryKey: [] }) {
  @text({ optional: true, column: 'run_id', max: 8000 }) runId?: string;
  @text({ optional: true, max: 8000 }) layer?: string;
  @text({ optional: true, max: 8000 }) entity?: string;
  @text({ optional: true, column: 'source_system', max: 8000 }) sourceSystem?: string;
  @text({ optional: true, column: 'source_path', max: 8000 }) sourcePath?: string;
  @text({ optional: true, column: 'target_table', max: 8000 }) targetTable?: string;
  @int({ optional: true, column: 'file_count' }) fileCount?: number;
  @int({ optional: true, column: 'rows_in' }) rowsIn?: number;
  @int({ optional: true, column: 'row_count' }) rowCount?: number;
  @int({ optional: true, column: 'rows_quarantined' }) rowsQuarantined?: number;
  @int({ optional: true, column: 'rows_merged' }) rowsMerged?: number;
  @decimal({ optional: true }) amount?: number;
  @text({ optional: true, max: 8000 }) notebook?: string;
  @date({ optional: true, column: 'logged_at' }) loggedAt?: Date;
}
