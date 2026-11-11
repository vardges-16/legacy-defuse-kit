// Generated from rayfin/connectors/trust/metadata.json (meta_ai_scorecard). Read-only Trust Ledger table.
import { entity, date, decimal, int, text, role } from '@microsoft/rayfin-core';
import { Source } from '@microsoft/rayfin-connectors';

@role('authenticated', ['read'])
@entity()
export class MetaAiScorecard extends Source({ schema: 'dbo', table: 'meta_ai_scorecard', primaryKey: [] }) {
  @text({ optional: true, column: 'run_id', max: 8000 }) runId?: string;
  @text({ optional: true, max: 8000 }) task?: string;
  @text({ optional: true, max: 8000 }) system?: string;
  @text({ optional: true, column: 'defect_id', max: 8000 }) defectId?: string;
  @text({ optional: true, max: 8000 }) method?: string;
  @int({ optional: true }) cases?: number;
  @int({ optional: true }) attempted?: number;
  @int({ optional: true }) correct?: number;
  @decimal({ optional: true }) precision?: number;
  @decimal({ optional: true }) recall?: number;
  @decimal({ optional: true }) f1?: number;
  @date({ optional: true, column: 'scored_at' }) scoredAt?: Date;
}
