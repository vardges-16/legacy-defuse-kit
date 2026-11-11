// Source: trust (fabric-sqlanalytics) — lh_trust SQL analytics endpoint, read-only.
import type { GraphQLBackedConnector } from '@microsoft/rayfin-connector-fabric-graphql';
import type { ConnectorConfig } from '@microsoft/rayfin-connectors';

import { MetaPipelineRuns } from './MetaPipelineRuns.js';
import { MetaPipelineActivityRuns } from './MetaPipelineActivityRuns.js';
import { MetaTrustSummary } from './MetaTrustSummary.js';
import { MetaReconciliation } from './MetaReconciliation.js';
import { MetaLoadLedger } from './MetaLoadLedger.js';
import { MetaQuarantine } from './MetaQuarantine.js';
import { MetaCompleteness } from './MetaCompleteness.js';
import { MetaAiScorecard } from './MetaAiScorecard.js';

export { MetaPipelineRuns } from './MetaPipelineRuns.js';
export { MetaPipelineActivityRuns } from './MetaPipelineActivityRuns.js';
export { MetaTrustSummary } from './MetaTrustSummary.js';
export { MetaReconciliation } from './MetaReconciliation.js';
export { MetaLoadLedger } from './MetaLoadLedger.js';
export { MetaQuarantine } from './MetaQuarantine.js';
export { MetaCompleteness } from './MetaCompleteness.js';
export { MetaAiScorecard } from './MetaAiScorecard.js';

export const connectorConfig = {
  connector: 'fabric-sqlanalytics',
  operations: ['read'],
  entities: { MetaPipelineRuns, MetaPipelineActivityRuns, MetaTrustSummary, MetaReconciliation, MetaLoadLedger, MetaQuarantine, MetaCompleteness, MetaAiScorecard },
} as const satisfies ConnectorConfig;

export type TrustSchema = GraphQLBackedConnector<
  { MetaPipelineRuns: typeof MetaPipelineRuns; MetaPipelineActivityRuns: typeof MetaPipelineActivityRuns; MetaTrustSummary: typeof MetaTrustSummary; MetaReconciliation: typeof MetaReconciliation; MetaLoadLedger: typeof MetaLoadLedger; MetaQuarantine: typeof MetaQuarantine; MetaCompleteness: typeof MetaCompleteness; MetaAiScorecard: typeof MetaAiScorecard },
  typeof connectorConfig
>;
