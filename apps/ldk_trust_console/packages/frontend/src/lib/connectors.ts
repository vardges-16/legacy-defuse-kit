//-----------------------------------------------------------------------
// Connector wiring for the Trust Ledger console.
// Owned by the app (the CLI-generated marker was removed on purpose).
// `trust` = lh_trust SQL analytics endpoint, read-only, delegated access:
// every viewer reads the ledger with their own Fabric permissions.
//-----------------------------------------------------------------------

import type { ConnectorsRuntime } from '@microsoft/rayfin-connectors';

import {
  connectorConfig as trustConfig,
  type TrustSchema,
} from '../../../../rayfin/connectors/trust/schema';

export type AppConnectorsSchema = {
  trust: TrustSchema;
};

export const connectorConfigs = {
  trust: trustConfig,
};

/** Category A (SQL) connectors need no runtime entry. */
export const connectorRuntimes: ConnectorsRuntime = {};
