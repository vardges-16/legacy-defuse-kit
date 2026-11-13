# Legacy Defuse Kit

**AI-Accelerated Legacy-to-Fabric Modernization Factory**, team **de_dust2**. Zontique Corp's cost, CMDB, budget, license and backup data move out of five legacy systems into Microsoft Fabric, are cleaned with AI, and a Trust Ledger proves that nothing was lost.

## Repository

| Path | Content |
|---|---|
| `fabric/` | Fabric workspace `ldk-workspace`, Git-synced (mirrors, lakehouses, warehouse, notebooks, pipeline, semantic model, report, data agent). Folders `00_legacy` to `06_intelligence` |
| `apps/ldk_trust_console/` | Fabric App (React) for pipeline and Trust Ledger monitoring; deploy with `npx rayfin up` |
| `sql/wh_gold/` | Gold build procedure `usp_build_gold` |
| `generators/generate_zontique.py` | Creates the five legacy systems with planted defects (seed 42) |
| `legacy/snowflake/zontique_legacy.sql` | Builds the Snowflake legacy database `ZONTIQUE_LEGACY` (cost, CMDB, budget) and its legacy views |
| `answer-keys/` | Ground truth for scoring only (see its README) |
| `docs/` | `architecture.md` (as built), Trust Ledger contract, Foundry agent setup, blueprint and SAD (submitted plan), data catalog (`docs/data-generation/README.md`), diagrams |
| `data/generated/` | Generated data files, not committed |

## Rebuild the legacy world

1. `python generators/generate_zontique.py data/generated`
2. Snowflake: run `legacy/snowflake/zontique_legacy.sql` part by part; upload `data/generated/snowflake/` files when the script says so.
3. Azure SQL: run `data/generated/azure_sql/license_load.sql` (pause the license mirror first).
4. VM Backups: upload `data/generated/vm_backups/` (`jobs/`, `storage/`) to the storage account container `vm-backups`; `lh_bronze` reads it through the OneLake shortcut `Files/landing/vm-backups`.
5. Fabric: re-create the Snowflake mirror, then run the pipeline `one_pipeline_to_rule_them_all`.
