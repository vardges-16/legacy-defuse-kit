# Trust Ledger contract

**Legacy Defuse Kit · team de_dust2.** The Trust Ledger proves that nothing got lost between source → Bronze → Silver → Gold. It only works if every Bronze, Silver and Gold step follows the 6 rules below. The helpers in `nb_ledger_utils` do the writing, so in practice it is one `%run` line and a few function calls. Design overview: [architecture.md](architecture.md).

## Where things live
| Item | Location |
|---|---|
| Sources | Snowflake `ZONTIQUE_LEGACY` (`COST`, `CMDB`, `FIN`; legacy views in `RPT`), Azure SQL `dbo` (License), VM Backup files in the storage container `vm-backups` |
| Bronze | `lh_bronze`: schema shortcuts `COST`, `CMDB`, `FIN` to `mir_snowflake`; `dbo` shortcuts to `mir_license`; Files shortcut `landing/vm-backups`; `nb_bronze_files` writes `bkp.backup_jobs` and `bkp.backup_storage` |
| Silver | `lh_silver`, schema `dbo`, written by `nb_silver_*`; contract views in schema `contract` |
| Gold | Warehouse `wh_gold`, built by the stored procedure `dbo.usp_build_gold` |
| Ledger tables | `lh_trust`, schema `dbo` (tables are created on the first write) |
| Helpers | `nb_ledger_utils` (`%run nb_ledger_utils` in the first cell). The table locations are in its `BRONZE` map |
| Checks | `nb_reconcile` (source = Bronze = Silver = Gold) and `nb_ai_score` (cleansing quality against the answer keys) |

## Ledger tables in `lh_trust`
| Table | One row per | Written by |
|---|---|---|
| `meta_load_ledger` | table written (Bronze, Silver): rows in, out, quarantined, merged, amount | `log_step` |
| `meta_quarantine` | rejected row (`_row_hash`, reason code, record key) | `quarantine` |
| `meta_merge_log` | merged-away duplicate (`_row_hash`, survivor hash) | `dedupe`, `log_merges` |
| `meta_file_manifest` | delivered file: name, rows, size | `nb_bronze_files` |
| `meta_completeness` | missing day, month or file | `log_gap` |
| `meta_ai_decisions` | AI decision: input, output, similarity, accepted | `ai_resolve`, `log_ai` |
| `meta_ai_review_queue` | AI result the gate did not accept (waiting for a human) | `ai_resolve`, closed by `nb_ai_review` |
| `meta_ai_mappings` | approved mapping, used as a rule on the next run | `ai_resolve`, `nb_ai_review` |
| `meta_reconciliation` | check per entity and run (PASS, FAIL, PENDING) | `nb_reconcile` |
| `meta_trust_summary` | entity per run: rows per layer, quarantined, merged, lost, amounts, status | `nb_reconcile` |
| `meta_ai_scorecard`, `meta_defect_detection` | cleansing task score / planted defect type | `nb_ai_score` |
| `meta_profile_tables`, `_columns`, `_findings` | profiled table, column, finding | `nb_profile_legacy` |

## The 6 rules
1. **Keep `_row_hash`.** Every Silver table carries the `_row_hash` of the Bronze row it came from. `bronze(name)` adds it before any change; it ignores the lineage columns `_SOURCE_FILE` and `_LOADED_AT`.
2. **Never drop a row silently.** Every Bronze row ends up in exactly one place: the Silver table, `quarantine(...)` (with a reason code) or `dedupe(...)` / `log_merges(...)` (as a merged duplicate).
3. **One `log_step` per table written**, with `rows_in`, `rows_out`, `quarantined`, `merged` and, where it applies, `amount`. Rule: rows in = rows out + quarantined + merged. The helper prints `OK` or `FAIL`.
4. **One run id per notebook run:** `RUN_ID = new_run_id(PIPELINE_RUN_ID)`. Every notebook has a parameter cell `PIPELINE_RUN_ID = ""` that the pipeline fills.
5. **AI only on what rules could not resolve, and only with a gate.** `ai_resolve(...)` sends distinct leftover values to `ai.classify` with a list of allowed labels and checks the answer with `ai.similarity`. Accepted, rejected and unmatched results are all logged; rejected ones go to the review queue.
6. **Hash personal data before Silver and before any AI step:** `hash_pii(df, [...])`. Names and emails are never sent to AI.

## Entities and tables `nb_reconcile` checks
Change them in one place: the `ENTITIES` cell of `nb_reconcile`. Source = Bronze and Bronze = Silver + quarantined + merged are checked for every entity; Silver amount = Gold amount only where an amount column is listed (cost, budget, backup storage). Use these entity names in every `log_step`, `quarantine`, `dedupe` and `log_ai`, because the reconciliation finds each Silver run by them.

| Entity | Source | Bronze | Silver (`lh_silver.dbo`) | Gold table (`wh_gold.dbo`) | Amount |
|---|---|---|---|---|---|
| `cost` | Snowflake `COST.FOCUS_COST` | `COST.FOCUS_COST` | `silver_cost` | `fact_cost` | `billed_cost` |
| `budget` | Snowflake `FIN.BUDGET` | `FIN.BUDGET` | `silver_budget` | `fact_budget` | `budget_cost` |
| `cmdb_assets` | Snowflake `CMDB.ASSETS` | `CMDB.ASSETS` | `silver_cmdb_applications` + `silver_cmdb_scopes` | `dim_application` | none |
| `cmdb_asset_groups` | Snowflake `CMDB.ASSET_GROUPS` | `CMDB.ASSET_GROUPS` | `silver_cmdb_asset_groups` | none | none |
| `vm_backup_jobs` | file manifest | `bkp.backup_jobs` | `silver_backup_jobs` | `fact_backup_job` | none |
| `vm_backup_storage` | file manifest | `bkp.backup_storage` | `silver_backup_storage` | `fact_backup_cost` | `storage_cost_usd` |
| `license_products` | Azure SQL (totals checked by hand) | `dbo.license_products` | `silver_license_products` | `dim_product` | none |
| `license_entitlements` | Azure SQL (by hand) | `dbo.license_entitlements` | `silver_license_entitlements` | `fact_license_position` | none |
| `license_installations` | Azure SQL (by hand) | `dbo.installations` | `silver_license_installations` | `fact_license_installation` | none |

## Source check
`nb_reconcile` queries Snowflake live for row counts and amounts (user `LEDGER_READER`, password in Key Vault secret `snowflake-ledger-reader-pwd`), so "source = Bronze" is a real check. VM Backups are checked against the file manifest; License (Azure SQL) against totals checked by hand. Mirrored column names are upper case, and the checks match column names case-insensitively.

## Gold
Gold is T-SQL in `wh_gold` (`usp_build_gold`), tables only. Nothing is logged from T-SQL: `nb_reconcile` reads the Gold tables afterwards and checks Silver amount = Gold amount. Fact tables keep the amount column names of Silver.

## Order of a full run
Pipeline `one_pipeline_to_rule_them_all`: `nb_orchestrator` (`nb_bronze_files` → five `nb_silver_*` → `nb_silver_contract`) → `usp_build_gold` → `nb_reconcile` → `nb_ai_score` → refresh of both semantic models.
