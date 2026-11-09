# Trust Ledger contract (for the Silver and Gold notebooks)

**Legacy Defuse Kit · team de_dust2.** The Trust Ledger proves that nothing got lost between source → Bronze → Silver → Gold. It only works if every transformation notebook follows the 6 rules below. The helpers in `nb_ledger_utils` do the writing, so in practice it means one `%run` line and a few function calls.

## Where things live
| Item | Location |
|---|---|
| Sources | **Snowflake** `ZONTIQUE_LEGACY` (schemas `COST`, `CMDB`, `FIN`, `VMB`; legacy views in `RPT`) and **Azure SQL** (License), both mirrored into Fabric. No storage accounts |
| Bronze | Table shortcuts to the mirrored tables in `lh_bronze` (`dbo`), as received. `nb_bronze_files` only logs their counts and amounts |
| Ledger tables | Lakehouse **`lh_trust`** (folder `05_trust`), schema `dbo` |
| Helpers | Notebook **`nb_ledger_utils`**: run `%run nb_ledger_utils` in the first cell |
| One-time setup | Notebook **`nb_trust_setup`**: creates the tables and copies the old `meta_*` tables from `lh_bronze` |
| Checks | Notebook **`nb_reconcile`**: writes `meta_reconciliation` (one row per check) and `meta_trust_summary` (one row per source) |
| Profiling | Notebook **`nb_profile_legacy`**: `LAYER = "bronze"`, or `LAYER = "silver"` with `SOURCE_LAKEHOUSE = "lh_silver"` |

## Ledger tables in `lh_trust`
| Table | One row per | Written by |
|---|---|---|
| `meta_load_ledger` | table written (Bronze, Silver, Gold) | `log_step(...)` |
| `meta_quarantine` | rejected row (`_row_hash`, `reason_code`, `record_key`) | `quarantine(...)` |
| `meta_merge_log` | merged-away duplicate (`_row_hash`, `survivor_hash`) | `dedupe(...)` or `log_merges(...)` |
| `meta_ai_decisions` | AI call result (input, output, error, accepted) | `log_ai(...)`; profiling AI layer |
| `meta_profile_tables` / `_columns` / `_findings` | profiled table, column, finding | `nb_profile_legacy` |
| `meta_reconciliation` | check (PASS / FAIL / PENDING) | `nb_reconcile` |
| `meta_trust_summary` | source per reconciliation run: rows per layer, quarantined, merged, lost, amounts, status | `nb_reconcile` |

## The 6 rules
1. **Keep `_row_hash`.** Every Silver table carries the `_row_hash` of the Bronze row it came from. All Bronze tables are mirrored, so call `with_row_hash(df)` on the Bronze table **before any change**. It ignores the lineage columns `_SOURCE_FILE` and `_LOADED_AT`.
2. **Never drop a row silently.** Every Bronze row ends up in exactly one place: the Silver table, `quarantine(...)` (with a reason code), or `dedupe(...)` / `log_merges(...)` (as a merged duplicate).
3. **One `log_step` per table written**, with `rows_in`, `rows_out`, `quarantined`, `merged` (and `amount` where it applies). The rule is rows_in = rows_out + quarantined + merged; the helper prints a warning when it doesn't add up.
4. **One run id per notebook run:** `RUN_ID = new_run_id(PIPELINE_RUN_ID)`. Give the notebook a parameter cell with `PIPELINE_RUN_ID = ""`; the pipeline fills it in.
5. **Log every AI decision** with `log_ai(...)`. Only accept AI output that lands on a valid value (an existing CMDB app, a known product); otherwise quarantine the row.
6. **Hash personal data before AI or Silver:** `hash_pii(df, ["OWNER_NAME", "OWNER_EMAIL"])`. On Runtime 2.0, call `ai_setup()` before any AI Function.

## Table names `nb_reconcile` expects
Change them in one place: the `ENTITIES` parameter cell of `nb_reconcile`.

| Entity | Source | Bronze (`lh_bronze`) | Silver (`lh_silver`) | Gold (`wh_gold`) | Amount column |
|---|---|---|---|---|---|
| `cost` | Snowflake `COST.FOCUS_COST` | `FOCUS_COST` | `silver_cost` | `fact_cost` | `billed_cost` (source `BILLEDCOST`) |
| `budget` | Snowflake `FIN.BUDGET` | `BUDGET` | `silver_budget` | `fact_budget` | `budget_cost` |
| `cmdb_assets` | Snowflake `CMDB.ASSETS` | `ASSETS` | `silver_cmdb_assets` | — | — |
| `cmdb_asset_groups` | Snowflake `CMDB.ASSET_GROUPS` | `ASSET_GROUPS` | `silver_cmdb_asset_groups` | — | — |
| `vm_backup_jobs` | Snowflake `VMB.BACKUP_JOBS` | `BACKUP_JOBS` | `silver_vm_backup_jobs` | — | — |
| `vm_backup_storage` | Snowflake `VMB.BACKUP_STORAGE` | `BACKUP_STORAGE` | `silver_vm_backup_storage` | `fact_backup_cost` | `storage_cost_usd` |
| `license_installations` | Azure SQL `dbo.installations` | `installations` | `silver_license_installations` | — | — |
| `license_entitlements` | Azure SQL `dbo.license_entitlements` | `license_entitlements` | `silver_license_entitlements` | — | — |
| `license_products` | Azure SQL `dbo.license_products` | `license_products` | `silver_license_products` | — | — |

Use the entity names above in `log_step`, `quarantine`, `dedupe` and `log_ai`. They are how the reconciliation finds your Silver run.

## Template: a Silver notebook
```python
%run nb_ledger_utils
```
```python
PIPELINE_RUN_ID = ""      # parameter cell
```
```python
RUN_ID = new_run_id(PIPELINE_RUN_ID)
ENTITY = "cmdb_assets"

b = with_row_hash(read_item("lh_bronze.Lakehouse/Tables/dbo/ASSETS"))   # hash first, before any change
b = hash_pii(b, ["OWNER_NAME", "OWNER_EMAIL"])

# 1. reject rows that can't be fixed: quarantine, don't drop
bad = b.filter(F.col("ASSET_ID").isNull())
n_q = quarantine(bad, ENTITY, "MISSING_KEY", RUN_ID, key_col="ASSET_ID")
good = b.join(bad.select("_row_hash"), "_row_hash", "left_anti")

# 2. rule-based cleaning (keep _row_hash!)
clean = good.withColumn("ASSET_ID", F.upper(F.trim("ASSET_ID")))

# 3. duplicates: keep one per key, log the rest
kept, n_m = dedupe(clean, ENTITY, ["ASSET_ID"], RUN_ID)

# 4. AI step (optional): ai_setup(); result = kept.ai.classify(...); log_ai(result, ENTITY, "department name", "department", "department_ai", RUN_ID, error_col="ai_error")

kept.write.format("delta").mode("overwrite").option("overwriteSchema", True) \
    .save(f"{ROOT}/lh_silver.Lakehouse/Tables/dbo/silver_cmdb_assets")
log_step(RUN_ID, "silver", ENTITY, "ASSETS", "silver_cmdb_assets",
         rows_out=kept.count(), rows_in=b.count(), quarantined=n_q, merged=n_m, notebook="nb_silver_cmdb")
```

## Source check
`nb_reconcile` queries Snowflake live for row counts and amounts (user `LEDGER_READER`, password in Key Vault secret `snowflake-ledger-reader-pwd`), so "source = Bronze" is a real check. License (Azure SQL) uses totals checked by hand. Mirrored column names are UPPER CASE; the checks match column names case-insensitively.

## Gold
Gold is T-SQL in `wh_gold`. Nothing to log from T-SQL: `nb_reconcile` reads the Gold tables afterwards and checks Silver amount = Gold amount. Gold fact tables must keep the amount columns named as above.

## Order of a full run
`nb_bronze_files` → Silver notebooks → Gold (`usp_build_gold`) → `nb_reconcile` → `nb_profile_legacy` (Silver).
