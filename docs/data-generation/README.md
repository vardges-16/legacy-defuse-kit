# Data Generation: Zontique Corp legacy systems (v3)

One generator, one shared truth, five legacy systems that join. Replaces the v2 specs in this folder.

`python generators/generate_zontique.py data/generated` (seed 42, same output every run, about 3 s). Data files go to `data/generated/` (git-ignored); answer keys are committed in `answer-keys/`.

## Scope

| Item | Value |
|---|---|
| Actuals | 2026-07-01 to 2026-10-31, daily, USD; `as_of` 2026-10-31 |
| Budget | FY2026, per cost center and month |
| Organisation | 3 business units, 15 cost centers (0410 to 3590), 20 applications (APP-001 to APP-020) |
| Cloud | Azure (4 subscriptions, resource groups `rg-<app>-<tier>-<env>-<region>-001`) and Google Cloud (projects `prj-<app>-<p/n/d>-<tier>-<hex>`) |
| Join keys | Application code (`MFGEXE`...) in tags and scope names; resource group / project; VM name; cost center |

## Systems and files

| # | System | Lands in | Files | Rows |
|---|---|---|---|---|
| 1 | Cloud cost, **FOCUS 1.2** (Azure + Google Cloud) | Snowflake `COST.FOCUS_COST` | `snowflake/cost/focus_{azure,gcp}_yyyymm.csv` (9 files, 59 columns) | 44,183 |
| 2 | Cloud CMDB | Snowflake `CMDB.ASSETS`, `CMDB.ASSET_GROUPS` | `snowflake/cmdb/*.csv` | 123 / 21 |
| 3 | Financial budget | Snowflake `FIN.BUDGET` | `snowflake/fin/financial_budget.csv` | 193 |
| 4 | License & Inventory | Azure SQL `dbo` (3 tables) | `azure_sql/license_load.sql` (+ CSV) | 8 / 13 / 178 |
| 5 | VM Backups | OneLake `lh_bronze` Files (daily drop) | `vm_backups/jobs/` (120 daily files), `storage/` (4 monthly) | 11,247 / 372 |

Snowflake build: `legacy/snowflake/zontique_legacy.sql`.

## Planted defects (all in `answer-keys/<system>/answer_key.csv`)

| System | ID | Defect | Silver action |
|---|---|---|---|
| Cost | C01 | Export re-run: 2026-08-15 delivered twice | Merge |
| Cost | C02 / C03 | Tag key variant (`APP`, `app_code`) / application tag missing (12% of resources) | Fix (from scope → CMDB) |
| Cost | C04 | Application tag is a name or typo ("Manufacturing Execution", `MFGEX`) | **AI-classify** |
| Cost | C05 | Cost-center tag disagrees with the CMDB | Flag (CMDB wins) |
| Cost | C06 | Google service name variant ("GCE", "Big Query"), ServiceCategory empty | **AI-classify** |
| Cost | C07 / C08 | Spend in scopes the CMDB says are deleted (zombies) / scopes not in the CMDB (shadow IT) | Flag |
| Cost | C09 / C10 / C11 / C12 | Missing GCP day / RG name upper-cased / other date format in one file / subscription renamed | Flag / Fix |
| CMDB | M01, M04, M13 | Duplicate application, scope, group | Merge |
| CMDB | M02 | Department name not the finance name ("Mktg") | **AI-classify** |
| CMDB | M05 | Scope with no application | **AI-extract** from the name |
| CMDB | M06, M07, M08, M10 | Stale mapping, retired app owns active scopes, deleted scope still billing, mapping > 18 months old | Flag |
| CMDB | M03, M09, M11, M12 | Date formats, owner email missing (PII), leading zero lost, timestamp offsets | Fix / Flag |
| Budget | B01 to B08 | Duplicate line, lost leading zero, amount in thousands, negative, missing months, date format, unknown cost center, department variant | Merge / Fix / Flag / Quarantine |
| License | D01 | Discovery product name variant, product id empty (30%) | **AI-classify** |
| License | D02 to D08 | Unmanaged host, deleted VM still seen, duplicate, cores empty, PAYG + BYOL (double licensing), over-deployment, expired entitlement | Flag / Merge / Fix |
| VM Backups | D01 to D09 | Orphaned backup storage, VM not in CMDB, unprotected prod VMs, name variants, size <= 0, overlap duplicates, schema drift, missing files, timestamp format | Flag / Merge / Fix |

FOCUS facts (not defects): reservation purchases carry `BilledCost`, covered usage carries `EffectiveCost`; one credit line; Google labels are lower case. **Chargeback and budgets use `EffectiveCost`; invoice reconciliation uses `BilledCost`.**

## The story the clean data tells (`answer-keys/expected_gold.json`)

- About $120–145k cloud spend a month, growing.
- **Data & Analytics (3170)** runs at 2.6× budget by October (AI services spike from September).
- **Sales (2110)** at +40%; **Legal & Compliance (3590)** and **Quality Control (0450)** overrun.
- About $11k a month of spend is unallocated (shadow IT); retired applications still bill (zombies).
- Orphaned backup storage, 4 double-licensed VMs, 2 under-licensed products.

The legacy RPT views and the Legacy Cost Report miss most of this, by design.
