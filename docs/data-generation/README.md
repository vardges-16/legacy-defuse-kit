# Data Generation: Zontique Corp legacy systems (v3)

One generator, one shared truth, five legacy systems that join. Replaces the v2 specs in this folder.

`python generators/generate_zontique.py data/generated` (seed 42, same output every run, about 15 s). Data files go to `data/generated/` (git-ignored); answer keys are committed in `answer-keys/`.

## Scope

| Item | Value |
|---|---|
| Cloud cost | 2024-01-01 to 2026-10-31, daily lines, one export file per provider and month (`focus_<provider>_yyyymm.csv`), USD; `as_of` 2026-10-31. A scope starts billing on its CMDB creation date, so spend grows from about $36k to $126k a month |
| VM Backups | **Storage reports monthly from 2024-01 to 2026-10** (same period as cost and budget; a VM appears from its creation month, retained storage grows over time). **Daily job logs only for the last 4 months** (2026-07 to 2026-10): the backup tool keeps no older job history |
| CMDB, License | Snapshots as of 2026-10-31 (the CMDB carries creation dates; License carries install and last-seen dates) |
| Budget | FY2024 to FY2026, per cost center and month |
| Organisation | 3 business units, 15 cost centers (0410 to 3590), 20 applications (APP-001 to APP-020) |
| Cloud | Azure (4 subscriptions, resource groups `rg-<app>-<tier>-<env>-<region>-001`) and Google Cloud (projects `prj-<app>-<p/n/d>-<tier>-<hex>`) |
| Join keys | Application code (`MFGEXE`...) in tags and scope names; resource group / project; VM name; cost center |

## Systems and files

| # | System | Lands in | Files | Rows |
|---|---|---|---|---|
| 1 | Cloud cost, **FOCUS 1.2** (Azure + Google Cloud) | Snowflake `COST.FOCUS_COST` | `snowflake/cost/focus_{azure,gcp}_yyyymm.csv` (69 files, 59 columns) | 275,649 |
| 2 | Cloud CMDB | Snowflake `CMDB.ASSETS`, `CMDB.ASSET_GROUPS` | `snowflake/cmdb/*.csv` | 123 / 21 |
| 3 | Financial budget | Snowflake `FIN.BUDGET` | `snowflake/fin/financial_budget.csv` | 577 |
| 4 | License & Inventory | Azure SQL `dbo` (3 tables) | `azure_sql/license_load.sql` (+ CSV) | 8 / 13 / 177 |
| 5 | VM Backups | Azure Storage container `vm-backups`, read through a OneLake shortcut at `lh_bronze` Files `landing/vm_backups` | `jobs/` (120 daily files, 2026-07 to 2026-10), `storage/` (34 monthly files, 2024-01 to 2026-10) | 11,098 / 2,395 (90 per month in 2026-07 to 2026-10) |

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

- Cloud spend grows from about $36k a month (January 2024) to about $126k (October 2026).
- **Data & Analytics (3170)** runs at 2.7× budget by October 2026 (AI services spike from September).
- **Sales (2110)** at +44%; **Legal & Compliance (3590)** and **Quality Control (0450)** overrun.
- Part of the spend is unallocated (shadow IT); retired applications still bill (zombies).
- Orphaned backup storage, 4 double-licensed VMs, 2 under-licensed products.
- Backup storage cost grows with the estate, from about $670 (2024-01) to about $3,840 (2026-10) a month.

The legacy RPT views and the Legacy Cost Report miss most of this, by design.
