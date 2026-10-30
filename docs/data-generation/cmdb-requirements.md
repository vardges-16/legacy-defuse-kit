# Cloud CMDB: Data Generation Requirements (MVP)

**Project:** Legacy Defuse Kit (team de_dust2) | **Scenario company:** Zontique Corp | **System:** 4 of 5, Cloud CMDB

This is the brief for a generator ("randomiser") that produces the simulated Cloud CMDB, its planted defects and its answer key. It is deliberately minimal (hackathon MVP): **3 tables**, one of them a large fact table.

## 1. Basis

| Item | Source |
|---|---|
| The CMDB records which application owns which cloud resource group; the mapping is stale and never joined in systematically | Blueprint, section 1 (system 4) |
| Cost and backups are linked to applications through the CMDB; applications are linked to departments | Blueprint, section 4 (Silver) |
| Simulated as a REST API on Azure Functions, data in Azure Blob Storage | Blueprint, section 4 |
| Owner names and emails are personal data, hashed in Silver | Blueprint, section 4 (Security & Compliance) |
| Planted defects plus an answer key to measure AI cleansing | Blueprint, section 4 |
| Table design, columns, volumes, defects below | **Assumption** (team may change) |

**Scope decision:** Azure only. Cost lines from other providers in Snowflake simply have no CMDB match, which is itself part of the "unattributed cost" story.

## 2. Tables at a glance

| Table | Role | Rows (default) | Grain |
|---|---|---|---|
| `applications` | Dimension | 80 (+4 planted duplicates) | One application |
| `resource_groups` | Dimension + ownership mapping | 400 (+8 planted duplicates) | One Azure resource group, with its owning application |
| `resources` | **Fact** | **10,000** (set by `resource_count`) | One Azure resource (VM, disk, database, ...) |

Department and owner live as columns on `applications` (no separate tables). Each resource group has **one** owning application.

## 3. Parameters

| Parameter | Default | Note |
|---|---|---|
| `seed` | 42 | Same seed gives identical files |
| `as_of_date` | 2026-10-31 | "Today" for staleness rules |
| `resource_count` | 10000 | Fact rows; tested range 10,000 to 50,000. Defect counts are percentages, rounded to whole rows |
| `history_start` | 2023-01-01 | Earliest created date |
| `email_domain` | `zontique.com` | Synthetic names only, never real people |

## 4. Columns

Dates are `YYYY-MM-DD`, timestamps UTC ISO 8601 in clean data. **PII** columns are hashed in Silver.

### 4.1 `applications`

| Column | Type | Rule |
|---|---|---|
| application_id | string | `APP-001` to `APP-080`, primary key |
| application_code | string | 3 to 6 uppercase letters, unique (for example `BILL`) |
| application_name | string | Plausible business name, unique |
| department | string | One of 10: Finance, IT Operations, Platform Engineering, Data & Analytics, Customer Service, Sales, Marketing, Human Resources, Legal & Compliance, Supply Chain |
| cost_center | string | `CC-` plus 4 digits, one per department |
| owner_name | string | **PII**, synthetic full name |
| owner_email | string | **PII**, `first.last@zontique.com` |
| criticality | string | Mission Critical, Business Critical, Business Operational, Administrative |
| lifecycle_status | string | Active (about 85%), Deprecated (8%), Retired (7%) |
| last_modified_utc | timestamp | |

### 4.2 `resource_groups`

| Column | Type | Rule |
|---|---|---|
| resource_group_id | string | `RG-0001` and up, primary key |
| resource_group_name | string | `rg-<app code lowercase>-<env>-<nn>`, for example `rg-bill-prod-01`, unique |
| subscription_name | string | One of 4: `zontique-prod`, `zontique-nonprod`, `zontique-shared`, `zontique-sandbox` |
| region | string | swedencentral, westeurope, northeurope |
| environment | string | prod, test, dev, shared |
| application_id | string | Owning application, FK to `applications` (the CMDB mapping) |
| mapping_last_verified | date | Clean: within 18 months before `as_of_date` |
| status | string | Active, Deleted (about 5% Deleted) |
| last_modified_utc | timestamp | |

### 4.3 `resources` (fact)

| Column | Type | Rule |
|---|---|---|
| resource_id | string | Azure-style ID: `/subscriptions/<guid>/resourceGroups/<rg name>/providers/<type>/<resource name>`, unique |
| resource_name | string | For VMs: `vm-<app code>-<env>-<nn>`; others any plausible name |
| resource_type | string | Mix below |
| resource_group_id | string | FK to `resource_groups` (Active RGs only in clean data) |
| sku | string | Plausible per type (for example `Standard_D4s_v5`, `Premium_LRS`, `S1`) |
| status | string | Active, Deleted (about 6% Deleted) |
| created_date | date | Between `history_start` and `as_of_date` |
| deleted_date | date | Set only when Deleted |
| tag_app_code | string | Equals the owning application's code in clean data |
| tag_environment | string | Equals the resource group's environment |
| tag_cost_center | string | Equals the owning application's cost center |
| last_modified_utc | timestamp | |

Resource type mix (share of `resource_count`):

| resource_type | Share |
|---|---|
| Microsoft.Compute/virtualMachines | 20% |
| Microsoft.Compute/disks | 30% |
| Microsoft.Network/networkInterfaces | 20% |
| Microsoft.Storage/storageAccounts | 10% |
| Microsoft.Sql/servers/databases | 8% |
| Microsoft.Web/sites | 7% |
| Microsoft.KeyVault/vaults | 5% |

## 5. Clean-data rules (generator self-test before defects)

1. Every foreign key resolves; IDs and names are unique.
2. Retired applications own no Active resource groups.
3. Resources sit in Active resource groups; Deleted resources have a `deleted_date`.
4. Tags on each resource match its resource group and owning application.
5. Each prod resource group contains at least one VM.

## 6. Planted defects

Counts at default volumes. A row carries **at most one** of D01 to D11, so the answer key is unambiguous. D12 to D14 are format-level and may overlap.

| ID | Table | Defect | Count | Expected Silver action |
|---|---|---|---|---|
| D01 | applications | Duplicate application (name variant, new id) | 4 rows | Merge |
| D02 | applications | Department name variant ("IT Ops", "Fin.", "HR Dept") | 6 apps | Fix |
| D03 | applications | `owner_email` missing or malformed | 5 apps | Flag or Fix |
| D04 | applications | Retired application still owns Active resource groups | 3 apps | Flag |
| D05 | resource_groups | `application_id` empty: unowned resource group | 40 RGs (10%) | Flag: unattributed cost |
| D06 | resource_groups | Stale mapping: `mapping_last_verified` older than 18 months | 60 RGs (15%) | Flag |
| D07 | resource_groups | Duplicate with name variant (case, `_` instead of `-`) | 8 rows | Merge |
| D08 | resources | `tag_app_code` missing | 8% | Fix from resource group |
| D09 | resources | `tag_app_code` disagrees with the resource group's owner | 3% | Flag |
| D10 | resources | `deleted_date` set but `status` still Active | 2% | Fix (Deleted) |
| D11 | resources | `resource_group_id` not found in `resource_groups` | 1% | Quarantine |
| D12 | resources | `tag_environment` variants (Prod, PRD, production) | 10% | Fix |
| D13 | all | Mixed date formats (`dd/MM/yyyy`, `N/A`) | 2% of date values | Fix or Quarantine |
| D14 | resources | API page overlap: last row of each page repeated on the next page | every page boundary | Merge (exact duplicate) |

## 7. Links to the other systems

All five generators start from one shared `master_truth.json` written by this generator (canonical applications, resource groups, resources).

| System | Joins to the CMDB on | Required behaviour |
|---|---|---|
| Snowflake (actual cost) | `resource_id`, or resource group name | Most cost lines reference CMDB resources; some use name variants (case, spaces). About 6% of cost value sits on unowned RGs (D05), on deleted resources, or on providers not in the CMDB |
| VM Backups | VM `resource_name` | Every VM in D10 still has backup storage billed (orphaned backups); some prod VMs have no recent successful backup |
| Financial System (budget) | `application_code` | Some budget rows for Retired applications |

## 8. Answer key

Written to blob storage next to the data, **never** served by the API.

| File | Content |
|---|---|
| `answer_key.csv` | One row per defect: `defect_id, table, record_id, field, delivered_value, expected_value, canonical_record_id, expected_action` |
| `expected_totals.json` | Delivered and clean row counts per table; list of unowned RG ids; list of resources that should be Deleted |
| `master_truth.json` | Clean data shared with the other four generators |

## 9. Delivery

**Blob storage**, container `cmdb`: `v1/applications.json`, `v1/resource_groups.json`, `v1/resources.json` (JSON arrays), and `answer-key/` for section 8.

**REST API** (Azure Function, key in the `x-functions-key` header):

| Endpoint | Table |
|---|---|
| `GET /api/cmdb/v1/applications` | applications |
| `GET /api/cmdb/v1/resource-groups` | resource_groups |
| `GET /api/cmdb/v1/resources` | resources |

Paging: `?page=1&pageSize=1000` (maximum 1000). Response body:

```json
{ "data": [ { } ], "page": 1, "pageSize": 1000, "totalCount": 10000, "nextPage": 2 }
```

`nextPage` is null on the last page. Null columns are written as `null`, not omitted.

## 10. Acceptance checklist

1. Same `seed` gives identical files.
2. The clean data passes section 5.
3. Each defect count in `answer_key.csv` matches section 6 (percentages rounded to whole rows).
4. Every `record_id` in the answer key exists in the delivered data.
5. Only synthetic names and `zontique.com` emails.
6. The default run finishes in under a minute.
