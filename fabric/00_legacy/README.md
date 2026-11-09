# Legacy Cost Report (Zontique, "before" state)

Power BI project (PBIP) that imitates the legacy report: Import mode, wired straight to the sources, logic hidden in the report, dated look (grey background, navy/yellow/orange, Times/Courier fonts, "FINAL_v7" header). It is the "before" picture; the Trust Ledger and the FinOps report are the "after".

## Sources (all in Snowflake `ZONTIQUE_LEGACY`, user `LEGACY_PBI_USER`, role `LEGACY_REPORT_ROLE`)
| Table | Source | Built-in flaw |
|---|---|---|
| `Cost by App` | View `RPT.V_COST_BY_APP` | Exact `application` tag only and BilledCost: untagged cost, tag variants and Google labels are invisible |
| `Budget` | View `RPT.V_BUDGET` (FY2024 to FY2026) | Monthly budget = all fiscal years added together and divided by the number of months; fiscal year is never matched to the cost month |
| `App Cost Center Map` | Hard-coded table ("Finance Excel, 2023") | 15 of 20 applications mapped; E-Commerce on the wrong cost center, General Ledger on closed cost center `0999` |

Hidden logic: calculated column `cost_center_code` (LOOKUPVALUE on the map) and measure `Actual (mapped)` (TREATAS on budget cost centers).

## Pages
- **Cost by Application**: slicers Application (dropdown) and Month; cards; bar chart by SolutionId.
- **Budget vs Actual**: slicers Business Unit and Department; cards; table by cost center.

## Expected numbers (all months, no slicer; seed 42 data)
| Figure | Value (USD) |
|---|---|
| Total Cost (tagged) | 1,903,002 |
| Applications (distinct SolutionId values, tag variants counted separately) | 52 |
| Unmapped Cost (application not in the map) | 531,664 |
| Silently lost (mapped to closed cost center `0999`, shown nowhere) | 37,446 |
| Untagged cost (not in the view at all) | 681,710 |
| Real total (FOCUS BilledCost) | 2,584,712 |

October 2026 only: tagged 90,625, of which 15,443 unmapped and 1,555 on `0999`.

Demo line: the legacy report accounts for 1.90M of 2.58M billed (74%); the Trust Ledger explains the other 0.68M. Its cost-center view then compares a three-year budget average with the latest month.

## Setup in the workspace
1. Semantic model **Legacy Cost Report** → Settings → Data source credentials → Snowflake: Basic, user `LEGACY_PBI_USER`, password from Key Vault `kv-ldk-2446996` (secret `snowflake-legacy-pbi-pwd`).
2. Refresh the semantic model once. The report then shows the numbers above.
