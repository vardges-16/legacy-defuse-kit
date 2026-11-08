# Legacy Cost Report (Zontique, "before" state)

Power BI project (PBIP) that imitates the legacy report: Import mode, wired straight to the sources, logic hidden in the report, dated look (grey background, navy/yellow/orange, Times/Courier fonts, "FINAL_v7" header). Goes into the repo under `legacy/powerbi/`.

## Sources
| Table | Source | Built-in flaw |
|---|---|---|
| `Cost by App` | Snowflake view `ZONTIQUE_BILLING.RAW.V_COST_BY_APP` (user `LEGACY_PBI_USER`) | Tag-only, BilledCost: untagged cost is invisible |
| `Budget` | `fin-inputs/fiscalyear=2026/financial_budget.csv` (ADLS) | Budget year differs from the cost month; compared anyway |
| `App Cost Center Map` | Hard-coded table ("Finance Excel, 2023") | 14 of 24 applications mapped; one to closed cost center `0999` |

Hidden logic: calculated column `cost_center_code` (LOOKUPVALUE on the map) and measure `Actual (mapped)` (TREATAS on budget cost centers).

## Pages
- **Cost by Application**: slicers Application (dropdown) and Month; cards; bar chart by SolutionId.
- **Budget vs Actual**: slicers Business Unit and Department; cards; table by cost center.

## Expected numbers (June 2024 cost, FY2026 budget, no slicer selected)
| Figure | Value (USD) |
|---|---|
| Total Cost (tagged) | 186,500 |
| Applications | 24 |
| Monthly Budget | 250,000 |
| Actual (mapped) | 160,595 |
| Unmapped Cost | 21,386 |
| Silently lost (cost center 0999, shown nowhere) | 4,519 |
| Untagged cost (not in the view at all) | 21,255 |
| Real total (FOCUS BilledCost) | 207,756 |

Demo line: the legacy report accounts for 160.6k of 207.8k; the Trust Ledger explains the other 47.2k.
