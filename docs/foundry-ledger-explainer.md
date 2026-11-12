# Foundry agent: ledger-explainer

**Legacy Defuse Kit · team de_dust2.** A Microsoft Foundry agent that answers executive and audit questions in plain language. It gets every figure from the Fabric data agent `trust-ledger-agent`; it never computes or invents numbers itself. Foundry items are not covered by Fabric Git integration, so this page is the source of truth for its configuration.

## Configuration
| Setting | Value |
|---|---|
| Foundry project | `ai-epam-2446996-project` |
| Agent | `ledger-explainer` (saved, not published) |
| Model | `gpt-5.4-nano` (orchestration and wording only; the Fabric data agent writes its own SQL) |
| Tool | Microsoft Fabric data agent → `trust-ledger-agent` (published) |
| Connection | `conn_fabric_trust_ledger`: custom keys `workspace-id` = `84598118-e0d7-451b-b901-affefb6f0cca`, `artifact-id` = `cda0780e-3111-4c10-9961-10b083fab1e8` (both marked secret) |
| Web search tool | Removed: answers come only from our data |

## Instructions
```
You are the Zontique Trust Ledger explainer for executives and auditors.
Your job: answer questions about IT/cloud spend and explain, in plain business language, whether the numbers can be trusted.

Always use the Fabric tool (trust-ledger-agent) for any figure: spend, budgets, unallocated cost, shadow IT, licenses, backups, reconciliation, lost or quarantined rows, pipeline runs. Never state a number that did not come from the tool. If the tool returns nothing, say so.

When giving a spend figure, also ask the tool for the latest reconciliation status and add one line: "Verified: X/9 sources reconciled, N rows lost."

Explain like a CFO briefing: lead with the answer, then why it matters, at most 5 sentences or a short table. Define jargon once (EffectiveCost = what we actually pay after discounts; unallocated = cost with no owner; shadow IT = cloud resources not in the CMDB).

Context you may use without the tool: the legacy report showed only $1.90M of $2.58M billed because it relied on exact application tags; the modernized pipeline reconciles every row from the five legacy systems to Gold, and quarantined or merged rows are recorded with a reason, never dropped silently.
```

## Test questions
| Question | Expected |
|---|---|
| Can I trust our cloud spend figure? | ≈ $2.58M effective cost, plus "9/9 reconciled, 0 rows lost" |
| Why did the legacy report show less than the real bill? | Tag-based legacy view ($1.90M) vs full FOCUS bill ($2.58M) |
| Which cost centers are over budget in 2026 and is that data complete? | List from `fact_budget_vs_actual`, plus reconciliation status |
| What happened in the last pipeline run? | Status, duration, failed or skipped steps from `meta_pipeline_runs` |

## Security and data handling
- **Identity passthrough (On-Behalf-Of):** the Fabric tool runs as the signed-in user. Each user needs access to `trust-ledger-agent`, `wh_gold` and `lh_trust`; service principals are not supported.
- **Read-only:** the data agent only reads `wh_gold` and `lh_trust`.
- **Data boundary:** answers returned to Foundry may leave the Fabric compliance boundary and follow Foundry's data handling terms. Acceptable here (synthetic data); for production, check this against the data classification.

## Performance
Each answer chains Foundry model → Fabric data agent (NL2SQL) → SQL endpoint → back, typically 20–60 s. For demos: warm up with 2–3 questions first, ask one topic per question, and cut waits in the recording.
