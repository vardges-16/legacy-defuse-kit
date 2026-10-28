# Legacy Defuse Kit — Software Architecture Document (SAD)

| | |
|---|---|
| **System** | Legacy Defuse Kit — AI-Accelerated Legacy-to-Fabric Modernization Factory |
| **Scenario company** | Zontique Corp |
| **Team** | de_dust2 |
| **Version** | 1.1 · 16 Oct 2026 |
| **Companion document** | Legacy Defuse Kit Blueprint (submission) |

---

## 1. Introduction

### 1.1 Purpose
This document describes the architecture of Legacy Defuse Kit: the main building blocks, how they connect, where they run, and the decisions behind them.

### 1.2 Scope
Legacy Defuse Kit modernizes the legacy reporting estate that connects five Zontique Corp systems and moves their data into Microsoft Fabric with proof that nothing was lost:

| # | Legacy system | Data | Role in the solution |
|---|---|---|---|
| 1 | Financial System | Budget per application and fiscal year | Budget side of budget vs. actual |
| 2 | Microsoft License & Inventory System | Microsoft licenses and installed instances | License compliance and cost |
| 3 | Snowflake | Actual cost from one or more cloud providers | Actual side of budget vs. actual |
| 4 | Cloud CMDB | Application to cloud resource group | Attributes cost to an application |
| 5 | VM Backups | Backup jobs, backup storage and backup cost per virtual machine | Backup cost per application; orphaned backups; unprotected VMs |

The legacy code pack (T-SQL stored procedures, SSIS packages, Snowflake DDL, BI report definition) is the as-is implementation that Legacy Defuse Kit analyzes and converts.

Out of scope for the hackathon: real-time streaming, direct cloud-billing feeds, production hardening beyond the sandbox.

### 1.3 Audience
Hackathon mentors and reviewers, the three-person build team, and anyone extending the solution afterwards.

### 1.4 Terms

| Term | Meaning |
|---|---|
| Medallion | Bronze (raw), Silver (cleaned), Gold (modeled) storage layers |
| Trust Ledger | Control tables that record evidence for every run: counts, hashes, totals, AI decisions |
| Answer key | The list of planted defects and expected totals produced with the synthetic data |
| Report sharing | Giving a person or group access to one Power BI report without any workspace role |

---

## 2. Architectural Goals and Constraints

### 2.1 Goals (quality attributes)

| Goal | What it means here | How the architecture meets it |
|---|---|---|
| Integrity | No row lost or changed without a record | Row hashes, control totals and counts reconciled per layer (Trust Ledger) |
| Auditability | Every number and every AI decision can be traced | Purview lineage, `ai_decisions` log, append-only ledger, Git history |
| Security | Only the right people reach the data | Entra ID, report-level sharing, PII hashing, labels |
| Explainability | People understand why the system decided something | Copilot-written reasons for placement; reason codes for quarantine |

---

## 3. System Context

![Solution overview](diagrams/01_solution_overview.png)

**External actors**

| Actor | Interaction |
|---|---|
| FinOps analysts | Use the Trust Dashboard; run reconciliation drill-through |
| Finance leaders | See budget vs. actual and trust score |
| Department / cost-center owners | Self-serve spend and budget views |
| Engineering | Review schema drift and Microsoft license gaps |
| Data stewards | Check AI outcomes against data quality results |
| Data engineers (team) | Build, run and fix the platform |

**External systems:** Financial System, Microsoft License & Inventory System, Snowflake, Cloud CMDB and VM Backups (all simulated), Microsoft Entra ID, Microsoft Purview, a Git repository.

---

## 4. Architecture Overview

Legacy Defuse Kit has two halves:

- **A modernization path** (stages 1–2) that turns the legacy code pack into Fabric artifacts: an object inventory, placement decisions, converted T-SQL, pipelines and notebooks, all in Git.
- **A data path** (stages 3–5 plus consumption) that moves the data from the 5 systems through Bronze, Silver and Gold to a semantic model, with every layer writing evidence to the Trust Ledger.

Two cross-cutting concerns span both: **governance and security** (Entra ID, Purview, Git) and **proof** (Trust Ledger).

![Technical architecture](diagrams/02_technical_architecture.png)

---

## 5. Process View — Key Runtime Flows

![Runtime and AI iteration workflow](diagrams/04_user_workflow.png)

### 5.1 Batch run (happy path)
1. The orchestrator starts a batch (`batch_id`) and reads active rows from `source_registry`.
2. Mirrored sources are already current in OneLake; pipeline sources are copied into Bronze. For each source, control totals are queried at the source at extraction time and stored in `recon_ledger`.
3. Bronze rows get technical columns and a row hash. Source vs. Bronze counts and totals are compared.
4. Silver cleansing runs. Every row ends in exactly one place: Silver, merged into another row, or quarantine. In = out + merged + quarantined is checked.
5. Gold is rebuilt for the affected periods. Silver vs. Gold control totals (per application and fiscal year) are compared.
6. If every check passes, the batch is marked `succeeded` and the Direct Lake model picks up the new data. If any check fails, the batch is marked `failed` and the dashboard shows the last good batch plus the failure.

### 5.2 AI iteration loop
Code changes made with AI help (converted procedures, pipelines, cleansing rules) are made in iterations. Each iteration is evaluated against the intermediate data quality checks (after Bronze and Silver) or the final checks (Gold) before the change is kept; the exact review approach is left open.
