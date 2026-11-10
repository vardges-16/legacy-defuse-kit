# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6c9baa84-5c32-41f1-80ae-e39555d6e429",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "84598118-e0d7-451b-b901-affefb6f0cca",
# META       "known_lakehouses": [
# META         {
# META           "id": "6c9baa84-5c32-41f1-80ae-e39555d6e429"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_silver_contract: data contracts as materialized lake views
# Each view is the "ready for Gold" contract of a Silver table. The CHECK constraints use **ON MISMATCH FAIL**:
# the Silver notebooks already quarantined every bad row with a reason code, so any violation here means a bug, and the
# refresh fails instead of silently dropping rows. Fabric shows the result in the lakehouse's **Materialized lake views**
# data-quality report and lineage view. An AI value still waiting for review (no service category) fails `cost_ready`:
# approve it in `nb_ai_review`, re-run `nb_silver_cost`, then this notebook.
# **Attach `lh_silver` as this notebook's default lakehouse** (MLVs live in the lakehouse of their sources).

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS lh_silver.contract

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW lh_silver.contract.cost_ready (
# MAGIC   CONSTRAINT charge_date_present CHECK (charge_date IS NOT NULL) ON MISMATCH FAIL,
# MAGIC   CONSTRAINT amounts_present CHECK (billed_cost IS NOT NULL AND effective_cost IS NOT NULL) ON MISMATCH FAIL,
# MAGIC   CONSTRAINT focus_category_known CHECK (service_category IS NOT NULL OR charge_category = 'Credit') ON MISMATCH FAIL,
# MAGIC   CONSTRAINT lineage_kept CHECK (_row_hash IS NOT NULL) ON MISMATCH FAIL
# MAGIC ) AS SELECT * FROM lh_silver.dbo.silver_cost

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW lh_silver.contract.budget_ready (
# MAGIC   CONSTRAINT month_present CHECK (budget_month IS NOT NULL) ON MISMATCH FAIL,
# MAGIC   CONSTRAINT amount_positive CHECK (budget_cost_usd >= 0) ON MISMATCH FAIL,
# MAGIC   CONSTRAINT cost_center_4_digits CHECK (length(cost_center_code) = 4) ON MISMATCH FAIL,
# MAGIC   CONSTRAINT lineage_kept CHECK (_row_hash IS NOT NULL) ON MISMATCH FAIL
# MAGIC ) AS SELECT * FROM lh_silver.dbo.silver_budget

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW lh_silver.contract.scopes_ready (
# MAGIC   CONSTRAINT scope_name_lower CHECK (scope_name = lower(scope_name)) ON MISMATCH FAIL,
# MAGIC   CONSTRAINT lineage_kept CHECK (_row_hash IS NOT NULL) ON MISMATCH FAIL
# MAGIC ) AS SELECT * FROM lh_silver.dbo.silver_cmdb_scopes

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW lh_silver.contract.backup_jobs_ready (
# MAGIC   CONSTRAINT job_id_present CHECK (job_id IS NOT NULL) ON MISMATCH FAIL,
# MAGIC   CONSTRAINT completed_has_size CHECK (status <> 'Completed' OR backup_size_gb > 0) ON MISMATCH FAIL,
# MAGIC   CONSTRAINT lineage_kept CHECK (_row_hash IS NOT NULL) ON MISMATCH FAIL
# MAGIC ) AS SELECT * FROM lh_silver.dbo.silver_backup_jobs

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC REFRESH MATERIALIZED LAKE VIEW lh_silver.contract.cost_ready FULL

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC REFRESH MATERIALIZED LAKE VIEW lh_silver.contract.budget_ready FULL

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC REFRESH MATERIALIZED LAKE VIEW lh_silver.contract.scopes_ready FULL

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC REFRESH MATERIALIZED LAKE VIEW lh_silver.contract.backup_jobs_ready FULL

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# notebook-triggered refreshes don't surface MLV errors: assert contract rows = Silver rows here
for mlv, tbl in [("cost_ready", "silver_cost"), ("budget_ready", "silver_budget"), ("scopes_ready", "silver_cmdb_scopes"),
                 ("backup_jobs_ready", "silver_backup_jobs")]:
    a, b = spark.table(f"lh_silver.contract.{mlv}").count(), spark.table(f"lh_silver.dbo.{tbl}").count()
    print(("OK  " if a == b else "FAIL"), mlv, a, "=", tbl, b)
    assert a == b, f"contract {mlv} has {a} rows, {tbl} has {b}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
