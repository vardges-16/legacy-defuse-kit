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
# META     },
# META     "environment": {
# META       "environmentId": "fca9e1b6-f5b2-ac39-4093-43fd7058507c",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_orchestrator: Bronze + Silver in one Spark session
# Runs the intake and the five Silver notebooks in dependency order (in parallel where possible), then the data
# contracts. Pipeline `one_pipeline_to_rule_them_all`:
# **nb_orchestrator -> Stored procedure `wh_gold.dbo.usp_build_gold` -> nb_reconcile -> nb_ai_score**.

# PARAMETERS CELL ********************

PIPELINE_RUN_ID = ""
RUN_CONTRACTS = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import datetime, uuid
run_id = PIPELINE_RUN_ID or datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
args = {"PIPELINE_RUN_ID": run_id}


def step(name, deps=()):
    return {"name": name, "path": name, "timeoutPerCellInSeconds": 1800, "args": args, "dependencies": list(deps)}


dag = {"activities": [
    step("nb_bronze_files"),
    step("nb_silver_cmdb", ["nb_bronze_files"]),
    step("nb_silver_budget", ["nb_silver_cmdb"]),
    step("nb_silver_cost", ["nb_silver_cmdb"]),
    step("nb_silver_license", ["nb_silver_cost"]),
    step("nb_silver_backup", ["nb_silver_cost"]),
] + ([step("nb_silver_contract", ["nb_silver_budget", "nb_silver_license", "nb_silver_backup"])] if RUN_CONTRACTS else []),
    "timeoutInSeconds": 7200, "concurrency": 3}
result = notebookutils.notebook.runMultiple(dag, {"displayDAGViaGraphviz": True})
print("run id:", run_id)
print(result)
notebookutils.notebook.exit(run_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
