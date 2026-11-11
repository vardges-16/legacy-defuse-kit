# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4deb63e1-7a6b-4d89-bbba-230cbba39cf7",
# META       "default_lakehouse_name": "lh_trust",
# META       "default_lakehouse_workspace_id": "84598118-e0d7-451b-b901-affefb6f0cca",
# META       "known_lakehouses": [
# META         {
# META           "id": "4deb63e1-7a6b-4d89-bbba-230cbba39cf7"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

# %% parameters
run_id = ""     # pipeline run id
step = ""       # pipeline, bronze_silver, gold_build, reconcile, ai_score
status = ""     # STARTED, Succeeded, Failed, Skipped

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%
from pyspark.sql import Row
import datetime
now = datetime.datetime.utcnow()
spark.createDataFrame([Row(run_id=run_id, step=step, status=status, logged_at=now)]) \
     .write.format("delta").mode("append").saveAsTable("meta_pipeline_log")
print("logged", step, status, now)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
