# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import synapse.ml.spark.aifunc as aifunc

aifunc.default_conf.set_deployment_name("gpt-5-mini")   # or "gpt-5.1"
aifunc.default_conf.set_reasoning_effort("low")
aifunc.default_conf.reset_temperature()

df = spark.createDataFrame([("MS SQL Svr 2019 Std",), ("Win Srv 2022 DC",), ("SQL Server 2022 Ent",)], ["raw"])
out = df.ai.classify(labels=["SQL Server 2019 Standard", "SQL Server 2022 Enterprise",
                             "Windows Server 2022 Datacenter"],
                     input_col="raw", output_col="product", error_col="ai_error")
display(out)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
