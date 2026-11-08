# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0eec7aa2-09fd-454f-9d23-5c1b67ed94f8",
# META       "default_lakehouse_name": "lh_legacy",
# META       "default_lakehouse_workspace_id": "84598118-e0d7-451b-b901-affefb6f0cca",
# META       "known_lakehouses": [
# META         {
# META           "id": "0eec7aa2-09fd-454f-9d23-5c1b67ed94f8"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Bronze load: CMDB CSV files -> Delta tables in lh_legacy, exactly as received
from pyspark.sql import functions as F
import datetime, uuid

SOURCES = {                                   # target table : folder in lh_legacy
    "bronze_cmdb_asset_groups": "Files/cmdb/object=assetgroups",
    "bronze_cmdb_assets":       "Files/cmdb/object=assets",
}
RUN_ID = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
ledger = []

for table, path in SOURCES.items():
    raw = (spark.read
           .option("header", True)
           .option("inferSchema", False)          # keep every value as text: bad data stays visible
           .option("recursiveFileLookup", True)
           .csv(path))
    cols = raw.columns
    df = (raw
          .withColumn("_source_file", F.col("_metadata.file_path"))
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_run_id", F.lit(RUN_ID))
          .withColumn("_row_hash", F.sha2(F.concat_ws("||",
              *[F.coalesce(F.col(f"`{c}`"), F.lit("<null>")) for c in cols]), 256)))
    df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(table)

    loaded = spark.table(table)
    rows, files = loaded.count(), loaded.select("_source_file").distinct().count()
    ledger.append((RUN_ID, "cmdb", path, table, files, rows, datetime.datetime.utcnow()))
    print(f"{table}: {rows} rows from {files} file(s)")

(spark.createDataFrame(ledger,
    "run_id string, source_system string, source_path string, target_table string, "
    "file_count long, row_count long, logged_at timestamp")
 .write.format("delta").mode("append").saveAsTable("meta_load_ledger"))
display(spark.table("meta_load_ledger").orderBy(F.desc("logged_at")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
