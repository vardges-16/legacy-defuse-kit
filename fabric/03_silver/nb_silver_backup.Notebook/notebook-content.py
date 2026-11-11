# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_silver_backup: VM Backups -> Silver
# Out: `silver_backup_jobs`, `silver_backup_storage`.
# | Defect | Action |
# |---|---|
# | VM name written differently (`VM-...`, `....zontique.local`) | fix: canonical lower-case short name |
# | timestamps as `dd.MM.yyyy HH:mm` | fix: multi-format parse |
# | job repeated in the next day's file | merge on `job_id` |
# | Completed job with size <= 0 | quarantine |
# | VM not in the CMDB | flag `vm_in_cmdb = false` |
# | backup storage still billed after the VM was deleted | flag `is_orphaned` |
# Unprotected production VMs (no completed job in the last 7 days) are computed in Gold.

# CELL ********************

%run nb_ledger_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

PIPELINE_RUN_ID = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

RUN_ID = new_run_id(PIPELINE_RUN_ID)
NB = "nb_silver_backup"
vm_name = lambda c: F.regexp_replace(F.lower(F.trim(c)), r"\.zontique\.local$", "")
rg_of = lambda c: F.lower(F.regexp_extract(c, r"(?i)/resourcegroups/([^/]+)", 1))

j = bronze("backup_jobs")
n_j = j.count()
j = parse_ts(j, "job_start_utc", "job_start")
j = parse_ts(j, "job_end_utc", "job_end")
j = (j.withColumn("vm_name", vm_name(F.col("vm_name_raw") if "vm_name_raw" in j.columns else F.col("vm_name")))
      .withColumn("backup_size_gb", num("backup_size_gb"))
      .withColumn("timestamp_format_fixed", ~F.col("job_start_utc").rlike(r"^\d{4}-\d{2}-\d{2}")))
j, n_m = dedupe(j, "vm_backup_jobs", ["job_id"], RUN_ID, order_by=[F.col("_source_file")], reason="FILE_OVERLAP")
bad = j.filter((F.col("status") == "Completed") & (F.coalesce(F.col("backup_size_gb"), F.lit(0)) <= 0))
n_q = quarantine(bad, "vm_backup_jobs", "COMPLETED_WITH_SIZE_LE_0", RUN_ID, key_col="job_id")
j = j.join(bad.select("_row_hash"), "_row_hash", "left_anti")

scopes = silver("silver_cmdb_scopes").select(F.col("scope_name").alias("resource_group"), "application_code", "cost_center",
                                              F.lit(True).alias("vm_in_cmdb"))
vm_cost = (silver("silver_cost").filter(F.col("service_name") == "Virtual Machines")
           .groupBy(F.lower("resource_name").alias("vm_name")).agg(F.max("charge_date").alias("vm_last_cost_date")))


def enrich(df):
    return (df.withColumn("resource_group", rg_of("vm_resource_id")).join(F.broadcast(scopes), "resource_group", "left")
              .join(vm_cost, "vm_name", "left").fillna(False, ["vm_in_cmdb"]))


jobs = enrich(j).select("job_id", "vm_name", F.col("vm_name").alias("vm_key"), "resource_group", "application_code",
                        "cost_center", "vm_in_cmdb", "vault_name", "policy_name", "job_start", "job_end",
                        F.to_date("job_start").alias("job_date"), "status", "backup_size_gb", "error_code",
                        "source_region", "timestamp_format_fixed", "_schema_version", "_source_file", "_row_hash")

s = bronze("backup_storage")
n_s = s.count()
s = (s.withColumn("vm_name", vm_name(F.col("vm_name"))).withColumn("retained_storage_gb", num("retained_storage_gb"))
      .withColumn("storage_cost_usd", num("storage_cost_usd")).withColumn("month", F.to_date(F.concat("month", F.lit("-01")))))
sto = (enrich(s)
       .withColumn("is_orphaned", (F.col("protection_state") == "ProtectionStopped") &
                   (F.col("vm_last_cost_date").isNull() | (F.col("vm_last_cost_date") < F.col("month"))))
       .select("month", "vm_name", "resource_group", "application_code", "cost_center", "vm_in_cmdb", "vault_name",
               "protection_state", "retained_storage_gb", "storage_cost_usd", "vm_last_cost_date", "is_orphaned",
               "_source_file", "_row_hash"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

nj = silver_write(jobs, "silver_backup_jobs")
ns = silver_write(sto, "silver_backup_storage")
log_step(RUN_ID, "silver", "vm_backup_jobs", "bkp.backup_jobs", "silver_backup_jobs", rows_out=nj, rows_in=n_j,
         quarantined=n_q, merged=n_m, notebook=NB)
log_step(RUN_ID, "silver", "vm_backup_storage", "bkp.backup_storage", "silver_backup_storage", rows_out=ns, rows_in=n_s,
         amount=silver("silver_backup_storage").select(F.sum("storage_cost_usd")).first()[0], notebook=NB)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
