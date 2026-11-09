# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_bronze_files: Bronze intake
# * **VM Backups** (files): daily job files and monthly storage files from `lh_bronze` Files `landing/vm-backups/`
#   -> `bkp.backup_jobs`, `bkp.backup_storage`. Every value stays text, as received. The schema drift
#   (`BACKUP_SIZE_GB` renamed `BackupSizeGB`, new `SOURCE_REGION`) is absorbed by an alias map, so no value is lost.
#   Each file is recorded in `meta_file_manifest`; missing days in `meta_completeness`.
# * **Mirrored tables** (Snowflake, Azure SQL): already in Bronze through shortcuts; only their counts and
#   amounts are logged, so the reconciliation can compare them with the source.

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
NB = "nb_bronze_files"
ALIASES = {"backupsizegb": "backup_size_gb"}   # drifted column name -> canonical name
import datetime as dt


def list_files(rel):
    """CSV files under a OneLake folder (name, path, size)."""
    try:
        items = notebookutils.fs.ls(f"{ROOT}/{rel}")
    except NameError:  # local test
        import os
        base = f"{ROOT}/{rel}".replace("file://", "")
        items = [type("F", (), dict(name=n, path=f"file://{base}/{n}", size=os.path.getsize(f"{base}/{n}")))
                 for n in sorted(os.listdir(base))]
    return sorted([(i.name, i.path, i.size) for i in items if i.name.lower().endswith(".csv")])


def read_csv_files(rel, entity):
    files = list_files(rel)
    frames, manifest = [], []
    for name, path, size in files:
        df = spark.read.option("header", True).option("inferSchema", False).csv(path)
        raw_cols = df.columns
        df = df.toDF(*[ALIASES.get(c.lower(), c.lower()) for c in raw_cols])
        version = "v2" if any(c.lower() in ALIASES for c in raw_cols) else "v1"
        df = (with_row_hash(df.withColumn("_schema_version", F.lit(version)))
              .withColumn("_source_file", F.lit(name)).withColumn("_loaded_at", F.current_timestamp()))
        n = df.count()
        frames.append(df)
        manifest.append((RUN_ID, entity, name, int(size), n, version, ",".join(raw_cols), dt.datetime.utcnow()))
    out = frames[0]
    for f in frames[1:]:
        out = out.unionByName(f, allowMissingColumns=True)
    trust_append(spark.createDataFrame(manifest, "run_id string, entity string, file_name string, bytes long, "
                                       "row_count long, schema_version string, header string, logged_at timestamp"),
                 "meta_file_manifest")
    return out, files


def write_bronze(df, name):
    df.write.format(FMT).mode("overwrite").option("overwriteSchema", "true").save(f"{ROOT}/{BRONZE[name]}")
    return read_item(BRONZE[name]).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## VM Backups files -> Bronze

# CELL ********************

jobs, jfiles = read_csv_files(f"{BKP_LANDING}/jobs", "vm_backup_jobs")
n_jobs = write_bronze(jobs, "backup_jobs")
log_step(RUN_ID, "bronze", "vm_backup_jobs", f"{BKP_LANDING}/jobs", "bkp.backup_jobs", rows_out=n_jobs,
         rows_in=jobs.count(), file_count=len(jfiles), source_system="VM Backups (files)", notebook=NB)

# completeness: one job file per day between the first and the last file
days = sorted(dt.date(int(n[12:16]), int(n[16:18]), int(n[18:20])) for n, _, _ in jfiles)
expected = [days[0] + dt.timedelta(i) for i in range((days[-1] - days[0]).days + 1)]
missing = [d for d in expected if d not in set(days)]
for d in missing:
    log_gap(RUN_ID, "vm_backup_jobs", "MISSING_DAILY_FILE", d, f"backup_jobs_{d:%Y%m%d}.csv not delivered")
print(f"job files: {len(jfiles)} delivered, {len(missing)} missing: {missing}")

storage, sfiles = read_csv_files(f"{BKP_LANDING}/storage", "vm_backup_storage")
n_sto = write_bronze(storage, "backup_storage")
log_step(RUN_ID, "bronze", "vm_backup_storage", f"{BKP_LANDING}/storage", "bkp.backup_storage", rows_out=n_sto,
         rows_in=storage.count(), file_count=len(sfiles), amount=storage.select(F.sum(num("storage_cost_usd"))).first()[0],
         source_system="VM Backups (files)", notebook=NB)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Mirrored tables: log what arrived (counts and control amounts)

# CELL ********************

MIRRORED = [  # entity, bronze name, source system, amount column
    ("cost", "FOCUS_COST", "Snowflake COST.FOCUS_COST", "billedcost"),
    ("cmdb_assets", "ASSETS", "Snowflake CMDB.ASSETS", None),
    ("cmdb_asset_groups", "ASSET_GROUPS", "Snowflake CMDB.ASSET_GROUPS", None),
    ("budget", "BUDGET", "Snowflake FIN.BUDGET", "budget_cost"),
    ("license_products", "license_products", "Azure SQL dbo.license_products", None),
    ("license_entitlements", "license_entitlements", "Azure SQL dbo.license_entitlements", None),
    ("license_installations", "installations", "Azure SQL dbo.installations", None),
]
for entity, name, system, amount_col in MIRRORED:
    try:
        df = bronze(name)
    except ValueError as e:
        print("SKIP", e)
        continue
    n = df.count()
    amt = df.select(F.sum(num(amount_col))).first()[0] if amount_col else None
    files = df.select("_source_file").distinct().count() if "_source_file" in df.columns else None
    log_step(RUN_ID, "bronze", entity, system, BRONZE[name], rows_out=n, rows_in=n, amount=amt,
             file_count=files, source_system=system, notebook=NB)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
