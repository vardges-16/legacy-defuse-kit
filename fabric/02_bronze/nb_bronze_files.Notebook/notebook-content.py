# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0eec7aa2-09fd-454f-9d23-5c1b67ed94f8",
# META       "default_lakehouse_name": "lh_bronze",
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

%run nb_ledger_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

PIPELINE_RUN_ID = ""   # set by the pipeline; empty = new id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Bronze = mirrored tables (Snowflake, Azure SQL) seen through table shortcuts in lh_bronze.
# No copy: record one ledger row per table with what Bronze sees.
RUN_ID = new_run_id(PIPELINE_RUN_ID)
MIRRORED = {   # table in lh_bronze : (entity, source system, source)
    "FOCUS_COST":           ("cost",                  "snowflake", "ZONTIQUE_LEGACY.COST.FOCUS_COST"),
    "BUDGET":               ("budget",                "snowflake", "ZONTIQUE_LEGACY.FIN.BUDGET"),
    "ASSETS":               ("cmdb_assets",           "snowflake", "ZONTIQUE_LEGACY.CMDB.ASSETS"),
    "ASSET_GROUPS":         ("cmdb_asset_groups",     "snowflake", "ZONTIQUE_LEGACY.CMDB.ASSET_GROUPS"),
    "BACKUP_JOBS":          ("vm_backup_jobs",        "snowflake", "ZONTIQUE_LEGACY.VMB.BACKUP_JOBS"),
    "BACKUP_STORAGE":       ("vm_backup_storage",     "snowflake", "ZONTIQUE_LEGACY.VMB.BACKUP_STORAGE"),
    "installations":        ("license_installations", "license",   "sql-odl-epam-2446996-database.dbo.installations"),
    "license_entitlements": ("license_entitlements",  "license",   "sql-odl-epam-2446996-database.dbo.license_entitlements"),
    "license_products":     ("license_products",      "license",   "sql-odl-epam-2446996-database.dbo.license_products"),
}
AMOUNTS = {"FOCUS_COST": "BilledCost", "BUDGET": "budget_cost", "BACKUP_STORAGE": "storage_cost_usd"}

for table, (entity, system, src) in MIRRORED.items():
    df = read_item(f"lh_bronze.Lakehouse/Tables/dbo/{table}")
    if df is None:
        print(f"skip {table}: no shortcut in lh_bronze yet")
        continue
    amt_col = next((c for c in df.columns if c.lower() == AMOUNTS.get(table, "").lower()), None)
    amount = df.select(F.sum(F.col(f"`{amt_col}`").cast("double"))).first()[0] if amt_col else None
    rows = df.count()
    log_step(RUN_ID, "bronze", entity, src, table, rows_out=rows, amount=amount,
             source_system=system, notebook="nb_bronze_files")
    print(f"{table}: {rows} rows" + (f", {amt_col} {amount:,.2f}" if amount is not None else ""))

display(trust_read("meta_load_ledger").filter(F.col("run_id") == RUN_ID))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
