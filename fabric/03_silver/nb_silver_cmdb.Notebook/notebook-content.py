# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_silver_cmdb: Cloud CMDB -> Silver
# Out: `silver_cmdb_applications`, `silver_cmdb_scopes` (Azure resource groups + GCP projects), `silver_cmdb_asset_groups`.
# # | Step | Defects handled | How |
# |---|---|---|
# | Dates, timestamps, cost center | mixed date formats, `+00:00`, lost leading zero | rules |
# | Duplicates | duplicate application / scope (upper-case name), second group for one app | merge, logged |
# | Department | "Mktg", "IT Ops"... | rules, then **AI classify** into the finance departments (gated) |
# | Scope owner | scope without group, stale group mapping | application code read from the scope name; conflicts flagged |
# | Flags | retired app owning active scopes, mapping > 18 months, owner email invalid | flags (not dropped) |
# | PII | owner name, email | hashed |

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
NB = "nb_silver_cmdb"
a = bronze("ASSETS")
g = bronze("ASSET_GROUPS")
n_assets, n_groups = a.count(), g.count()

a = parse_date(a, "created_date", "created_dt")
a = parse_date(a, "deleted_date", "deleted_dt")
a = parse_date(a, "mapping_last_verified", "mapping_verified_dt")
a = parse_ts(a, "last_modified_utc", "last_modified_ts")
a = a.withColumn("cost_center", F.when(F.trim("cost_center") != "", F.lpad(F.trim("cost_center"), 4, "0")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Applications: merge duplicates, canonical department (rules, then AI)

# CELL ********************

apps = a.filter(F.col("asset_type") == "BusinessApplication").withColumn("_name_key", norm_key(F.col("asset_name")))
apps, m_apps = dedupe(apps, "cmdb_assets", ["_name_key"], RUN_ID, order_by=[F.col("asset_id")])
alias = {r["asset_id"]: r["asset_id"] for r in apps.select("asset_id").collect()}
dups = (a.filter(F.col("asset_type") == "BusinessApplication").withColumn("_name_key", norm_key(F.col("asset_name")))
        .join(apps.select("_name_key", F.col("asset_id").alias("survivor_id")), "_name_key"))
for r in dups.collect():
    alias[r["asset_id"]] = r["survivor_id"]          # APP-021 -> APP-012

# canonical departments = the finance system's department names (most frequent spelling per department code)
bud = bronze("BUDGET")
canon = [r["department_name"] for r in bud.groupBy("department_code", "department_name").count()
         .withColumn("rk", F.row_number().over(Window.partitionBy("department_code").orderBy(F.desc("count"))))
         .filter("rk = 1").collect()]
canon_key = {pyn(c.replace("&", "and")): c for c in canon}
raw_depts = [r[0] for r in apps.select("department").distinct().collect() if r[0]]
dept_map = {d: canon_key[pyn(d.replace("&", "and"))] for d in raw_depts if pyn(d.replace("&", "and")) in canon_key}
leftover = [d for d in raw_depts if d not in dept_map]
dept_map.update(ai_resolve(leftover, "cmdb_assets", "department", canon, RUN_ID,
                           instructions="Map this company department name or abbreviation to the official department."))
apps = (apps.withColumn("department_raw", F.col("department"))
            .withColumn("department", map_values(F.col("department"), dept_map))
            .withColumn("department_method", F.when(F.col("department_raw").isin(list(set(dept_map) - set(leftover))), "RULE")
                        .when(F.col("department").isNotNull(), "AI").otherwise("UNRESOLVED")))

# owner: validate email, then hash name + email (PII)
apps = apps.withColumn("owner_email_valid", F.coalesce(F.col("owner_email").rlike(r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$"), F.lit(False)))
apps = hash_pii(apps, ["owner_name", "owner_email"])
silver_apps = apps.select(
    F.col("asset_id").alias("application_id"), F.col("asset_name").alias("application_name"),
    F.upper("application_code").alias("application_code"), "department", "department_raw", "department_method",
    "cost_center", "criticality", F.col("status").alias("lifecycle_status"),
    F.col("owner_name").alias("owner_name_hash"), F.col("owner_email").alias("owner_email_hash"), "owner_email_valid",
    F.col("created_dt").alias("created_date"), F.col("last_modified_ts").alias("last_modified_utc"), "_row_hash")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Asset groups: one group per application

# CELL ********************

g = parse_ts(g, "last_modified_utc", "last_modified_ts")
grp_use = a.groupBy("asset_group_id").count().withColumnRenamed("count", "_scopes")
g = g.join(grp_use, "asset_group_id", "left").fillna(0, ["_scopes"])
g, m_groups = dedupe(g, "cmdb_asset_groups", ["application_asset_id"], RUN_ID,
                     order_by=[F.desc("_scopes"), F.col("asset_group_id")])
group_app = {r["asset_group_id"]: alias.get(r["application_asset_id"], r["application_asset_id"]) for r in g.collect()}
silver_groups = g.select("asset_group_id", "asset_group_name", F.col("application_asset_id").alias("application_id"),
                         "group_type", F.col("last_modified_ts").alias("last_modified_utc"), "_row_hash")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Scopes: merge duplicates, resolve the owning application
# The application code is part of every scope name (`rg-<code>-...`, `prj-<code>-...`). The CMDB group mapping is
# often missing or stale, so the name wins and every disagreement is flagged.

# CELL ********************

app_code = {r["application_id"]: r["application_code"] for r in silver_apps.collect()}
code_app = {v: k for k, v in app_code.items()}
sc = a.filter(F.col("asset_type").isin("AzureResourceGroup", "GcpProject")).withColumn("scope_name", F.lower(F.trim("asset_name")))
sc, m_scopes = dedupe(sc, "cmdb_assets", ["scope_name"], RUN_ID,
                      order_by=[(F.col("asset_name") == F.lower(F.col("asset_name"))).desc(), F.col("asset_id")])
sc = (sc.withColumn("name_code", F.upper(F.split("scope_name", "-")[1]))
        .withColumn("group_app_id", map_values(F.col("asset_group_id"), group_app))
        .withColumn("group_code", map_values(F.col("group_app_id"), app_code))
        .withColumn("name_code", F.when(F.col("name_code").isin(list(code_app)), F.col("name_code"))))
sc = (sc.withColumn("application_code", F.coalesce("name_code", "group_code"))
        .withColumn("mapping_source", F.when(F.col("name_code").isNotNull() & F.col("group_code").isNull(), "NAME (no group)")
                    .when(F.col("name_code").isNotNull() & (F.col("name_code") != F.col("group_code")), "NAME (stale group)")
                    .when(F.col("name_code").isNotNull(), "NAME = GROUP").when(F.col("group_code").isNotNull(), "GROUP")
                    .otherwise("UNRESOLVED"))
        .withColumn("mapping_conflict", F.coalesce(F.col("name_code") != F.col("group_code"), F.lit(False)))
        .withColumn("mapping_stale", F.col("mapping_verified_dt").isNull() |
                    (F.col("mapping_verified_dt") < F.add_months(F.lit(AS_OF).cast("date"), -18))))
silver_scopes = (sc.join(silver_apps.select("application_code", "application_id",
                                            F.col("lifecycle_status").alias("app_lifecycle"), "cost_center")
                         .withColumnRenamed("cost_center", "app_cost_center"), "application_code", "left")
    .withColumn("retired_app_active_scope", F.col("app_lifecycle").isin("Retired", "Deprecated") & (F.col("status") == "Active"))
    .select(F.col("asset_id").alias("scope_id"), "scope_name", F.col("asset_type").alias("scope_type"),
            F.col("cloud_provider").alias("provider"), F.col("cloud_account_id").alias("account_id"),
            F.col("cloud_account_name").alias("account_name"), F.col("cloud_native_id").alias("native_id"),
            "environment", F.col("status").alias("cmdb_status"), F.col("deleted_dt").alias("cmdb_deleted_date"),
            "application_code", "application_id", F.col("app_cost_center").alias("cost_center"), "app_lifecycle",
            "mapping_source", "mapping_conflict", "mapping_stale", F.col("mapping_verified_dt").alias("mapping_last_verified"),
            "retired_app_active_scope", F.col("created_dt").alias("created_date"), "_row_hash"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

n_a = silver_write(silver_apps, "silver_cmdb_applications")
n_s = silver_write(silver_scopes, "silver_cmdb_scopes")
n_g = silver_write(silver_groups, "silver_cmdb_asset_groups")
log_step(RUN_ID, "silver", "cmdb_assets", "ASSETS", "silver_cmdb_applications + silver_cmdb_scopes", rows_out=n_a + n_s,
         rows_in=n_assets, merged=m_apps + m_scopes, notebook=NB)
log_step(RUN_ID, "silver", "cmdb_asset_groups", "ASSET_GROUPS", "silver_cmdb_asset_groups", rows_out=n_g,
         rows_in=n_groups, merged=m_groups, notebook=NB)
silver("silver_cmdb_scopes").groupBy("mapping_source").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
