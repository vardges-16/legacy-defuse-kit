# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_silver_license: License & Inventory (Azure SQL) -> Silver
# Out: `silver_license_products`, `silver_license_entitlements`, `silver_license_installations`.
#
# | Defect | Action |
# |---|---|
# | discovery product name variant, product id empty ("Win Srv 2022 DC") | rules, then **AI classify** into the 8 catalog products (gated) |
# | duplicate installation row | merge (same content, new id) |
# | core count empty | fix from the VM size in FOCUS (`SkuPriceDetails.CoreCount`) |
# | host not in cloud cost / CMDB | flag `host_in_cloud = false` (unmanaged) |
# | VM deleted but still reported | flag `vm_deleted_still_seen` |
# | BYOL Windows on a VM that pays Windows by the hour (PAYG) | flag `double_licensing` |
# | entitlement expired | flag `is_expired` |
# Over-deployment (installed BYOL cores > entitled cores) is computed in Gold (`fact_license_position`).

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
NB = "nb_silver_license"
p = bronze("license_products")
e = bronze("license_entitlements")
i = bronze("installations")
n_p, n_e, n_i = p.count(), e.count(), i.count()

products = {int(r["product_id"]): r["product_name"] for r in p.collect()}
pid_of = {v: k for k, v in products.items()}
by_key = {pyn(v): k for k, v in products.items()}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Installations: product (rules, then AI), duplicates, cores, flags

# CELL ********************

i = i.withColumn("product_id_num", F.expr("try_cast(product_id as int)"))
raw = [r[0] for r in i.filter(F.col("product_id_num").isNull()).select("raw_product_name").distinct().collect() if r[0]]
name_map = {v: products[by_key[pyn(v)]] for v in raw if pyn(v) in by_key}
rule_keys = list(name_map)
left = [v for v in raw if v not in name_map]
name_map.update(ai_resolve(left, "license_installations", "product", list(products.values()), RUN_ID,
                           instructions="Software found by a discovery tool on a server. Return the exact licensed product and edition."))
pid_map = {k: str(pid_of[v]) for k, v in name_map.items()}
i = (i.withColumn("product_id_resolved", F.coalesce(F.col("product_id_num"), map_values(F.col("raw_product_name"), pid_map).cast("int")))
      .withColumn("product_method", F.when(F.col("product_id_num").isNotNull(), "SOURCE")
                  .when(F.col("raw_product_name").isin(rule_keys), "RULE")
                  .when(F.col("product_id_resolved").isNotNull(), "AI").otherwise("UNRESOLVED")))

# duplicates: same host, product, dates, cores and model (only the id differs)
i = i.withColumn("host_key", F.lower(F.trim("host_name")))
i, n_m = dedupe(i, "license_installations",
                ["host_key", "raw_product_name", "install_date", "core_count", "license_model", "environment"], RUN_ID,
                order_by=[F.expr("try_cast(installation_id as bigint)")])

# hosts in the cloud bill: VM cores, last cost day, pay-as-you-go Windows charges
cost = silver("silver_cost")
vms = (cost.filter(F.col("service_name").isin("Virtual Machines", "Compute Engine") & F.col("resource_name").isNotNull())
       .groupBy(F.lower("resource_name").alias("host_key"))
       .agg(F.max(F.get_json_object("sku_price_details", "$.CoreCount").cast("int")).alias("vm_cores"),
            F.max("charge_date").alias("vm_last_cost_date")))
payg = (cost.filter(F.col("service_name") == "Virtual Machines Licenses")
        .select(F.lower("resource_name").alias("host_key")).distinct().withColumn("vm_pays_windows_payg", F.lit(True)))
i = (i.join(vms, "host_key", "left").join(payg, "host_key", "left")
      .withColumn("core_count_num", F.expr("try_cast(core_count as int)"))
      .withColumn("core_count_fixed", F.col("core_count_num").isNull() & F.col("vm_cores").isNotNull())
      .withColumn("cores", F.coalesce("core_count_num", "vm_cores"))
      .withColumn("host_in_cloud", F.col("vm_cores").isNotNull())
      .withColumn("vm_deleted_still_seen", F.col("vm_last_cost_date").isNotNull() &
                  (F.col("vm_last_cost_date") < F.date_sub(F.lit(AS_OF).cast("date"), 2)) &
                  (F.col("last_seen_date").cast("date") > F.col("vm_last_cost_date")))
      .withColumn("double_licensing", F.coalesce(F.col("vm_pays_windows_payg"), F.lit(False)) &
                  (F.col("license_model") == "BYOL") & F.col("product_id_resolved").between(1, 4)))
inst = i.select(F.col("installation_id").cast("bigint").alias("installation_id"), "host_name", "host_key", "raw_product_name",
                F.col("product_id_resolved").alias("product_id"), "product_method", "cores", "core_count_fixed",
                "license_model", "environment", F.col("install_date").cast("date").alias("install_date"),
                F.col("last_seen_date").cast("date").alias("last_seen_date"), "host_in_cloud", "vm_last_cost_date",
                "vm_deleted_still_seen", "double_licensing", "_row_hash")

ents = (e.withColumn("end_date", F.col("end_date").cast("date")).withColumn("start_date", F.col("start_date").cast("date"))
        .withColumn("is_expired", F.col("end_date") < F.lit(AS_OF).cast("date"))
        .select(F.col("entitlement_id").cast("int").alias("entitlement_id"), F.col("product_id").cast("int").alias("product_id"),
                "agreement_number", F.col("quantity").cast("int").alias("quantity_cores"), "start_date", "end_date",
                F.col("unit_cost_usd").cast("double").alias("unit_cost_usd"), "license_model", "is_expired", "_row_hash"))
prods = p.select(F.col("product_id").cast("int").alias("product_id"), "product_name", "product_family", "edition",
                 "license_metric", "_row_hash")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

n1 = silver_write(prods, "silver_license_products")
n2 = silver_write(ents, "silver_license_entitlements")
n3 = silver_write(inst, "silver_license_installations")
log_step(RUN_ID, "silver", "license_products", "license_products", "silver_license_products", rows_out=n1, rows_in=n_p, notebook=NB)
log_step(RUN_ID, "silver", "license_entitlements", "license_entitlements", "silver_license_entitlements", rows_out=n2, rows_in=n_e, notebook=NB)
log_step(RUN_ID, "silver", "license_installations", "installations", "silver_license_installations", rows_out=n3, rows_in=n_i,
         merged=n_m, notebook=NB)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
