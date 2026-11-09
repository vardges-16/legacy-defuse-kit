# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_silver_budget: Financial budget -> Silver
# Out: `silver_budget` (one row per cost center and month, FY2026).
# # | Defect | Action |
# |---|---|
# | cost center without leading zero (`420`) | fix: pad to 4 digits |
# | month as `MM/dd/yyyy` | fix: multi-format parse |
# | department name variant | fix: canonical name per department code (rule, no AI needed) |
# | amount entered in thousands | fix x1000 when it is ~1/1000 of the cost center's normal month; flagged |
# | negative amount | quarantine |
# | line delivered twice | merge |
# | months missing / cost center unknown to the CMDB | flag (`meta_completeness`, `cc_has_application`) |

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
NB, ENTITY = "nb_silver_budget", "budget"
b = bronze("BUDGET")
n_in = b.count()
b = (b.withColumn("cost_center_code", F.lpad(F.trim("cost_center_code"), 4, "0"))
      .withColumn("budget_cost_num", num("budget_cost")))
b = parse_date(b, "budget_month", "budget_month_dt", ambiguous="MM/dd/yyyy")

# quarantine: negative or unreadable amount, unreadable month
bad = b.filter(F.col("budget_cost_num").isNull() | (F.col("budget_cost_num") < 0) | F.col("budget_month_dt").isNull())
reason = (F.when(F.col("budget_cost_num").isNull(), "AMOUNT_UNREADABLE").when(F.col("budget_cost_num") < 0, "NEGATIVE_AMOUNT")
          .otherwise("MONTH_UNREADABLE"))
n_q = quarantine(bad, ENTITY, reason, RUN_ID, key_col="cost_center_code")
good = b.join(bad.select("_row_hash"), "_row_hash", "left_anti")

# duplicates: one line per cost center and month
good, n_m = dedupe(good, ENTITY, ["cost_center_code", "budget_month_dt"], RUN_ID)

# amounts entered in thousands: ~1/1000 of the cost center's median month
med = good.groupBy("cost_center_code").agg(F.expr("percentile_approx(budget_cost_num, 0.5)").alias("_median"))
good = (good.join(med, "cost_center_code")
            .withColumn("amount_scaled_x1000", (F.col("budget_cost_num") * 1000 / F.col("_median")).between(0.5, 2.0))
            .withColumn("budget_cost_usd", F.when(F.col("amount_scaled_x1000"), F.col("budget_cost_num") * 1000)
                        .otherwise(F.col("budget_cost_num"))))

# department name: the most frequent spelling per department code
dept = (good.groupBy("department_code", "department_name").count()
        .withColumn("rk", F.row_number().over(Window.partitionBy("department_code").orderBy(F.desc("count"))))
        .filter("rk = 1").select("department_code", F.col("department_name").alias("department_canonical")))
good = good.join(dept, "department_code", "left")

# cost centers used by an application in the CMDB
cmdb_cc = [r[0] for r in silver("silver_cmdb_applications").select("cost_center").distinct().collect() if r[0]]
good = good.withColumn("cc_has_application", F.col("cost_center_code").isin(cmdb_cc))

out = good.select(
    "enterprise_code", "business_unit_code", "business_unit_name", "business_code", "business_name", "department_code",
    F.col("department_canonical").alias("department_name"), F.col("department_name").alias("department_name_raw"),
    "cost_center_code", "cost_center_name", "budget_owner_id", F.col("fiscal_year").cast("int").alias("fiscal_year"),
    F.col("budget_month_dt").alias("budget_month"), "budget_cost_usd", F.col("budget_cost_num").alias("budget_cost_source"),
    "amount_scaled_x1000", "cc_has_application",
    "currency", "_row_hash")
n_out = silver_write(out, "silver_budget")
log_step(RUN_ID, "silver", ENTITY, "BUDGET", "silver_budget", rows_out=n_out, rows_in=n_in, quarantined=n_q, merged=n_m,
         amount=silver("silver_budget").select(F.sum("budget_cost_usd")).first()[0], notebook=NB)

# completeness: 12 months per cost center
months = silver("silver_budget").groupBy("cost_center_code").agg(F.collect_set(F.month("budget_month")).alias("m")).collect()
for r in months:
    for mth in sorted(set(range(1, 13)) - set(r["m"])):
        log_gap(RUN_ID, ENTITY, "MISSING_BUDGET_MONTH", f"{r['cost_center_code']}|2026-{mth:02d}", layer="silver")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
