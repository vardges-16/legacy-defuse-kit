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

# MARKDOWN ********************

# # nb_reconcile: Trust Ledger reconciliation
# Proves that nothing got lost: **source → Bronze → Silver → Gold**. The source side is queried live in Snowflake
# (user `LEDGER_READER`, password from Key Vault); the License source (Azure SQL) uses totals checked by hand. Writes one row per check to `meta_reconciliation`
# and one row per source to `meta_trust_summary` (both in `lh_trust`), status PASS / FAIL / PENDING.
# 
# Silver side (see `docs/trust-ledger-contract.md`): Silver tables keep the Bronze `_row_hash`; rejects in
# `meta_quarantine`, merged duplicates in `meta_merge_log`, and each Silver notebook calls `log_step(..., layer="silver")`.
# Quarantine and merges are counted for the latest Silver run of each source (from `meta_load_ledger`).

# CELL ********************

%pip install -q snowflake-connector-python

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_ledger_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

PIPELINE_RUN_ID = ""                  # set by the pipeline; empty = new id
TOLERANCE = 0.01                      # USD difference allowed on amounts

BRONZE = "lh_bronze.Lakehouse/Tables/dbo"
SILVER = "lh_silver.Lakehouse/Tables/dbo"
GOLD = "wh_gold.Warehouse/Tables/dbo"

# source: "sf" = Snowflake table queried live (count + amount); else totals checked by hand
ENTITIES = {
    "cost": dict(source=dict(sf="COST.FOCUS_COST", sf_amount="BILLEDCOST"),
                 bronze=f"{BRONZE}/FOCUS_COST", amount="BilledCost",
                 silver=f"{SILVER}/silver_cost", silver_amount="billed_cost",
                 gold=f"{GOLD}/fact_cost", gold_amount="billed_cost"),
    "budget": dict(source=dict(sf="FIN.BUDGET", sf_amount="BUDGET_COST"),
                   bronze=f"{BRONZE}/BUDGET", amount="budget_cost",
                   silver=f"{SILVER}/silver_budget", silver_amount="budget_cost",
                   gold=f"{GOLD}/fact_budget", gold_amount="budget_cost"),
    "cmdb_assets": dict(source=dict(sf="CMDB.ASSETS"),
                        bronze=f"{BRONZE}/ASSETS", silver=f"{SILVER}/silver_cmdb_assets"),
    "cmdb_asset_groups": dict(source=dict(sf="CMDB.ASSET_GROUPS"),
                              bronze=f"{BRONZE}/ASSET_GROUPS", silver=f"{SILVER}/silver_cmdb_asset_groups"),
    "vm_backup_jobs": dict(source=dict(sf="VMB.BACKUP_JOBS"),
                           bronze=f"{BRONZE}/BACKUP_JOBS", silver=f"{SILVER}/silver_vm_backup_jobs"),
    "vm_backup_storage": dict(source=dict(sf="VMB.BACKUP_STORAGE", sf_amount="STORAGE_COST_USD"),
                              bronze=f"{BRONZE}/BACKUP_STORAGE", amount="storage_cost_usd",
                              silver=f"{SILVER}/silver_vm_backup_storage", silver_amount="storage_cost_usd",
                              gold=f"{GOLD}/fact_backup_cost", gold_amount="storage_cost_usd"),
    "license_installations": dict(source=dict(rows=17, note="checked by hand in Azure SQL"),
                                  bronze=f"{BRONZE}/installations", silver=f"{SILVER}/silver_license_installations"),
    "license_entitlements": dict(source=dict(rows=17, note="checked by hand in Azure SQL"),
                                 bronze=f"{BRONZE}/license_entitlements", silver=f"{SILVER}/silver_license_entitlements"),
    "license_products": dict(source=dict(rows=4, note="checked by hand in Azure SQL"),
                             bronze=f"{BRONZE}/license_products", silver=f"{SILVER}/silver_license_products"),
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

RUN_ID = new_run_id(PIPELINE_RUN_ID)
NOW = datetime.datetime.utcnow()
results, summary = [], []
ledger, qlog, mlog = trust_read("meta_load_ledger"), trust_read("meta_quarantine"), trust_read("meta_merge_log")


def col_ci(df, name):
    """Case-insensitive column lookup (mirrored Snowflake columns are upper case)."""
    if df is None or not name:
        return None
    return next((c for c in df.columns if c.lower() == name.lower()), None)


def amount_sum(df, name):
    c = col_ci(df, name)
    return None if c is None else float(df.select(F.sum(F.col(f"`{c}`").cast("double"))).first()[0] or 0.0)


def latest_silver_run(entity):
    if ledger is None or "layer" not in ledger.columns:
        return None
    r = (ledger.filter((F.col("layer") == "silver") & (F.col("entity") == entity))
         .orderBy(F.desc("logged_at")).select("run_id").first())
    return r[0] if r else None


def log_rows(log, entity, run_id):
    return None if log is None else log.filter((F.col("entity") == entity) & (F.col("run_id") == run_id))


for entity, cfg in ENTITIES.items():
    s = dict(source_rows=None, bronze_rows=None, silver_rows=None, quarantined_rows=None, merged_rows=None,
             lost_rows=None, bronze_amount=None, silver_amount=None, gold_amount=None, silver_run_id=None)
    st = []

    def check(name, ll, lv, rl, rv, note="", tol=0.0):
        if lv is None or rv is None:
            diff, status = None, "PENDING"
        else:
            diff = round(float(lv) - float(rv), 6)
            status = "PASS" if abs(diff) <= tol else "FAIL"
        results.append((RUN_ID, entity, name, ll, None if lv is None else float(lv), rl,
                        None if rv is None else float(rv), diff, status, note, NOW))
        st.append(status)

    amt = cfg.get("amount")
    bronze = read_item(cfg["bronze"])
    if bronze is None:
        check("0 bronze table exists", "bronze", None, "-", None, f"not found: {cfg['bronze']}")
    else:
        s["bronze_rows"], s["bronze_amount"] = bronze.count(), amount_sum(bronze, amt)

        # 1. Source = Bronze
        src = cfg["source"]
        if "sf" in src:
            try:
                a = f"SUM(TRY_TO_DOUBLE({src['sf_amount']}::VARCHAR))" if src.get("sf_amount") else "NULL"
                r = sf_query(f"SELECT COUNT(*), {a} FROM {SF['database']}.{src['sf']}")[0]
                s_rows, s_amt, note = int(r[0]), (None if r[1] is None else float(r[1])), f"queried Snowflake {src['sf']}"
            except Exception as e:
                s_rows, s_amt, note = None, None, f"Snowflake query failed: {str(e)[:150]}"
        else:
            s_rows, s_amt, note = src.get("rows"), src.get("amount"), src.get("note", "")
        s["source_rows"] = s_rows
        check("1 rows: source = bronze", "source", s_rows, "bronze", s["bronze_rows"], note)
        if amt:
            check("1 amount: source = bronze", "source", s_amt, "bronze", s["bronze_amount"], note, TOLERANCE)

        # 2. Bronze = Silver + quarantined + merged; no Bronze row lost
        silver = read_item(cfg["silver"]) if cfg.get("silver") else None
        srun = latest_silver_run(entity)
        s["silver_run_id"] = srun
        if silver is None or "_row_hash" not in silver.columns or srun is None:
            why = ("silver table not built yet" if silver is None else
                   "silver table has no _row_hash" if "_row_hash" not in silver.columns else
                   "silver step not logged (log_step layer='silver')")
            check("2 rows: bronze = silver + quarantined + merged", "bronze", s["bronze_rows"], "silver", None, why)
        else:
            q, m = log_rows(qlog, entity, srun), log_rows(mlog, entity, srun)
            s["silver_rows"] = silver.count()
            s["quarantined_rows"] = q.count() if q is not None else 0
            s["merged_rows"] = m.count() if m is not None else 0
            check("2 rows: bronze = silver + quarantined + merged", "bronze", s["bronze_rows"], "silver+q+m",
                  s["silver_rows"] + s["quarantined_rows"] + s["merged_rows"],
                  f"quarantined {s['quarantined_rows']}, merged {s['merged_rows']}, silver run {srun}")

            b_c = col_ci(bronze, amt)
            bh = with_row_hash(bronze).select(
                "_row_hash", (F.col(f"`{b_c}`").cast("double") if b_c else F.lit(0.0)).alias("_amt"))
            known = silver.select("_row_hash")
            for log in (q, m):
                if log is not None:
                    known = known.unionByName(log.select("_row_hash"))
            s["lost_rows"] = bh.join(known.distinct(), "_row_hash", "left_anti").count()
            check("2 rows: bronze rows not found anywhere = 0", "lost", s["lost_rows"], "expected", 0,
                  "row hash not in silver, quarantine or merge log")

            if amt:
                per_hash = bh.dropDuplicates(["_row_hash"])
                side = lambda log: 0.0 if log is None else float(
                    log.select("_row_hash").join(per_hash, "_row_hash").select(F.sum("_amt")).first()[0] or 0.0)
                s["silver_amount"] = amount_sum(silver, cfg.get("silver_amount"))
                total = None if s["silver_amount"] is None else s["silver_amount"] + side(q) + side(m)
                check("2 amount: bronze = silver + quarantined + merged", "bronze", s["bronze_amount"], "silver+q+m",
                      total, "differs if Silver corrects amounts", TOLERANCE)

        # 3. Silver = Gold
        if amt:
            gold = read_item(cfg["gold"]) if cfg.get("gold") else None
            s["gold_amount"] = amount_sum(gold, cfg.get("gold_amount"))
            check("3 amount: silver = gold", "silver", s["silver_amount"], "gold", s["gold_amount"],
                  "gold table not built yet" if gold is None else "", TOLERANCE)

    status = "FAIL" if "FAIL" in st else "PENDING" if "PENDING" in st else "PASS"
    summary.append((RUN_ID, NOW, entity, status, s["source_rows"], s["bronze_rows"], s["silver_rows"],
                    s["quarantined_rows"], s["merged_rows"], s["lost_rows"], s["bronze_amount"], s["silver_amount"],
                    s["gold_amount"], st.count("PASS"), st.count("FAIL"), st.count("PENDING"), s["silver_run_id"]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

res = spark.createDataFrame(results, "run_id string, entity string, check_name string, left_layer string, "
                            "left_value double, right_layer string, right_value double, difference double, "
                            "status string, note string, checked_at timestamp")
summ = spark.createDataFrame(summary, "run_id string, checked_at timestamp, entity string, status string, "
                             "source_rows long, bronze_rows long, silver_rows long, quarantined_rows long, "
                             "merged_rows long, lost_rows long, bronze_amount double, silver_amount double, "
                             "gold_amount double, checks_pass long, checks_fail long, checks_pending long, "
                             "silver_run_id string")
trust_append(res, "meta_reconciliation")
trust_append(summ, "meta_trust_summary")

print(RUN_ID, {r["status"]: r["count"] for r in res.groupBy("status").count().collect()})
display(summ.drop("run_id", "checked_at"))
display(res.select("entity", "check_name", "left_value", "right_value", "difference", "status", "note")
           .orderBy("entity", "check_name"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
