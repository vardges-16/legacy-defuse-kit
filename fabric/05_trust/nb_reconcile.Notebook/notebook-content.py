# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "e9ad44cc-27b2-b904-44c6-8fb17be9c76d",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_reconcile: Trust Ledger reconciliation
# Proves that nothing got lost: **source -> Bronze -> Silver -> Gold**, per entity.
# * Source: Snowflake queried live (user `LEDGER_READER`, password from Key Vault); VM Backups from the file manifest;
#   License (Azure SQL) from totals checked by hand.
# * Bronze = Silver + quarantined + merged, and every Bronze `_row_hash` is found in one of them (no silent loss).
# * Silver amount = Gold amount.
# Writes one row per check to `meta_reconciliation` and one row per entity to `meta_trust_summary` (PASS / FAIL / PENDING).

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SLV = "lh_silver.Lakehouse/Tables/dbo"
GLD = "wh_gold.Warehouse/Tables/dbo"
# source: sf = Snowflake table queried live; files = file manifest; rows = totals checked by hand
ENTITIES = {
    "cost": dict(source=dict(sf="COST.FOCUS_COST", sf_amount="BILLEDCOST"), bronze="FOCUS_COST", amount="billedcost",
                 silver=["silver_cost"], silver_amount="billed_cost", gold="fact_cost", gold_amount="billed_cost"),
    "budget": dict(source=dict(sf="FIN.BUDGET", sf_amount="BUDGET_COST"), bronze="BUDGET", amount="budget_cost",
                   silver=["silver_budget"], silver_amount="budget_cost_source",
                   silver_gold_amount="budget_cost_usd", gold="fact_budget", gold_amount="budget_cost"),
    "cmdb_assets": dict(source=dict(sf="CMDB.ASSETS"), bronze="ASSETS",
                        silver=["silver_cmdb_applications", "silver_cmdb_scopes"]),
    "cmdb_asset_groups": dict(source=dict(sf="CMDB.ASSET_GROUPS"), bronze="ASSET_GROUPS", silver=["silver_cmdb_asset_groups"]),
    "vm_backup_jobs": dict(source=dict(files="vm_backup_jobs"), bronze="backup_jobs", silver=["silver_backup_jobs"]),
    "vm_backup_storage": dict(source=dict(files="vm_backup_storage"), bronze="backup_storage", amount="storage_cost_usd",
                              silver=["silver_backup_storage"], silver_amount="storage_cost_usd",
                              gold="fact_backup_cost", gold_amount="storage_cost_usd"),
    "license_products": dict(source=dict(rows=8, note="checked by hand in Azure SQL"), bronze="license_products",
                             silver=["silver_license_products"]),
    "license_entitlements": dict(source=dict(rows=13, note="checked by hand in Azure SQL"), bronze="license_entitlements",
                                 silver=["silver_license_entitlements"]),
    "license_installations": dict(source=dict(rows=177, note="checked by hand in Azure SQL"), bronze="installations",
                                  silver=["silver_license_installations"]),
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
manifest = trust_read("meta_file_manifest")


def col_ci(df, name):
    if df is None or not name:
        return None
    return next((c for c in df.columns if c.lower() == name.lower()), None)


def amount_sum(df, name):
    c = col_ci(df, name)
    return None if c is None else float(df.select(F.sum(F.expr(f"try_cast(`{c}` as double)"))).first()[0] or 0.0)


def latest_run(entity, layer):
    if ledger is None:
        return None
    r = (ledger.filter((F.col("layer") == layer) & (F.col("entity") == entity))
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
    bronze = read_item(BRONZE[cfg["bronze"]])
    if bronze is None:
        check("0 bronze table exists", "bronze", None, "-", None, f"not found: {BRONZE[cfg['bronze']]}")
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
        elif "files" in src:
            brun = latest_run(entity, "bronze")
            mf = None if manifest is None or brun is None else manifest.filter((F.col("entity") == src["files"]) & (F.col("run_id") == brun))
            s_rows = None if mf is None else int(mf.select(F.sum("row_count")).first()[0] or 0)
            s_amt, note = None, f"file manifest, bronze run {brun}"
        else:
            s_rows, s_amt, note = src.get("rows"), src.get("amount"), src.get("note", "")
        s["source_rows"] = s_rows
        check("1 rows: source = bronze", "source", s_rows, "bronze", s["bronze_rows"], note)
        if amt and s_amt is not None:
            check("1 amount: source = bronze", "source", s_amt, "bronze", s["bronze_amount"], note, TOLERANCE)

        # 2. Bronze = Silver + quarantined + merged; no Bronze row lost
        silvers = [read_item(f"{SLV}/{t}") for t in cfg["silver"]]
        srun = latest_run(entity, "silver")
        s["silver_run_id"] = srun
        if any(x is None for x in silvers) or srun is None:
            check("2 rows: bronze = silver + quarantined + merged", "bronze", s["bronze_rows"], "silver", None,
                  "silver table not built yet" if srun else "silver step not logged")
        else:
            q, m = log_rows(qlog, entity, srun), log_rows(mlog, entity, srun)
            s["silver_rows"] = sum(x.count() for x in silvers)
            s["quarantined_rows"] = q.count() if q is not None else 0
            s["merged_rows"] = m.count() if m is not None else 0
            check("2 rows: bronze = silver + quarantined + merged", "bronze", s["bronze_rows"], "silver+q+m",
                  s["silver_rows"] + s["quarantined_rows"] + s["merged_rows"],
                  f"quarantined {s['quarantined_rows']}, merged {s['merged_rows']}, silver run {srun}")

            b_c = col_ci(bronze, amt)
            bh = with_row_hash(bronze).select("_row_hash", (F.expr(f"try_cast(`{b_c}` as double)") if b_c else F.lit(0.0)).alias("_amt"))
            known = silvers[0].select("_row_hash")
            for x in silvers[1:] + [q, m]:
                if x is not None:
                    known = known.unionByName(x.select("_row_hash"))
            s["lost_rows"] = bh.join(known.distinct(), "_row_hash", "left_anti").count()
            check("2 rows: bronze rows not found anywhere = 0", "lost", s["lost_rows"], "expected", 0,
                  "row hash not in silver, quarantine or merge log")

            if amt:
                per_hash = bh.dropDuplicates(["_row_hash"])
                side = lambda log: 0.0 if log is None else float(
                    log.select("_row_hash").join(per_hash, "_row_hash").select(F.sum("_amt")).first()[0] or 0.0)
                s["silver_amount"] = amount_sum(silvers[0], cfg.get("silver_amount"))
                total = None if s["silver_amount"] is None else s["silver_amount"] + side(q) + side(m)
                check("2 amount: bronze = silver + quarantined + merged", "bronze", s["bronze_amount"], "silver+q+m",
                      total, "silver amount before corrections", TOLERANCE)

        # 3. Silver = Gold
        if cfg.get("gold"):
            gold = read_item(f"{GLD}/{cfg['gold']}")
            s["gold_amount"] = amount_sum(gold, cfg.get("gold_amount"))
            sg = amount_sum(silvers[0], cfg.get("silver_gold_amount", cfg.get("silver_amount"))) if silvers[0] is not None else None
            check("3 amount: silver = gold", "silver", sg, "gold", s["gold_amount"],
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
display(res.select("entity", "check_name", "left_value", "right_value", "difference", "status", "note").orderBy("entity", "check_name"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
