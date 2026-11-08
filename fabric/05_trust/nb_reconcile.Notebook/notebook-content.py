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

# MARKDOWN ********************

# # nb_reconcile: Trust Ledger reconciliation
# Proves that nothing got lost between layers: **source → Bronze → Silver → Gold**.
# Writes one row per check to `meta_reconciliation` (default lakehouse `lh_legacy`), status PASS / FAIL / PENDING.
# 
# Silver contract (for the Silver notebooks): every Silver table keeps the Bronze `_row_hash` of its row;
# rejected rows go to `silver_quarantine` (`entity`, `_row_hash`, `reason_code`); merged duplicates go to
# `silver_merge_log` (`entity`, `_row_hash`, `survivor_hash`). For mirrored sources without `_row_hash`,
# compute it with `with_hash()` below (same formula as `nb_bronze_files`).

# CELL ********************

# Parameters
WORKSPACE = "ldk-workspace"
ROOT = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com"
FMT = "delta"
TOLERANCE = 0.01                      # USD difference allowed on amounts
RESULT_TABLE = "meta_reconciliation"  # written to the default lakehouse (lh_legacy)

LEGACY = "lh_legacy.Lakehouse/Tables/dbo"
SILVER = "lh_silver.Lakehouse/Tables/dbo"
GOLD = "wh_gold.Warehouse/Tables/dbo"
QUARANTINE = f"{SILVER}/silver_quarantine"   # entity, _row_hash, reason_code
MERGE_LOG = f"{SILVER}/silver_merge_log"     # entity, _row_hash, survivor_hash

# source: "files" = re-read the raw files; otherwise totals checked by hand in the source system
ENTITIES = {
    "cost": dict(source=dict(rows=45051, amount=207755.805688061292, note="checked in Snowflake"),
                 bronze=f"{LEGACY}/FOCUS_COST", amount="BilledCost",
                 silver=f"{SILVER}/silver_cost", silver_amount="billed_cost",
                 gold=f"{GOLD}/fact_cost", gold_amount="billed_cost"),
    "budget": dict(source=dict(files="lh_legacy.Lakehouse/Files/fin"),
                   bronze=f"{LEGACY}/bronze_fin_budget", amount="budget_cost",
                   silver=f"{SILVER}/silver_budget", silver_amount="budget_cost",
                   gold=f"{GOLD}/fact_budget", gold_amount="budget_cost"),
    "cmdb_assets": dict(source=dict(files="lh_legacy.Lakehouse/Files/cmdb/object=assets"),
                        bronze=f"{LEGACY}/bronze_cmdb_assets", silver=f"{SILVER}/silver_cmdb_assets"),
    "cmdb_asset_groups": dict(source=dict(files="lh_legacy.Lakehouse/Files/cmdb/object=assetgroups"),
                              bronze=f"{LEGACY}/bronze_cmdb_asset_groups", silver=f"{SILVER}/silver_cmdb_asset_groups"),
    "license_installations": dict(source=dict(rows=17, note="checked in Azure SQL"),
                                  bronze=f"{LEGACY}/installations", silver=f"{SILVER}/silver_license_installations"),
    "license_entitlements": dict(source=dict(rows=17, note="checked in Azure SQL"),
                                 bronze=f"{LEGACY}/license_entitlements", silver=f"{SILVER}/silver_license_entitlements"),
    "license_products": dict(source=dict(rows=4, note="checked in Azure SQL"),
                             bronze=f"{LEGACY}/license_products", silver=f"{SILVER}/silver_license_products"),
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
import datetime, uuid

RUN_ID = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
LEDGER_COLS = {"_source_file", "_ingested_at", "_run_id", "_row_hash"}
results = []


def load(path):
    """Read a table by OneLake path; None if it doesn't exist (yet)."""
    try:
        return spark.read.format(FMT).load(f"{ROOT}/{path}")
    except Exception:
        return None


def col_ci(df, name):
    """Case-insensitive column lookup (mirrored Snowflake columns are upper case)."""
    if df is None or not name:
        return None
    return next((c for c in df.columns if c.lower() == name.lower()), None)


def with_hash(df):
    """Same row hash as nb_bronze_files; computed here for mirrored tables that have none."""
    if "_row_hash" in df.columns:
        return df
    cols = [c for c in df.columns if c not in LEDGER_COLS]
    parts = [F.coalesce(F.col(f"`{c}`").cast("string"), F.lit("<null>")) for c in cols]
    return df.withColumn("_row_hash", F.sha2(F.concat_ws("||", *parts), 256))


def amount_sum(df, name):
    c = col_ci(df, name)
    if c is None:
        return None
    return float(df.select(F.sum(F.col(f"`{c}`").cast("double"))).first()[0] or 0.0)


def record(entity, check, left_layer, left, right_layer, right, note="", tol=0.0):
    if left is None or right is None:
        diff, status = None, "PENDING"
    else:
        diff = round(float(left) - float(right), 6)
        status = "PASS" if abs(diff) <= tol else "FAIL"
    results.append((RUN_ID, entity, check, left_layer, None if left is None else float(left),
                    right_layer, None if right is None else float(right), diff, status, note,
                    datetime.datetime.utcnow()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

quarantine, merge_log = load(QUARANTINE), load(MERGE_LOG)


def entity_rows(log):
    return None if log is None else log.filter(F.col("entity") == entity)


for entity, cfg in ENTITIES.items():
    amt = cfg.get("amount")
    bronze = load(cfg["bronze"])
    if bronze is None:
        record(entity, "bronze table exists", "bronze", None, "-", None, f"not found: {cfg['bronze']}")
        continue
    b_rows, b_amt = bronze.count(), amount_sum(bronze, amt)

    # 1. Source = Bronze
    src = cfg["source"]
    if "files" in src:
        raw = (spark.read.option("header", True).option("recursiveFileLookup", True)
               .csv(f"{ROOT}/{src['files']}"))
        s_rows, s_amt, note = raw.count(), amount_sum(raw, amt), "raw files re-read"
    else:
        s_rows, s_amt, note = src.get("rows"), src.get("amount"), src.get("note", "")
    record(entity, "1 rows: source = bronze", "source", s_rows, "bronze", b_rows, note)
    if amt:
        record(entity, "1 amount: source = bronze", "source", s_amt, "bronze", b_amt, note, TOLERANCE)

    # 2. Bronze = Silver + quarantined + merged (nothing silently dropped)
    silver = load(cfg["silver"]) if cfg.get("silver") else None
    if silver is None or "_row_hash" not in silver.columns:
        why = "silver table not built yet" if silver is None else "silver table has no _row_hash"
        record(entity, "2 rows: bronze = silver + quarantined + merged", "bronze", b_rows, "silver", None, why)
        continue
    q, m = entity_rows(quarantine), entity_rows(merge_log)
    q_rows, m_rows = (q.count() if q is not None else 0), (m.count() if m is not None else 0)
    record(entity, "2 rows: bronze = silver + quarantined + merged", "bronze", b_rows,
           "silver+q+m", silver.count() + q_rows + m_rows, f"quarantined {q_rows}, merged {m_rows}")

    b_c = col_ci(bronze, amt)
    bh = with_hash(bronze).select("_row_hash", (F.col(f"`{b_c}`").cast("double") if b_c else F.lit(0.0)).alias("_amt"))
    known = silver.select("_row_hash")
    for log in (q, m):
        if log is not None:
            known = known.unionByName(log.select("_row_hash"))
    lost = bh.join(known.distinct(), "_row_hash", "left_anti")
    record(entity, "2 rows: bronze rows not found anywhere = 0", "lost", lost.count(), "expected", 0,
           "row hash not in silver, quarantine or merge log")

    if amt:
        per_hash = bh.dropDuplicates(["_row_hash"])
        side = lambda log: 0.0 if log is None else float(
            log.select("_row_hash").join(per_hash, "_row_hash").select(F.sum("_amt")).first()[0] or 0.0)
        s_amt = amount_sum(silver, cfg.get("silver_amount"))
        total = None if s_amt is None else s_amt + side(q) + side(m)
        record(entity, "2 amount: bronze = silver + quarantined + merged", "bronze", b_amt, "silver+q+m", total,
               "differs if Silver corrects amounts", TOLERANCE)

        # 3. Silver = Gold
        gold = load(cfg["gold"]) if cfg.get("gold") else None
        record(entity, "3 amount: silver = gold", "silver", s_amt, "gold",
               amount_sum(gold, cfg.get("gold_amount")),
               "gold table not built yet" if gold is None else "", TOLERANCE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema = ("run_id string, entity string, check_name string, left_layer string, left_value double, "
          "right_layer string, right_value double, difference double, status string, note string, checked_at timestamp")
res = spark.createDataFrame(results, schema)
res.write.format(FMT).mode("append").saveAsTable(RESULT_TABLE)

print(RUN_ID, {r["status"]: r["count"] for r in res.groupBy("status").count().collect()})
display(res.select("entity", "check_name", "left_value", "right_value", "difference", "status", "note")
           .orderBy("entity", "check_name"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
