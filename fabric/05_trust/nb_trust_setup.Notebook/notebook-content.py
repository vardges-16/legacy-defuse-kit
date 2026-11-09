# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_trust_setup: create the Trust Ledger tables in `lh_trust` (run once)
# 1. Creates the ledger tables in `lh_trust` with their full schema (only if missing).
# 2. Copies the existing `meta_*` tables from `lh_bronze` once (only into empty targets). The old tables in `lh_bronze`
#    stay untouched: delete them yourself after checking the counts below.
# Safe to re-run.

# CELL ********************

%run nb_ledger_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SCHEMAS = {
    "meta_load_ledger": LEDGER_SCHEMA,
    "meta_quarantine": "run_id string, layer string, entity string, _row_hash string, reason_code string, "
                       "record_key string, detail string, logged_at timestamp",
    "meta_merge_log": "run_id string, layer string, entity string, _row_hash string, survivor_hash string, "
                      "reason_code string, logged_at timestamp",
    "meta_ai_decisions": "run_id string, layer string, entity string, purpose string, subject string, "
                         "input_text string, output_text string, error string, accepted boolean, logged_at timestamp",
    "meta_reconciliation": "run_id string, entity string, check_name string, left_layer string, left_value double, "
                           "right_layer string, right_value double, difference double, status string, note string, "
                           "checked_at timestamp",
    "meta_trust_summary": "run_id string, checked_at timestamp, entity string, status string, source_rows long, "
                          "bronze_rows long, silver_rows long, quarantined_rows long, merged_rows long, lost_rows long, "
                          "bronze_amount double, silver_amount double, gold_amount double, checks_pass long, "
                          "checks_fail long, checks_pending long, silver_run_id string",
}
OLD = "lh_bronze.Lakehouse/Tables/dbo"
COPY = ["meta_load_ledger", "meta_profile_tables", "meta_profile_columns", "meta_profile_findings",
        "meta_ai_decisions", "meta_reconciliation"]

for t, schema in SCHEMAS.items():
    if trust_read(t) is None:
        trust_append(spark.createDataFrame([], schema), t)
        print("created", t)

for t in COPY:
    old = read_item(f"{OLD}/{t}")
    if old is None:
        continue
    new = trust_read(t)
    if new is not None and new.limit(1).count():
        print(f"skip {t}: lh_trust already has rows")
        continue
    if "layer" not in old.columns:
        old = old.withColumn("layer", F.lit("bronze"))
    if t == "meta_load_ledger" and "entity" not in old.columns:
        old = old.withColumn("entity", F.col("target_table"))
    trust_append(old, t)
    print(f"copied {t}: {old.count()} rows")

for t in list(SCHEMAS) + COPY[1:4]:
    df = trust_read(t)
    print(f"{t:24s} {'missing' if df is None else df.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
