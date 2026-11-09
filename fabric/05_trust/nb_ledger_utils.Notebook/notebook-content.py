# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_ledger_utils: shared Trust Ledger helpers
# Use in any notebook with one cell: `%run nb_ledger_utils`. Everything is written to **`lh_trust`** by OneLake path,
# so it works whatever the notebook's default lakehouse is.
# 
# | Function | Use it to |
# |---|---|
# | `new_run_id(PIPELINE_RUN_ID)` | get one run id per notebook run (the pipeline run id when called from a pipeline) |
# | `read_item("lh_bronze.Lakehouse/Tables/dbo/x")` | read any table in the workspace by path |
# | `with_row_hash(df)` | add `_row_hash` (Bronze formula) to mirrored tables **before any change** |
# | `quarantine(df, entity, reason, RUN_ID)` | park rejected rows (never drop silently) |
# | `dedupe(df, entity, key_cols, RUN_ID)` | keep one row per key, log the others as merged |
# | `log_merges(df, entity, RUN_ID)` | log merges you did yourself (`_row_hash` + `survivor_hash`) |
# | `log_ai(df, entity, purpose, input_col, output_col, RUN_ID)` | log every AI decision |
# | `log_step(RUN_ID, layer, entity, source, target, rows_out, ...)` | one ledger row per table written |
# | `hash_pii(df, cols)` | hash personal data before any AI step |
# | `ai_setup()` | Runtime 2.0 AI Functions settings (`gpt-5-mini`) |
# | `sf_query(sql)` | read-only query in Snowflake as `LEDGER_READER` (Key Vault password) |

# CELL ********************

from pyspark.sql import functions as F, Window
from pyspark.sql.column import Column
import datetime, uuid

WORKSPACE = "ldk-workspace"
ROOT = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com"
TRUST = f"{ROOT}/lh_trust.Lakehouse/Tables/dbo"
FMT = "delta"
LEDGER_COLS = ("_source_file", "_ingested_at", "_loaded_at", "_run_id", "_row_hash")  # never part of the hash

# Source system (Snowflake) for the "source = Bronze" check; password in Key Vault
SF = dict(account="dijxdsb-cr18840", user="LEDGER_READER", role="LEDGER_READER_ROLE", warehouse="LDK_WH",
          database="ZONTIQUE_LEGACY", vault="https://kv-ldk-2446996.vault.azure.net/",
          secret="snowflake-ledger-reader-pwd")


def new_run_id(pipeline_run_id=""):
    """One id per run: the pipeline run id if given, else a timestamp id."""
    return pipeline_run_id or datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]


def read_item(path):
    """Read a table by OneLake path inside the workspace, e.g. 'lh_silver.Lakehouse/Tables/dbo/silver_cost'.
    Returns None if it doesn't exist (yet)."""
    try:
        return spark.read.format(FMT).load(f"{ROOT}/{path}")
    except Exception:
        return None


def trust_read(table):
    try:
        return spark.read.format(FMT).load(f"{TRUST}/{table}")
    except Exception:
        return None


def trust_append(df, table):
    df.write.format(FMT).mode("append").option("mergeSchema", "true").save(f"{TRUST}/{table}")


def with_row_hash(df):
    """Bronze row hash: sha256 over all data columns, '<null>' for nulls. Keeps an existing _row_hash."""
    if "_row_hash" in df.columns:
        return df
    cols = [c for c in df.columns if c.lower() not in LEDGER_COLS]
    parts = [F.coalesce(F.col(f"`{c}`").cast("string"), F.lit("<null>")) for c in cols]
    return df.withColumn("_row_hash", F.sha2(F.concat_ws("||", *parts), 256))


def hash_pii(df, cols):
    """Replace personal data (names, emails) by a stable hash before Silver or any AI step."""
    for c in cols:
        df = df.withColumn(c, F.when(F.col(c).isNull(), None).otherwise(F.sha2(F.lower(F.trim(F.col(c))), 256)))
    return df


def _lit_or_col(v):
    return v if isinstance(v, Column) else F.lit(v)


def quarantine(df, entity, reason_code, run_id, layer="silver", key_col=None, detail=None):
    """Park rejected rows: their Bronze _row_hash + a reason code (text or column). Returns the row count."""
    q = df.select(
        F.lit(run_id).alias("run_id"), F.lit(layer).alias("layer"), F.lit(entity).alias("entity"),
        F.col("_row_hash"), _lit_or_col(reason_code).cast("string").alias("reason_code"),
        (F.col(f"`{key_col}`").cast("string") if key_col else F.lit(None).cast("string")).alias("record_key"),
        _lit_or_col(detail).cast("string").alias("detail"),
        F.current_timestamp().alias("logged_at"))
    trust_append(q, "meta_quarantine")
    return q.count()


def log_merges(df, entity, run_id, survivor_col="survivor_hash", layer="silver", reason="DUPLICATE"):
    """df: one row per merged-away record, with its _row_hash and the _row_hash of the record that was kept."""
    m = df.select(F.lit(run_id).alias("run_id"), F.lit(layer).alias("layer"), F.lit(entity).alias("entity"),
                  F.col("_row_hash"), F.col(survivor_col).alias("survivor_hash"),
                  F.lit(reason).alias("reason_code"), F.current_timestamp().alias("logged_at"))
    trust_append(m, "meta_merge_log")
    return m.count()


def dedupe(df, entity, key_cols, run_id, order_by=None, layer="silver"):
    """Keep one row per key (first by order_by, else by _row_hash); log the others as merged.
    Returns (kept_df, merged_count)."""
    order = order_by if order_by is not None else [F.col("_row_hash")]
    w = Window.partitionBy(*key_cols).orderBy(*order)
    ranked = df.withColumn("_rn", F.row_number().over(w)).withColumn("survivor_hash", F.first("_row_hash").over(w))
    merged = ranked.filter("_rn > 1")
    n = log_merges(merged, entity, run_id, layer=layer) if merged.limit(1).count() else 0
    return ranked.filter("_rn = 1").drop("_rn", "survivor_hash"), n


def log_ai(df, entity, purpose, input_col, output_col, run_id, error_col=None, accepted_col=None, layer="silver"):
    """Log one row per AI decision: input, output, error and whether the result was accepted."""
    a = df.select(
        F.lit(run_id).alias("run_id"), F.lit(layer).alias("layer"), F.lit(entity).alias("entity"),
        F.lit(purpose).alias("purpose"),
        (F.col("_row_hash") if "_row_hash" in df.columns else F.lit(None).cast("string")).alias("subject"),
        F.col(f"`{input_col}`").cast("string").alias("input_text"),
        F.col(f"`{output_col}`").cast("string").alias("output_text"),
        (F.col(f"`{error_col}`").cast("string") if error_col else F.lit(None).cast("string")).alias("error"),
        (F.col(f"`{accepted_col}`").cast("boolean") if accepted_col else F.lit(None).cast("boolean")).alias("accepted"),
        F.current_timestamp().alias("logged_at"))
    trust_append(a, "meta_ai_decisions")
    return a.count()


LEDGER_SCHEMA = ("run_id string, layer string, entity string, source_system string, source_path string, "
                 "target_table string, file_count long, rows_in long, row_count long, rows_quarantined long, "
                 "rows_merged long, amount double, notebook string, logged_at timestamp")


def log_step(run_id, layer, entity, source, target, rows_out, rows_in=None, quarantined=0, merged=0,
             amount=None, file_count=None, source_system=None, notebook=None):
    """One ledger row per table written. Rule: rows_in = rows_out + quarantined + merged."""
    row = [(run_id, layer, entity, source_system or entity, source, target,
            None if file_count is None else int(file_count), None if rows_in is None else int(rows_in),
            int(rows_out), int(quarantined), int(merged), None if amount is None else float(amount),
            notebook, datetime.datetime.utcnow())]
    trust_append(spark.createDataFrame(row, LEDGER_SCHEMA), "meta_load_ledger")
    if rows_in is not None and rows_in != rows_out + quarantined + merged:
        print(f"WARNING {entity}: rows_in {rows_in} != out {rows_out} + quarantined {quarantined} + merged {merged}")


def sf_query(sql):
    """Read-only query in the Snowflake source as LEDGER_READER (needs snowflake-connector-python)."""
    import snowflake.connector
    pwd = notebookutils.credentials.getSecret(SF["vault"], SF["secret"])
    con = snowflake.connector.connect(account=SF["account"], user=SF["user"], password=pwd, role=SF["role"],
                                      warehouse=SF["warehouse"], database=SF["database"])
    try:
        cur = con.cursor()
        cur.execute(sql)
        return cur.fetchall()
    finally:
        con.close()


def ai_setup(model="gpt-5-mini"):
    """AI Functions settings needed on Fabric Runtime 2.0 (else: DeploymentConfigNotFound)."""
    import synapse.ml.spark.aifunc as aifunc
    aifunc.default_conf.set_deployment_name(model)
    aifunc.default_conf.set_reasoning_effort("low")
    aifunc.default_conf.reset_temperature()
    return aifunc


print("Trust Ledger helpers loaded ->", TRUST)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
