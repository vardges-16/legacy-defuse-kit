# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_ledger_utils: shared Trust Ledger, Silver and AI helpers
# Use in any notebook with one cell: `%run nb_ledger_utils`. Everything is read and written by OneLake path,
# so it works whatever the notebook's default lakehouse is.
# # | Function | Use it to |
# |---|---|
# | `new_run_id(PIPELINE_RUN_ID)` | one run id per notebook run (the pipeline run id when called from a pipeline) |
# | `bronze(name)` | read a Bronze table (see `BRONZE`), add `_row_hash`, lower-case the column names |
# | `silver(name)` / `silver_write(df, name)` | read / overwrite a Silver table in `lh_silver` (dbo) |
# | `quarantine(df, entity, reason, RUN_ID)` | park rejected rows (never drop silently) |
# | `dedupe(df, entity, key_cols, RUN_ID)` | keep one row per key, log the others as merged |
# | `ai_resolve(values, entity, purpose, labels, RUN_ID)` | AI on distinct leftover values only, with a gate, decision log and review queue |
# | `log_step(RUN_ID, layer, entity, source, target, rows_out, ...)` | one ledger row per table written |
# | `log_gap(...)` | record a completeness gap (missing day, month or file) |
# | `hash_pii(df, cols)` | hash personal data before Silver or any AI step |
# | `sf_query(sql)` | read-only query in Snowflake as `LEDGER_READER` (Key Vault password) |

# CELL ********************

from pyspark.sql import functions as F, Window
from pyspark.sql.column import Column
import datetime, uuid, re

WORKSPACE = "ldk-workspace"
ROOT = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com"
TRUST = f"{ROOT}/lh_trust.Lakehouse/Tables/dbo"
SILVER = f"{ROOT}/lh_silver.Lakehouse/Tables/dbo"
FMT = "delta"
LEDGER_COLS = ("_source_file", "_ingested_at", "_loaded_at", "_run_id", "_row_hash", "_schema_version")  # never hashed
AS_OF = "2026-10-31"          # data cut-off of the legacy extract

# Bronze objects -> OneLake path. Change only here if the shortcuts are named differently.
BRONZE = {
    "FOCUS_COST":           "lh_bronze.Lakehouse/Tables/COST/FOCUS_COST",
    "ASSETS":               "lh_bronze.Lakehouse/Tables/CMDB/ASSETS",
    "ASSET_GROUPS":         "lh_bronze.Lakehouse/Tables/CMDB/ASSET_GROUPS",
    "BUDGET":               "lh_bronze.Lakehouse/Tables/FIN/BUDGET",
    "license_products":     "lh_bronze.Lakehouse/Tables/dbo/license_products",
    "license_entitlements": "lh_bronze.Lakehouse/Tables/dbo/license_entitlements",
    "installations":        "lh_bronze.Lakehouse/Tables/dbo/installations",
    "backup_jobs":          "lh_bronze.Lakehouse/Tables/bkp/backup_jobs",
    "backup_storage":       "lh_bronze.Lakehouse/Tables/bkp/backup_storage",
}
BKP_LANDING = "lh_bronze.Lakehouse/Files/landing/vm_backups"

# Source system (Snowflake) for the "source = Bronze" check; password in Key Vault
SF = dict(account="dijxdsb-cr18840", user="LEDGER_READER", role="LEDGER_READER_ROLE", warehouse="LDK_WH",
          database="ZONTIQUE_LEGACY", vault="https://kv-ldk-2446996.vault.azure.net/",
          secret="snowflake-ledger-reader-pwd")


def new_run_id(pipeline_run_id=""):
    """One id per run: the pipeline run id if given, else a timestamp id."""
    return pipeline_run_id or datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]


def read_item(path):
    """Read a table by OneLake path inside the workspace; None if it doesn't exist (yet)."""
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
    n = q.count()
    if n:
        trust_append(q, "meta_quarantine")
    return n


def log_merges(df, entity, run_id, survivor_col="survivor_hash", layer="silver", reason="DUPLICATE"):
    """df: one row per merged-away record, with its _row_hash and the _row_hash of the record that was kept."""
    m = df.select(F.lit(run_id).alias("run_id"), F.lit(layer).alias("layer"), F.lit(entity).alias("entity"),
                  F.col("_row_hash"), F.col(survivor_col).alias("survivor_hash"),
                  _lit_or_col(reason).alias("reason_code"), F.current_timestamp().alias("logged_at"))
    n = m.count()
    if n:
        trust_append(m, "meta_merge_log")
    return n


def dedupe(df, entity, key_cols, run_id, order_by=None, layer="silver", reason="DUPLICATE"):
    """Keep one row per key (first by order_by, else by _row_hash); log the others as merged.
    Returns (kept_df, merged_count)."""
    order = order_by if order_by is not None else [F.col("_row_hash")]
    w = Window.partitionBy(*key_cols).orderBy(*order)
    ranked = df.withColumn("_rn", F.row_number().over(w)).withColumn("survivor_hash", F.first("_row_hash").over(w))
    ranked = ranked.cache()
    n = log_merges(ranked.filter("_rn > 1"), entity, run_id, layer=layer, reason=reason)
    return ranked.filter("_rn = 1").drop("_rn", "survivor_hash"), n


def log_ai(df, entity, purpose, input_col, output_col, run_id, error_col=None, accepted_col=None, layer="silver"):
    """Log one row per AI decision (row-level variant; ai_resolve logs per distinct value)."""
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
    ok = rows_in is None or rows_in == rows_out + quarantined + merged
    print(f"{'OK  ' if ok else 'FAIL'} {layer}/{entity}: in {rows_in} = out {rows_out} + quarantined {quarantined} + merged {merged}")
    return ok


def log_gap(run_id, entity, gap_type, expected_key, detail="", layer="bronze"):
    """Completeness gap: a day, month or file that should exist but doesn't."""
    row = [(run_id, layer, entity, gap_type, str(expected_key), detail, datetime.datetime.utcnow())]
    trust_append(spark.createDataFrame(row, "run_id string, layer string, entity string, gap_type string, "
                                            "expected_key string, detail string, logged_at timestamp"), "meta_completeness")


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


print("Trust Ledger helpers loaded ->", TRUST)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Silver helpers
# Bronze is kept exactly as received (all text). These helpers read it, parse the legacy formats safely
# (`try_*` functions: a bad value becomes NULL and is flagged, never a crash), and write Silver tables.

# CELL ********************

def bronze(name):
    """Read a Bronze table, add the Bronze _row_hash first, then lower-case the column names."""
    df = read_item(BRONZE[name])
    if df is None:
        raise ValueError(f"Bronze table '{name}' not found at {BRONZE[name]}: check the shortcut / BRONZE map")
    df = with_row_hash(df)
    return df.toDF(*[c.lower() for c in df.columns])


def silver(name):
    return spark.read.format(FMT).load(f"{SILVER}/{name}")


def silver_write(df, name):
    """Overwrite a Silver table and return its row count (read back from storage)."""
    df.write.format(FMT).mode("overwrite").option("overwriteSchema", "true").save(f"{SILVER}/{name}")
    return silver(name).count()


def num(col_name):
    """Text -> double without failing on bad values."""
    return F.expr(f"try_cast(trim(`{col_name}`) as double)")


def parse_date(df, src, dst, ambiguous="dd/MM/yyyy"):
    """Legacy date text -> date. Handles yyyy-MM-dd[...], dd/MM/yyyy, MM/dd/yyyy[ HH:mm], 'N/A'.
    Adds <dst>_ambiguous when day and month are both <= 12 in a slash date."""
    df = df.withColumn("_d", F.trim(F.col(src)))
    iso = F.col("_d").rlike(r"^\d{4}-\d{2}-\d{2}")
    sl = F.col("_d").rlike(r"^\d{1,2}/\d{1,2}/\d{4}")
    a = F.expr("try_cast(split(_d, '/')[0] as int)")
    b = F.expr("try_cast(split(_d, '/')[1] as int)")
    out = (F.when(iso, F.expr("try_to_timestamp(substr(_d, 1, 10), 'yyyy-MM-dd')"))
            .when(sl & (a > 12), F.expr("try_to_timestamp(substr(_d, 1, 10), 'dd/MM/yyyy')"))
            .when(sl & (b > 12), F.expr("try_to_timestamp(substr(_d, 1, 10), 'MM/dd/yyyy')"))
            .when(sl, F.expr(f"try_to_timestamp(substr(_d, 1, 10), '{ambiguous}')")))
    return (df.withColumn(dst, out.cast("date"))
              .withColumn(f"{dst}_ambiguous", F.coalesce(sl & (a <= 12) & (b <= 12), F.lit(False)))
              .drop("_d"))


def parse_ts(df, src, dst):
    """Legacy timestamp text -> timestamp (ISO with Z or +00:00, dd.MM.yyyy HH:mm, MM/dd/yyyy HH:mm)."""
    df = df.withColumn("_t", F.trim(F.col(src)))
    out = F.coalesce(
        F.expr("try_to_timestamp(substr(_t, 1, 19), \"yyyy-MM-dd'T'HH:mm:ss\")"),
        F.expr("try_to_timestamp(_t, 'dd.MM.yyyy HH:mm')"),
        F.expr("try_to_timestamp(_t, 'MM/dd/yyyy HH:mm')"),
        F.expr("try_to_timestamp(substr(_t, 1, 10), 'yyyy-MM-dd')"))
    return df.withColumn(dst, out).drop("_t")


def norm_key(c):
    """Comparison key: upper case, letters and digits only."""
    return F.upper(F.regexp_replace(F.coalesce(c, F.lit("")), "[^A-Za-z0-9]", ""))


def pyn(s):
    return re.sub(r"[^A-Za-z0-9]", "", s or "").upper()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## AI with a gate: `ai_resolve`
# Rules always run first; AI only sees the **distinct values the rules could not resolve**.
# 1. Known mappings (approved by a reviewer or accepted in an earlier run) are reused: no AI call.
# 2. `ai.classify` picks one of the allowed labels (or `UNMATCHED`): the AI can't invent a value.
# 3. `ai.similarity` scores the input against the chosen label; **accepted only if score >= threshold**.
# 4. Every decision goes to `meta_ai_decisions`; rejected ones to `meta_ai_review_queue` for a human.
# 5. Accepted mappings go to `meta_ai_mappings`, so next run they are rules (AI teaches the rules).

# CELL ********************

AI_MODEL = "gpt-5-mini"
AI_THRESHOLD = 0.6


def ai_setup(model=AI_MODEL):
    """AI Functions settings needed on Fabric Runtime 2.0 (else: DeploymentConfigNotFound)."""
    import synapse.ml.spark.aifunc as aifunc
    aifunc.default_conf.set_deployment_name(model)
    aifunc.default_conf.set_reasoning_effort("low")
    aifunc.default_conf.reset_temperature()
    return aifunc


def _ai_classify(df, input_col, labels, output_col):
    return df.ai.classify(labels=labels, input_col=input_col, output_col=output_col, error_col=f"{output_col}_error")


def _ai_similarity(df, input_col, other_col, output_col):
    return df.ai.similarity(input_col=input_col, other_col=other_col, output_col=output_col, error_col=f"{output_col}_error")


AI_DECISION_SCHEMA = ("run_id string, layer string, entity string, purpose string, input_text string, "
                      "output_text string, label_text string, score double, gate_outcome string, accepted boolean, "
                      "error string, model string, prompt_version string, logged_at timestamp")


def ai_resolve(values, entity, purpose, labels, run_id, label_text=None, threshold=AI_THRESHOLD,
               instructions=None, layer="silver"):
    """values: dirty inputs (any iterable). labels: the only allowed outputs. label_text: label -> description
    used for the similarity gate (default: the label itself). Returns {input: label} for accepted decisions."""
    values = sorted({v for v in values if v is not None and str(v).strip()})
    if not values:
        return {}
    known = {}
    m = trust_read("meta_ai_mappings")
    if m is not None:
        for r in m.filter((F.col("purpose") == purpose) & (F.col("status") == "ACTIVE")).collect():
            if r["label"] in labels:
                known[r["input_text"]] = r["label"]
    lt = label_text or {}
    now = datetime.datetime.utcnow()
    prompt_version = f"classify:{len(labels)}labels:t{threshold}"
    rows, result = [], {}
    for v in values:
        if v in known:
            result[v] = known[v]
            rows.append((run_id, layer, entity, purpose, v, known[v], lt.get(known[v], known[v]), None,
                         "KNOWN_MAPPING", True, None, None, prompt_version, now))
    todo = [v for v in values if v not in known]
    if todo:
        try:
            ai_setup()
        except Exception as e:  # outside Fabric (tests) the AI layer is mocked
            print("ai_setup skipped:", e)
        df = spark.createDataFrame([(v,) for v in todo], "input string")
        if instructions:
            df = df.withColumn("input_ctx", F.concat(F.lit(instructions + " Value: "), F.col("input")))
            df = _ai_classify(df, "input_ctx", labels + ["UNMATCHED"], "ai_label")
        else:
            df = _ai_classify(df, "input", labels + ["UNMATCHED"], "ai_label")
        lab_map = F.create_map(*[x for k in labels for x in (F.lit(k), F.lit(lt.get(k, k)))])
        df = df.withColumn("label_text", F.coalesce(lab_map[F.col("ai_label")], F.col("ai_label")))
        df = _ai_similarity(df, "input", "label_text", "ai_score")
        for r in df.collect():
            lab = r["ai_label"]
            score = None if r["ai_score"] is None else float(r["ai_score"])
            valid = lab in labels
            ok = valid and score is not None and score >= threshold
            outcome = "AUTO_ACCEPT" if ok else ("REVIEW" if valid else "UNMATCHED")
            err = r["ai_label_error"] if "ai_label_error" in r.asDict() else None
            rows.append((run_id, layer, entity, purpose, r["input"], lab, r["label_text"], score, outcome, ok,
                         None if err is None else str(err), AI_MODEL, prompt_version, now))
            if ok:
                result[r["input"]] = lab
    dec = spark.createDataFrame(rows, AI_DECISION_SCHEMA)
    trust_append(dec, "meta_ai_decisions")
    rev = dec.filter("gate_outcome in ('REVIEW', 'UNMATCHED')")
    if rev.count():
        trust_append(rev.select("run_id", "entity", "purpose", "input_text", F.col("output_text").alias("proposed_label"),
                                "score", F.lit("OPEN").alias("status"), "logged_at"), "meta_ai_review_queue")
    new = dec.filter("gate_outcome = 'AUTO_ACCEPT'")
    if new.count():
        trust_append(new.select("purpose", "input_text", F.col("output_text").alias("label"),
                                F.lit("AI_AUTO").alias("source"), F.lit("ACTIVE").alias("status"),
                                "run_id", "logged_at"), "meta_ai_mappings")
    stats = {o: sum(1 for x in rows if x[8] == o) for o in ("KNOWN_MAPPING", "AUTO_ACCEPT", "REVIEW", "UNMATCHED")}
    print(f"AI {entity}/{purpose}: {len(values)} distinct values -> {stats}")
    return result


def map_values(col, mapping):
    """Column lookup from a Python dict (used to join ai_resolve results back to all rows)."""
    if not mapping:
        return F.lit(None).cast("string")
    return F.create_map(*[x for k, v in mapping.items() for x in (F.lit(k), F.lit(v))])[col]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
