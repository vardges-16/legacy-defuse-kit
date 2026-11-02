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

# # nb_profile_legacy: data profiling of the legacy sources (Bronze)
# 
# Legacy Defuse Kit, team de_dust2. Profiles **every table and every data file** in the default lakehouse (`lh_legacy`) and writes the results to Delta tables, so the Trust Ledger, the Power BI report and the data agent can use them.
# 
# **Before running**
# 1. Attach `lh_legacy` as the **default lakehouse** (left pane, Lakehouses).
# 2. Make every source visible in `lh_legacy`:
#    - Snowflake: shortcut in **Tables** to the mirrored database `ZONTIQUE_BILLING` tables.
#    - Storage containers (`cmdb-inputs`, `fin-inputs`, later `vm-backups`): shortcut in **Files** to each container.
#    - Azure SQL database: mirror it, then shortcut its tables into **Tables**.
# 3. Optional AI layer: set `ENABLE_AI = True` (needs a paid F2+ capacity, the Copilot / Azure OpenAI tenant setting, Runtime 1.3+).
# 
# **Outputs** (Delta tables in `lh_legacy`, appended per run): `meta_profile_tables`, `meta_profile_columns`, `meta_profile_findings`, and `meta_ai_decisions` when AI is on.
# 
# **Personal data:** columns that look like names or emails are masked in the stored results and never sent to AI (blueprint: PII hashed before AI processing).


# CELL ********************

# Parameters
RUN_LABEL = "baseline"                 # free text to tell runs apart
EXCLUDE_TABLE_PREFIXES = ("meta_",)    # don't profile our own output tables
FILES_ROOT = "Files"                   # folder in the default lakehouse to scan for data files
FILE_FORMATS = {".csv": "csv", ".json": "json", ".parquet": "parquet"}
CSV_SEPARATOR = ","
LARGE_TABLE_ROWS = 1_000_000           # above this, top values and patterns use a sample
SAMPLE_FRACTION = 0.2
TOP_N = 5
NULL_RATE_WARN = 0.05                  # 5% nulls or more -> finding
PII_NAME_REGEX = r"(?i)(e_?mail|owner_name|display_name|full_name|first_name|last_name|person)"
MAX_DRIFT_FILES = 400                  # max files per folder checked for header drift
WRITE_FORMAT = "delta"
ENABLE_AI = False                      # True = AI semantic types + AI explanations of findings
OUT = {"tables": "meta_profile_tables", "columns": "meta_profile_columns",
       "findings": "meta_profile_findings", "ai": "meta_ai_decisions"}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import re, uuid, datetime, json
from collections import defaultdict
from pyspark.sql import functions as F, types as T

RUN_ID = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
RUN_TS = datetime.datetime.utcnow()
print("Run:", RUN_ID, RUN_LABEL)

try:
    _fs = notebookutils.fs          # Fabric
except NameError:
    try:
        _fs = mssparkutils.fs       # older Fabric runtimes
    except NameError:
        _fs = None                  # local test: os.walk fallback below

NUM_RE = r"^-?[0-9]+([.][0-9]+)?$"
COMMA_DEC_RE = r"^-?[0-9]+,[0-9]+$"
DATE_RES = {
    "yyyy-MM-dd[THH:mm:ss]": r"^[0-9]{4}-[0-9]{2}-[0-9]{2}([T ][0-9]{2}:[0-9]{2}(:[0-9]{2})?([.][0-9]+)?Z?)?$",
    "dd/MM/yyyy": r"^[0-9]{2}/[0-9]{2}/[0-9]{4}",
    "dd.MM.yyyy": r"^[0-9]{2}[.][0-9]{2}[.][0-9]{4}",
    "yyyy/MM/dd": r"^[0-9]{4}/[0-9]{2}/[0-9]{2}",
}
NULL_TOKENS = ["n/a", "na", "null", "none", "-", "?"]
KEY_NAME_RE = re.compile(r"(?i)(^id$|_id$|^line_id$|^job_id$)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Discover sources: tables + data files grouped by folder and extension
def list_tables():
    rows = spark.sql("SHOW TABLES").collect()
    out = []
    for r in rows:
        name = r["tableName"]
        if getattr(r, "isTemporary", False) or name.startswith(EXCLUDE_TABLE_PREFIXES):
            continue
        out.append(name)
    return sorted(out)

def list_files(root):
    files = []
    if _fs is not None:
        stack = [root]
        while stack:
            p = stack.pop()
            try:
                entries = _fs.ls(p)
            except Exception as e:
                print("  cannot list", p, "->", e); continue
            for e in entries:
                if e.isDir:
                    stack.append(e.path)
                else:
                    files.append(e.path)
    else:
        import os
        for d, _, fs in os.walk(root):
            files += [os.path.join(d, f) for f in fs]
    return files

def group_files(files):
    groups = defaultdict(list)
    for f in files:
        ext = "." + f.rsplit(".", 1)[-1].lower() if "." in f.rsplit("/", 1)[-1] else ""
        if ext in FILE_FORMATS:
            folder = f.rsplit("/", 1)[0]
            groups[(folder, ext)].append(f)
    return groups

tables = list_tables()
file_groups = group_files(list_files(FILES_ROOT))
print(f"Tables: {len(tables)} -> {tables}")
for (folder, ext), fl in file_groups.items():
    print(f"Files: {folder}/*{ext} ({len(fl)} files)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. Readers
def read_files(paths, ext):
    fmt = FILE_FORMATS[ext]
    if fmt == "csv":
        df = (spark.read.option("header", True).option("inferSchema", False)
              .option("sep", CSV_SEPARATOR).csv(paths))
    elif fmt == "json":
        df = spark.read.option("multiLine", True).json(paths)
        # REST-style envelope {"data": [...], ...} -> one row per element
        if "data" in df.columns and isinstance(df.schema["data"].dataType, T.ArrayType):
            df = df.select(F.explode("data").alias("_r")).select("_r.*")
    else:
        df = spark.read.parquet(*paths)
    return df

def csv_header_drift(paths):
    """Distinct header lines across CSV files (first line of each file)."""
    sample = sorted(paths)[:MAX_DRIFT_FILES]
    rdd = spark.sparkContext.wholeTextFiles(",".join(sample))
    heads = rdd.map(lambda kv: (kv[1].split("\n", 1)[0].strip().lstrip("﻿"), kv[0])).collect()
    by_header = defaultdict(list)
    for h, f in heads:
        by_header[h].append(f)
    return by_header

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3. Column and table profiling
def _is_pii(col_name, email_like, nonnull):
    return bool(re.search(PII_NAME_REGEX, col_name)) or (nonnull > 0 and email_like / nonnull > 0.5)

def _pattern(c):
    """Format signature: runs of upper case -> A, lower case -> a, digits -> 9 (rg-bill-prod-01 -> a-a-a-9)."""
    s = F.substring(c, 1, 60)
    s = F.regexp_replace(s, "[A-Z]+", "A")
    s = F.regexp_replace(s, "[a-z]+", "a")
    return F.regexp_replace(s, "[0-9]+", "9")

def profile_dataset(df, source_name, source_kind, file_count=None):
    cols = [c for c in df.columns if not c.startswith("_")]
    df = df.select([F.col(f"`{c}`") for c in cols])
    dtypes = dict(df.dtypes)
    total = df.count()
    numeric_types = ("int", "bigint", "double", "float", "smallint", "tinyint")

    aggs = []
    for i, c in enumerate(cols):
        raw = F.col(f"`{c}`")
        s = raw.cast("string")
        t = F.trim(s)
        lt = F.lower(t)
        p = f"c{i}__"
        aggs += [
            F.count(raw).alias(p + "nonnull"),
            F.approx_count_distinct(s, 0.01).alias(p + "distinct"),
            F.approx_count_distinct(lt, 0.01).alias(p + "distinct_norm"),
            F.sum(F.when(t == "", 1).otherwise(0)).alias(p + "blank"),
            F.sum(F.when(s != t, 1).otherwise(0)).alias(p + "padded"),
            F.sum(F.when(lt.isin(NULL_TOKENS), 1).otherwise(0)).alias(p + "null_tokens"),
            F.sum(F.when(t.rlike(NUM_RE), 1).otherwise(0)).alias(p + "numeric_like"),
            F.sum(F.when(t.rlike(COMMA_DEC_RE), 1).otherwise(0)).alias(p + "comma_decimal"),
            F.sum(F.when(t.contains("@"), 1).otherwise(0)).alias(p + "email_like"),
            F.sum(F.when(t.rlike(NUM_RE) & (t.cast("double") < 0), 1).otherwise(0)).alias(p + "negative"),
            F.avg(F.length(s)).alias(p + "avg_len"),
            F.max(F.length(s)).alias(p + "max_len"),
        ]
        for k, rx in DATE_RES.items():
            aggs.append(F.sum(F.when(t.rlike(rx), 1).otherwise(0)).alias(p + "date_" + k))
        if dtypes[c].startswith(numeric_types) or dtypes[c].startswith("decimal") or dtypes[c] in ("date", "timestamp", "timestamp_ntz"):
            aggs += [F.min(raw).cast("string").alias(p + "min"), F.max(raw).cast("string").alias(p + "max")]
        else:
            aggs += [F.min(t).alias(p + "min"), F.max(t).alias(p + "max")]
    a = df.agg(*aggs).collect()[0].asDict() if cols else {}

    big = total > LARGE_TABLE_ROWS
    sdf = df.sample(False, SAMPLE_FRACTION, seed=42) if big else df

    col_rows = []
    for i, c in enumerate(cols):
        p = f"c{i}__"
        nonnull = a[p + "nonnull"] or 0
        pii = _is_pii(c, a[p + "email_like"] or 0, nonnull)
        raw = F.col(f"`{c}`")
        pats = (sdf.where(raw.isNotNull()).groupBy(_pattern(raw.cast("string")).alias("p"))
                .count().orderBy(F.desc("count")).limit(3).collect())
        pat_total = sum(r["count"] for r in pats) or 1
        top_pattern_share = (pats[0]["count"] / pat_total) if pats else None
        if pii:
            top_vals = "masked (personal data)"
            vmin = vmax = "masked"
        else:
            tv = (sdf.where(raw.isNotNull()).groupBy(raw.cast("string").alias("v"))
                  .count().orderBy(F.desc("count")).limit(TOP_N).collect())
            top_vals = json.dumps([[r["v"], r["count"]] for r in tv], ensure_ascii=False)
            vmin, vmax = a[p + "min"], a[p + "max"]
        date_formats = [k for k in DATE_RES if (a[p + "date_" + k] or 0) > 0]
        col_rows.append({
            "run_id": RUN_ID, "run_label": RUN_LABEL, "profiled_at": RUN_TS,
            "source_name": source_name, "source_kind": source_kind, "column_name": c,
            "data_type": dtypes[c], "row_count": total, "null_count": total - nonnull,
            "null_pct": round((total - nonnull) / total, 4) if total else None,
            "distinct_approx": a[p + "distinct"], "distinct_normalized_approx": a[p + "distinct_norm"],
            "blank_count": a[p + "blank"], "padded_count": a[p + "padded"],
            "null_token_count": a[p + "null_tokens"], "numeric_like_count": a[p + "numeric_like"],
            "comma_decimal_count": a[p + "comma_decimal"], "negative_count": a[p + "negative"],
            "avg_len": float(a[p + "avg_len"]) if a[p + "avg_len"] is not None else None,
            "max_len": a[p + "max_len"], "min_value": vmin, "max_value": vmax,
            "date_formats": ",".join(date_formats) if date_formats else None,
            "top_values": top_vals,
            "top_patterns": json.dumps([[r["p"], r["count"]] for r in pats], ensure_ascii=False),
            "top_pattern_share": round(top_pattern_share, 4) if top_pattern_share is not None else None,
            "is_pii": pii, "sampled": big, "semantic_type": None,
        })

    dup_rows = total - df.distinct().count() if cols else 0
    key_col = next((c for c in cols if KEY_NAME_RE.search(c)), None)
    dup_key = None
    if key_col:
        k = df.where(F.col(f"`{key_col}`").isNotNull())
        dup_key = k.count() - k.select(f"`{key_col}`").distinct().count()
    table_row = {
        "run_id": RUN_ID, "run_label": RUN_LABEL, "profiled_at": RUN_TS,
        "source_name": source_name, "source_kind": source_kind, "row_count": total,
        "column_count": len(cols), "file_count": file_count, "duplicate_rows": dup_rows,
        "key_column": key_col, "duplicate_keys": dup_key,
    }
    return table_row, col_rows

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 4. Run the profiling
table_rows, column_rows, drift = [], [], {}

for t in tables:
    print("Profiling table", t)
    try:
        tr, cr = profile_dataset(spark.table(t), t, "table")
        table_rows.append(tr); column_rows += cr
    except Exception as e:
        print("  FAILED:", e)

for (folder, ext), paths in sorted(file_groups.items()):
    name = f"{folder}/*{ext}"
    print("Profiling files", name, f"({len(paths)})")
    try:
        if FILE_FORMATS[ext] == "csv":
            drift[name] = csv_header_drift(paths)
        tr, cr = profile_dataset(read_files(paths, ext), name, "files", file_count=len(paths))
        table_rows.append(tr); column_rows += cr
    except Exception as e:
        print("  FAILED:", e)

print(f"Profiled {len(table_rows)} sources, {len(column_rows)} columns")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5. Rule-based findings (deterministic, no AI)
findings = []
def add(source, column, check, severity, metric, detail):
    findings.append({"run_id": RUN_ID, "run_label": RUN_LABEL, "profiled_at": RUN_TS,
                     "source_name": source, "column_name": column, "check_code": check,
                     "severity": severity, "metric_value": float(metric) if metric is not None else None,
                     "detail": detail, "ai_explanation": None})

for r in column_rows:
    s, c, n = r["source_name"], r["column_name"], r["row_count"] or 0
    if n and r["null_pct"] is not None and r["null_pct"] >= NULL_RATE_WARN:
        add(s, c, "NULL_RATE", "warning", r["null_pct"], f"{r['null_count']} of {n} values are null")
    if r["null_token_count"]:
        add(s, c, "NULL_TOKENS", "warning", r["null_token_count"], "Text placeholders such as N/A or null instead of real nulls")
    if r["blank_count"]:
        add(s, c, "BLANK_VALUES", "warning", r["blank_count"], "Empty or whitespace-only values")
    if r["padded_count"]:
        add(s, c, "PADDED_WHITESPACE", "info", r["padded_count"], "Leading or trailing spaces")
    d, dn = r["distinct_approx"] or 0, r["distinct_normalized_approx"] or 0
    if 0 < d <= 10000 and d > dn:
        add(s, c, "CASE_OR_SPACE_VARIANTS", "warning", d - dn, f"{d} distinct values but {dn} after lower-casing and trimming: same value written differently")
    if r["date_formats"] and "," in r["date_formats"]:
        add(s, c, "MIXED_DATE_FORMATS", "warning", r["date_formats"].count(",") + 1, f"Date formats found: {r['date_formats']}")
    if r["comma_decimal_count"]:
        add(s, c, "COMMA_DECIMAL", "warning", r["comma_decimal_count"], "Numbers written with a comma as decimal separator")
    if r["negative_count"]:
        add(s, c, "NEGATIVE_VALUES", "info", r["negative_count"], "Negative numbers (check: credits/refunds or errors?)")
    nonnull = n - (r["null_count"] or 0)
    mostly_numeric = nonnull > 0 and (r["numeric_like_count"] or 0) / nonnull > 0.9
    if (r["top_pattern_share"] is not None and r["top_pattern_share"] < 0.8 and d <= 500
            and (r["avg_len"] or 0) <= 40 and not r["is_pii"] and not mostly_numeric):
        add(s, c, "MIXED_PATTERNS", "info", r["top_pattern_share"], f"Most common format covers only {r['top_pattern_share']:.0%} of values; top formats {r['top_patterns']}")

for t in table_rows:
    if t["duplicate_rows"]:
        add(t["source_name"], None, "DUPLICATE_ROWS", "warning", t["duplicate_rows"], "Fully identical rows")
    if t["duplicate_keys"]:
        add(t["source_name"], t["key_column"], "DUPLICATE_KEYS", "warning", t["duplicate_keys"], f"Repeated values in key column {t['key_column']}")

for name, by_header in drift.items():
    if len(by_header) > 1:
        detail = "; ".join(f"{len(fs)} file(s), first {sorted(fs)[0].rsplit('/',1)[-1]}: {h[:120]}" for h, fs in by_header.items())
        add(name, None, "SCHEMA_DRIFT", "warning", len(by_header), f"{len(by_header)} different headers: {detail}")

print(f"{len(findings)} findings")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 6. Optional AI layer: semantic type per column + plain-language explanation per finding
LABELS = ["identifier", "person name", "email address", "date or timestamp", "money amount",
          "quantity or measure", "code or category", "free text", "boolean flag", "resource path or URL"]
ai_log = []

if ENABLE_AI:
    # Never send personal data: PII columns are labelled locally and skipped
    desc_rows = [(r["source_name"], r["column_name"],
                  f"column name: {r['column_name']}; data type: {r['data_type']}; "
                  f"common formats: {r['top_patterns']}; sample values: {r['top_values']}")
                 for r in column_rows if not r["is_pii"]]
    if desc_rows:
        ddf = spark.createDataFrame(desc_rows, ["source_name", "column_name", "description"])
        cls = ddf.ai.classify(labels=LABELS, input_col="description", output_col="semantic_type",
                              error_col="ai_error").collect()
        sem = {(r["source_name"], r["column_name"]): r["semantic_type"] for r in cls}
        for r in cls:
            ai_log.append({"run_id": RUN_ID, "logged_at": RUN_TS, "purpose": "classify column semantic type",
                           "subject": f"{r['source_name']}.{r['column_name']}", "input_text": r["description"],
                           "output_text": r["semantic_type"], "error": r["ai_error"]})
        for r in column_rows:
            r["semantic_type"] = "person name or email (masked)" if r["is_pii"] else sem.get((r["source_name"], r["column_name"]))

    if findings:
        fdf = spark.createDataFrame([(i, f["source_name"], f["column_name"] or "(table)", f["check_code"],
                                      str(f["metric_value"]), f["detail"]) for i, f in enumerate(findings)],
                                    ["idx", "source_name", "column_name", "check_code", "metric_value", "detail"])
        exp = fdf.ai.generate_response(
            prompt=("You are a data quality analyst for a legacy-to-Fabric migration. Source {source_name}, "
                    "column {column_name}, check {check_code}, metric {metric_value}, detail: {detail}. "
                    "In one sentence, say what the likely data problem is and which cleansing action fits best: "
                    "Fix, Merge, Flag or Quarantine."),
            is_prompt_template=True, output_col="ai_explanation", error_col="ai_error",
            response_format="text").collect()
        for r in exp:
            findings[r["idx"]]["ai_explanation"] = r["ai_explanation"]
            ai_log.append({"run_id": RUN_ID, "logged_at": RUN_TS, "purpose": "explain profiling finding",
                           "subject": f"{r['source_name']}.{r['column_name']}:{r['check_code']}",
                           "input_text": r["detail"], "output_text": r["ai_explanation"], "error": r["ai_error"]})
    print(f"AI layer done: {len(ai_log)} AI calls logged")
else:
    print("AI layer skipped (ENABLE_AI = False)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 7. Save results (append, one run_id per run)
TABLE_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType()), T.StructField("run_label", T.StringType()),
    T.StructField("profiled_at", T.TimestampType()), T.StructField("source_name", T.StringType()),
    T.StructField("source_kind", T.StringType()), T.StructField("row_count", T.LongType()),
    T.StructField("column_count", T.IntegerType()), T.StructField("file_count", T.IntegerType()),
    T.StructField("duplicate_rows", T.LongType()), T.StructField("key_column", T.StringType()),
    T.StructField("duplicate_keys", T.LongType())])
COLUMN_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType()), T.StructField("run_label", T.StringType()),
    T.StructField("profiled_at", T.TimestampType()), T.StructField("source_name", T.StringType()),
    T.StructField("source_kind", T.StringType()), T.StructField("column_name", T.StringType()),
    T.StructField("data_type", T.StringType()), T.StructField("row_count", T.LongType()),
    T.StructField("null_count", T.LongType()), T.StructField("null_pct", T.DoubleType()),
    T.StructField("distinct_approx", T.LongType()), T.StructField("distinct_normalized_approx", T.LongType()),
    T.StructField("blank_count", T.LongType()), T.StructField("padded_count", T.LongType()),
    T.StructField("null_token_count", T.LongType()), T.StructField("numeric_like_count", T.LongType()),
    T.StructField("comma_decimal_count", T.LongType()), T.StructField("negative_count", T.LongType()),
    T.StructField("avg_len", T.DoubleType()), T.StructField("max_len", T.IntegerType()),
    T.StructField("min_value", T.StringType()), T.StructField("max_value", T.StringType()),
    T.StructField("date_formats", T.StringType()), T.StructField("top_values", T.StringType()),
    T.StructField("top_patterns", T.StringType()), T.StructField("top_pattern_share", T.DoubleType()),
    T.StructField("is_pii", T.BooleanType()), T.StructField("sampled", T.BooleanType()),
    T.StructField("semantic_type", T.StringType())])
FINDING_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType()), T.StructField("run_label", T.StringType()),
    T.StructField("profiled_at", T.TimestampType()), T.StructField("source_name", T.StringType()),
    T.StructField("column_name", T.StringType()), T.StructField("check_code", T.StringType()),
    T.StructField("severity", T.StringType()), T.StructField("metric_value", T.DoubleType()),
    T.StructField("detail", T.StringType()), T.StructField("ai_explanation", T.StringType())])
AI_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType()), T.StructField("logged_at", T.TimestampType()),
    T.StructField("purpose", T.StringType()), T.StructField("subject", T.StringType()),
    T.StructField("input_text", T.StringType()), T.StructField("output_text", T.StringType()),
    T.StructField("error", T.StringType())])

def save(rows, schema, name):
    if not rows:
        print("nothing to save for", name); return
    clean = [{f.name: r.get(f.name) for f in schema.fields} for r in rows]
    spark.createDataFrame(clean, schema).write.format(WRITE_FORMAT).mode("append").saveAsTable(name)
    print(f"saved {len(rows)} rows -> {name}")

save(table_rows, TABLE_SCHEMA, OUT["tables"])
save(column_rows, COLUMN_SCHEMA, OUT["columns"])
save(findings, FINDING_SCHEMA, OUT["findings"])
save(ai_log, AI_SCHEMA, OUT["ai"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 8. Summary of this run
f = spark.table(OUT["findings"]).where(F.col("run_id") == RUN_ID)
display(f.groupBy("source_name", "severity").count().orderBy("source_name", "severity"))
display(f.orderBy(F.desc("severity"), "source_name", "column_name"))
display(spark.table(OUT["tables"]).where(F.col("run_id") == RUN_ID))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
