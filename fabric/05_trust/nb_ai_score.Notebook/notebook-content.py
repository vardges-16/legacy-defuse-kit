# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_ai_score: cleansing quality against the answer keys
# The answer keys (planted defects with the expected value) are ground truth for **scoring only**: they are read here
# and nowhere else. Upload once to `lh_trust` Files `answer_keys/` (same layout as the repo folder `answer-keys/`).
# # * `meta_ai_scorecard`: per cleansing task (rules + AI), how many planted cases were resolved, precision, recall, F1,
#   split by method (RULE / AI / UNRESOLVED).
# * `meta_defect_detection`: per planted defect type, how many the pipeline caught.

# CELL ********************

%run nb_ledger_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

PIPELINE_RUN_ID = ""
ANSWER_KEYS = "lh_trust.Lakehouse/Files/answer_keys"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

RUN_ID = new_run_id(PIPELINE_RUN_ID)
NOW = datetime.datetime.utcnow()


def key(system):
    return (spark.read.option("header", True).csv(f"{ROOT}/{ANSWER_KEYS}/{system}/answer_key.csv"))


cost, apps = silver("silver_cost"), silver("silver_cmdb_applications")
scopes, inst = silver("silver_cmdb_scopes"), silver("silver_license_installations")
prods = {r["product_id"]: r["product_name"] for r in silver("silver_license_products").collect()}
score_rows, detect_rows = [], []


def score(task, system, defect, pairs):
    """pairs: DataFrame with expected, predicted, method (one row per planted case)."""
    for method in ["ALL"] + [r[0] for r in pairs.select("method").distinct().collect()]:
        p = pairs if method == "ALL" else pairs.filter(F.col("method") == method)
        n = p.count()
        attempted = p.filter(F.col("predicted").isNotNull()).count()
        correct = p.filter(F.col("predicted") == F.col("expected")).count()
        prec = correct / attempted if attempted else None
        rec = correct / n if n else None
        f1 = 2 * prec * rec / (prec + rec) if prec and rec else None
        score_rows.append((RUN_ID, task, system, defect, method, n, attempted, correct, prec, rec, f1, NOW))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## AI / rule cleansing tasks

# CELL ********************

# cost C04: application tag values (names, typos) -> application code
k = key("cost").filter("defect_id = 'C04'").select(F.col("record_id").alias("resource_id"), F.col("expected_value").alias("expected"))
pred = cost.groupBy("resource_id").agg(F.first("tag_app_code", ignorenulls=True).alias("predicted"),
                                       F.first("tag_app_method", ignorenulls=True).alias("method"))
score("application tag value -> code", "cost", "C04", k.join(pred, "resource_id", "left").fillna("UNRESOLVED", ["method"]))

# cost C06: non-standard service names -> FOCUS service name
k = (key("cost").filter("defect_id = 'C06'")
     .select(F.col("record_id").alias("resource_id"), F.trim(F.split("expected_value", "/")[0]).alias("expected")))
pred = cost.groupBy("resource_id").agg(F.first("service_name").alias("predicted"), F.first("service_method").alias("method"))
score("service name -> FOCUS service", "cost", "C06", k.join(pred, "resource_id", "left").fillna("UNRESOLVED", ["method"]))

# cmdb M02: department variants -> finance department
k = key("cmdb").filter("defect_id = 'M02'").select(F.col("record_id").alias("application_id"), F.col("expected_value").alias("expected"))
pred = apps.select("application_id", F.col("department").alias("predicted"), F.col("department_method").alias("method"))
score("department name -> finance department", "cmdb", "M02", k.join(pred, "application_id", "left").fillna("UNRESOLVED", ["method"]))

# license D01: discovery product names -> catalog product
k = key("license").filter("defect_id = 'D01'").select(F.col("record_id").cast("bigint").alias("installation_id"),
                                                       F.col("expected_value").alias("expected"))
pred = inst.select("installation_id", map_values(F.col("product_id").cast("string"), {str(a): b for a, b in prods.items()}).alias("predicted"),
                   F.col("product_method").alias("method"))
score("discovery name -> licensed product", "license", "D01", k.join(pred, "installation_id", "left").fillna("UNRESOLVED", ["method"]))

# cmdb M05 + M06: scope owner (no group / stale group) -> application
truth = {r["record_id"]: r for r in key("cmdb").filter("defect_id in ('M05','M06')").collect()}
grp_app = {r["asset_group_id"]: r["application_id"] for r in silver("silver_cmdb_asset_groups").collect()}
code_of = {r["application_id"]: r["application_code"] for r in apps.collect()}
exp = spark.createDataFrame([(rid, code_of.get(grp_app.get(r["expected_value"])), r["defect_id"]) for rid, r in truth.items()],
                            "scope_id string, expected string, defect string")
pred = scopes.select("scope_id", F.col("application_code").alias("predicted"), F.col("mapping_source").alias("method"))
score("scope owner (missing / stale mapping)", "cmdb", "M05+M06", exp.join(pred, "scope_id", "left").fillna("UNRESOLVED", ["method"]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Defect detection: did the pipeline catch each planted defect?

# CELL ********************

def detect(system, defect, expected_ids, caught_ids, note=""):
    exp_set, got = set(expected_ids), set(caught_ids)
    hit = len(exp_set & got)
    detect_rows.append((RUN_ID, system, defect, len(exp_set), hit, hit / len(exp_set) if exp_set else None, note, NOW))


def ids(system, defect, col="record_id"):
    return [r[0] for r in key(system).filter(F.col("defect_id") == defect).select(col).collect()]


qlog, mlog, gaps = trust_read("meta_quarantine"), trust_read("meta_merge_log"), trust_read("meta_completeness")
detect("cost", "C03 tag missing", ids("cost", "C03"), [r[0] for r in cost.filter("tag_missing").select("resource_id").distinct().collect()])
detect("cost", "C02 tag key variant", ids("cost", "C02"), [r[0] for r in cost.filter("tag_key_variant").select("resource_id").distinct().collect()])
detect("cost", "C05 stale cost-center tag", ids("cost", "C05"), [r[0] for r in cost.filter("tag_cc_conflict").select("resource_id").distinct().collect()])
detect("cost", "C07 zombie spend", ids("cost", "C07"), [r[0] for r in cost.filter("is_zombie_spend").select("scope_key").distinct().collect()])
detect("cost", "C08 shadow IT", ids("cost", "C08"), [r[0] for r in cost.filter("is_shadow_it").select("scope_key").distinct().collect()])
detect("cost", "C10 RG name variant", ids("cost", "C10"), [r[0] for r in cost.filter("rg_name_variant").select("scope_key").distinct().collect()])
detect("cost", "C01 export re-run", ["rerun"], ["rerun"] if mlog is not None and mlog.filter("entity = 'cost' and reason_code = 'EXPORT_RERUN'").count() else [])
detect("cost", "C09 missing day", ["GCP"], ["GCP"] if gaps is not None and gaps.filter("entity = 'cost' and expected_key like 'Google%'").count() else [])
detect("cmdb", "M01+M04 duplicates", ids("cmdb", "M01") + ids("cmdb", "M04"),
       [r[0] for r in key("cmdb").filter("defect_id in ('M01','M04')").select("record_id").collect()
        if apps.filter(F.col("application_id") == r[0]).count() == 0 and scopes.filter(F.col("scope_id") == r[0]).count() == 0])
detect("cmdb", "M07 retired app owns active scopes", ids("cmdb", "M07"),
       [r[0] for r in scopes.filter("retired_app_active_scope").select("application_id").distinct().collect()])
detect("cmdb", "M10 stale mapping date", ids("cmdb", "M10"), [r[0] for r in scopes.filter("mapping_stale").select("scope_id").collect()])
detect("license", "D06 double licensing", ids("license", "D06"),
       [str(r[0]) for r in inst.filter("double_licensing").select("installation_id").collect()])
detect("license", "D02 unmanaged host", ids("license", "D02"), [str(r[0]) for r in inst.filter("not host_in_cloud").select("installation_id").collect()])
detect("license", "D03 deleted VM still seen", ids("license", "D03"),
       [str(r[0]) for r in inst.filter("vm_deleted_still_seen").select("installation_id").collect()])
sto = silver("silver_backup_storage")
detect("vm-backups", "D01 orphaned backup", ids("vm-backups", "D01"),
       [f"{r[0]}|{str(r[1])[:7]}" for r in sto.filter("is_orphaned").select("vm_name", "month").collect()])
detect("vm-backups", "D02 VM not in CMDB", ids("vm-backups", "D02"),
       [r[0] for r in silver("silver_backup_jobs").filter("not vm_in_cmdb").select("vm_name").distinct().collect()])
detect("vm-backups", "D05 size <= 0", ids("vm-backups", "D05"),
       [] if qlog is None else [r[0] for r in qlog.filter("entity = 'vm_backup_jobs'").select("record_key").collect()])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sc = spark.createDataFrame(score_rows, "run_id string, task string, system string, defect_id string, method string, cases long, "
                           "attempted long, correct long, precision double, recall double, f1 double, scored_at timestamp")
dd = spark.createDataFrame(detect_rows, "run_id string, system string, defect string, planted long, detected long, "
                           "detection_rate double, note string, scored_at timestamp")
trust_append(sc, "meta_ai_scorecard")
trust_append(dd, "meta_defect_detection")
display(sc.filter("method = 'ALL'").drop("run_id", "scored_at"))
display(sc.filter("method != 'ALL'").drop("run_id", "scored_at"))
display(dd.drop("run_id", "scored_at"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
