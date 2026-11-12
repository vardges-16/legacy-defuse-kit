# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_run_audit: pipeline run audit (Trust Ledger)
# Last step of one_pipeline_to_rule_them_all. Pulls every activity run of this pipeline run from the Fabric REST API
# (queryactivityruns), marks expected steps that never ran as Skipped, joins the data checks of the same run
# (meta_load_ledger, meta_reconciliation) and writes:
# * meta_pipeline_activity_runs: one row per activity (status, start, end, duration, error)
# * meta_pipeline_runs: one row per run (overall status, failed/skipped steps, ledger rows, reconciliation FAILs)
# Exit value (JSON) drives the If Condition + Fail activity, so the pipeline is red when a step or a data check failed.

# CELL ********************

%run nb_ledger_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

PIPELINE_RUN_ID = ""                                         # @pipeline().RunId
WORKSPACE_ID = ""                                            # @pipeline().DataFactory (Workspace ID)
PIPELINE_NAME = "one_pipeline_to_rule_them_all"
TRIGGER_TYPE = ""                                            # @pipeline().TriggerType (optional)
EXPECTED_STEPS = "bronze_silver,gold_build,reconcile,ai_score"
SELF_NAME = "run_audit"                                      # this activity's name in the pipeline

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import datetime, json, re, requests

now = datetime.datetime.utcnow()
WORKSPACE_ID = WORKSPACE_ID or notebookutils.runtime.context.get("currentWorkspaceId")
if not PIPELINE_RUN_ID:
    notebookutils.notebook.exit(json.dumps({"failed": 0, "steps": "none", "reconcile_fail": 0,
                                            "note": "no pipeline run id: interactive run, nothing audited"}))


def ts(s):
    """ISO string from the API (7-digit fractions, Z) -> naive UTC datetime."""
    if not s:
        return None
    s = re.sub(r"(\.\d{6})\d+", r"\1", s).replace("Z", "+00:00")
    return datetime.datetime.fromisoformat(s).replace(tzinfo=None)


# 1. Activity runs of this pipeline run (paged by continuationToken)
headers = {"Authorization": f"Bearer {notebookutils.credentials.getToken('pbi')}", "Content-Type": "application/json"}
url = (f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}"
       f"/datapipelines/pipelineruns/{PIPELINE_RUN_ID}/queryactivityruns")
body = {"filters": [], "orderBy": [{"orderBy": "ActivityRunStart", "order": "ASC"}],
        "lastUpdatedAfter": (now - datetime.timedelta(days=2)).isoformat() + "Z",
        "lastUpdatedBefore": (now + datetime.timedelta(hours=1)).isoformat() + "Z"}
runs, token = [], None
while True:
    if token:
        body["continuationToken"] = token
    resp = requests.post(url, headers=headers, json=body, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    runs += data if isinstance(data, list) else data.get("value", [])
    token = None if isinstance(data, list) else data.get("continuationToken")
    if not token:
        break
print(len(runs), "activity runs returned by the API")
if not runs:
    raise RuntimeError(f"queryactivityruns returned no activity runs for run id {PIPELINE_RUN_ID}: "
                       "cannot audit this run (check that the pipeline passes @pipeline().RunId)")

# 2. One row per activity (last attempt wins), expected steps that never ran -> Skipped
acts = {}
for r in runs:
    name = r.get("activityName")
    if name == SELF_NAME:
        continue
    err = r.get("error") or {}
    msg = (err.get("message") or "")[:1000] or None
    acts[name] = (PIPELINE_RUN_ID, name, r.get("activityType"), r.get("status"),
                  ts(r.get("activityRunStart")), ts(r.get("activityRunEnd")),
                  None if r.get("durationInMs") is None else r["durationInMs"] / 1000.0,
                  err.get("errorCode") or None, msg, r.get("activityRunId"), now)
expected = [s.strip() for s in EXPECTED_STEPS.split(",") if s.strip()]
for step in expected:
    if step not in acts:
        acts[step] = (PIPELINE_RUN_ID, step, None, "Skipped", None, None, None, None,
                      "did not run: an upstream step failed or was skipped", None, now)

ACT_SCHEMA = ("run_id string, activity_name string, activity_type string, status string, started_at timestamp, "
              "ended_at timestamp, duration_s double, error_code string, error_message string, "
              "activity_run_id string, audited_at timestamp")
act_df = spark.createDataFrame(list(acts.values()), ACT_SCHEMA)

# 3. Data checks of the same run (notebooks use PIPELINE_RUN_ID as their run id)
ledger, rec = trust_read("meta_load_ledger"), trust_read("meta_reconciliation")
ledger_rows = 0 if ledger is None else ledger.filter(F.col("run_id") == PIPELINE_RUN_ID).count()
rec_fail = 0 if rec is None else rec.filter((F.col("run_id") == PIPELINE_RUN_ID) & (F.col("status") == "FAIL")).count()
rec_total = 0 if rec is None else rec.filter(F.col("run_id") == PIPELINE_RUN_ID).count()

# 4. Run summary
failed = [a for a in expected if acts[a][3] not in ("Succeeded", "Skipped")]   # Failed, Cancelled, ...
skipped = [a for a in expected if acts[a][3] == "Skipped"]
starts = [v[4] for v in acts.values() if v[4] is not None]
started = min(starts) if starts else None
status = "Succeeded" if not failed and not skipped and rec_fail == 0 else "Failed"
run_row = [(PIPELINE_RUN_ID, PIPELINE_NAME, TRIGGER_TYPE or None, started, now,
            None if started is None else (now - started).total_seconds(), status,
            ",".join(failed) or None, ",".join(skipped) or None, ledger_rows, rec_total, rec_fail, now)]
RUN_SCHEMA = ("run_id string, pipeline_name string, trigger_type string, started_at timestamp, ended_at timestamp, "
              "duration_s double, status string, failed_steps string, skipped_steps string, ledger_rows long, "
              "reconcile_checks long, reconcile_fail long, audited_at timestamp")

trust_append(act_df, "meta_pipeline_activity_runs")
trust_append(spark.createDataFrame(run_row, RUN_SCHEMA), "meta_pipeline_runs")
display(act_df.orderBy("started_at"))
print(f"{status}: failed={failed} skipped={skipped} ledger_rows={ledger_rows} reconcile FAIL={rec_fail}/{rec_total}")

# 5. Exit value for the If Condition in the pipeline
notebookutils.notebook.exit(json.dumps({
    "failed": len(failed) + len(skipped) + rec_fail,
    "steps": ",".join(failed + skipped) or "none",
    "reconcile_fail": rec_fail}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
