# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_ai_review: human review of AI decisions the gate didn't accept
# 1. Run the first cell: it shows the open items in `meta_ai_review_queue` (input, AI proposal, similarity score).
# 2. Fill `DECISIONS` with what you approve (`input -> label`) or reject (`label = None`).
# 3. Run the second cell. Approved mappings become rules (`meta_ai_mappings`, source `HUMAN`): next run they are
#    applied without calling AI. Then re-run the Silver notebook of that entity.

# CELL ********************

%run nb_ledger_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

q = trust_read("meta_ai_review_queue")
open_items = q.filter("status = 'OPEN'").dropDuplicates(["purpose", "input_text"]) if q is not None else None
display(open_items.select("entity", "purpose", "input_text", "proposed_label", "score") if open_items is not None else spark.createDataFrame([], "x string"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# purpose -> {input_text: approved label or None to reject}
DECISIONS = {
    # "application_tag": {"MFGEX": "MFGEXE"},
}
now = datetime.datetime.utcnow()
rows = [(purpose, inp, lab, "HUMAN", "ACTIVE" if lab else "REJECTED", "review", now)
        for purpose, d in DECISIONS.items() for inp, lab in d.items()]
if rows:
    trust_append(spark.createDataFrame(rows, "purpose string, input_text string, label string, source string, status string, "
                                       "run_id string, logged_at timestamp"), "meta_ai_mappings")
    done = spark.createDataFrame([(p, i, "CLOSED", now) for p, i, *_ in rows], "purpose string, input_text string, status string, logged_at timestamp")
    trust_append(done.withColumn("run_id", F.lit("review")), "meta_ai_review_queue")
    print(f"{len(rows)} decisions saved; re-run the Silver notebooks to apply them")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
