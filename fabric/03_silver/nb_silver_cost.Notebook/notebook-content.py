# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_silver_cost: FOCUS 1.2 cost (Azure + Google Cloud) -> Silver
# Out: `silver_cost`, one row per cost line, typed, attributed to an application and cost center.
# # | Step | Defects handled | How |
# |---|---|---|
# | Export re-run | the same day delivered twice | merge on a content hash (all FOCUS columns, not the export run id) |
# | Dates | `MM/dd/yyyy HH:mm` in one file | multi-format parse |
# | Tag keys | `APP`, `app_code`, `Application` | synonym rules |
# | Tag values | application name, `MFG-EXE`, `MFGEXE-PROD`, typos | rules, then **AI classify** on the leftover distinct values (gated) |
# | Attribution | missing tags, stale cost-center tags | owner from the CMDB scope (resource group / project) first, tag second |
# | Services | `GCE`, `Big Query`, empty ServiceCategory | rules, then **AI classify** into the known FOCUS service names |
# | Flags | shadow IT, zombie spend, subscription renamed | flags + completeness gaps (missing day) |
# FOCUS rule: **EffectiveCost** for chargeback and budgets, **BilledCost** for the invoice.

# CELL ********************

%run nb_ledger_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

PIPELINE_RUN_ID = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

RUN_ID = new_run_id(PIPELINE_RUN_ID)
NB, ENTITY = "nb_silver_cost", "cost"
c = bronze("FOCUS_COST")
n_in = c.count()

# 1. export re-run duplicates: same content, different x_exportrunid -> keep the first export
focus_cols = [x for x in c.columns if x not in ("x_exportrunid",) and x.lower() not in LEDGER_COLS]
c = c.withColumn("_content_hash", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(x), F.lit("<null>")) for x in focus_cols]), 256))
c, n_m = dedupe(c, ENTITY, ["_content_hash"], RUN_ID, order_by=[F.col("x_exportrunid"), F.col("_row_hash")], reason="EXPORT_RERUN")

# 2. types
c = parse_ts(c, "chargeperiodstart", "_cps")
c = parse_ts(c, "billingperiodstart", "_bps")
c = (c.withColumn("charge_date", F.to_date("_cps")).withColumn("billing_period_start", F.to_date("_bps"))
      .withColumn("date_format_fixed", ~F.col("chargeperiodstart").rlike(r"^\d{4}-\d{2}-\d{2}")))
for src, dst in [("billedcost", "billed_cost"), ("effectivecost", "effective_cost"), ("listcost", "list_cost"),
                 ("contractedcost", "contracted_cost"), ("consumedquantity", "consumed_quantity")]:
    c = c.withColumn(dst, num(src))

bad = c.filter(F.col("charge_date").isNull() | (F.col("billed_cost").isNull() & F.col("effective_cost").isNull()))
n_q = quarantine(bad, ENTITY, F.when(F.col("charge_date").isNull(), "DATE_UNREADABLE").otherwise("AMOUNT_UNREADABLE"),
                 RUN_ID, key_col="resourceid")
c = c.join(bad.select("_row_hash"), "_row_hash", "left_anti")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Tags: canonical keys (rules), application value (rules, then AI on the leftovers)

# CELL ********************

APP_KEYS = ["application", "app", "app-code", "appcode", "application-code"]
CC_KEYS = ["cost-center", "costcenter", "cost-centre", "costcentre"]
c = c.withColumn("_tags_raw", F.from_json("tags", "map<string,string>"))
c = c.withColumn("_tags", F.transform_keys("_tags_raw", lambda k, v: F.lower(F.regexp_replace(k, "[_ ]", "-"))))
c = (c.withColumn("tag_app_raw", F.coalesce(*[F.col("_tags")[k] for k in APP_KEYS]))
      .withColumn("tag_cc_raw", F.coalesce(*[F.col("_tags")[k] for k in CC_KEYS]))
      .withColumn("tag_key_variant", F.col("tag_app_raw").isNotNull() &
                  F.coalesce(~F.array_contains(F.map_keys("_tags_raw"), "application"), F.lit(True))))

apps = silver("silver_cmdb_applications").select("application_code", "application_name", "cost_center").collect()
codes = [r["application_code"] for r in apps]
name_of = {r["application_code"]: r["application_name"] for r in apps}
by_name = {pyn(r["application_name"]): r["application_code"] for r in apps}
raw_vals = [r[0] for r in c.select("tag_app_raw").distinct().collect() if r[0]]


def rule_app(v):
    u = v.strip().upper()
    if u in codes:
        return u
    s = pyn(re.sub(r"[-_ ]?(PROD|PRD|TEST|DEV|SHARED)$", "", u))
    if s in codes:
        return s
    return by_name.get(pyn(v))


tag_map = {v: rule_app(v) for v in raw_vals if rule_app(v)}
tag_rule_keys = list(tag_map)
leftover = [v for v in raw_vals if v not in tag_map]
tag_map.update(ai_resolve(leftover, ENTITY, "application_tag", codes, RUN_ID,
                          label_text={k: f"{k} {name_of[k]}" for k in codes},
                          instructions="Cloud cost tag value. Return the application code it refers to."))
c = (c.withColumn("tag_app_code", map_values(F.col("tag_app_raw"), tag_map))
      .withColumn("tag_app_method", F.when(F.col("tag_app_raw").isNull(), None)
                  .when(F.col("tag_app_raw").isin(tag_rule_keys), "RULE")
                  .when(F.col("tag_app_code").isNotNull(), "AI").otherwise("UNRESOLVED")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Scope -> CMDB owner, attribution, waste flags

# CELL ********************

c = c.withColumn("scope_key", F.lower(F.when(F.col("providername") == "Google Cloud", F.col("subaccountid"))
                                       .otherwise(F.coalesce(F.regexp_extract("resourceid", r"(?i)/resourcegroups/([^/]+)", 1),
                                                             F.col("x_resourcegroupname")))))
c = c.withColumn("scope_key", F.when(F.col("scope_key") != "", F.col("scope_key")))
c = c.withColumn("rg_name_variant", F.col("x_resourcegroupname").isNotNull() & (F.col("x_resourcegroupname") != F.lower("x_resourcegroupname")))
scopes = silver("silver_cmdb_scopes").select(
    F.col("scope_name").alias("scope_key"), F.col("application_code").alias("cmdb_app_code"), "cmdb_status",
    "cmdb_deleted_date", F.col("account_name").alias("cmdb_account_name"), F.lit(True).alias("_in_cmdb"))
c = c.join(F.broadcast(scopes), "scope_key", "left")
cc_of = F.create_map(*[x for r in apps for x in (F.lit(r["application_code"]), F.lit(r["cost_center"]))])
c = (c.withColumn("application_code", F.coalesce("cmdb_app_code", "tag_app_code"))
      .withColumn("allocation_method", F.when(F.col("cmdb_app_code").isNotNull(), "CMDB_SCOPE")
                  .when(F.col("tag_app_code").isNotNull(), F.concat(F.lit("TAG_"), F.col("tag_app_method")))
                  .when(F.col("scope_key").isNull(), "NO_RESOURCE (commitment / credit)").otherwise("UNALLOCATED"))
      .withColumn("cost_center", cc_of[F.col("application_code")])
      .withColumn("is_shadow_it", F.col("scope_key").isNotNull() & F.col("_in_cmdb").isNull())
      .withColumn("is_zombie_spend", (F.col("cmdb_status") == "Deleted") & (F.col("charge_date") > F.col("cmdb_deleted_date")))
      .withColumn("tag_app_conflict", F.coalesce(F.col("tag_app_code") != F.col("cmdb_app_code"), F.lit(False)))
      .withColumn("tag_cc_conflict", F.coalesce(F.lpad("tag_cc_raw", 4, "0") != F.col("cost_center"), F.lit(False)))
      .withColumn("tag_missing", F.col("scope_key").isNotNull() & F.col("tag_app_raw").isNull()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Services: known FOCUS names (rules), then AI on the leftovers

# CELL ********************

known = {r["servicename"]: r["servicecategory"] for r in
         c.filter(F.col("servicecategory").isNotNull() & (F.col("servicecategory") != "")).select("servicename", "servicecategory").distinct().collect()}
known_key = {pyn(k): k for k in known}
svc_raw = [r[0] for r in c.select("servicename").distinct().collect() if r[0]]
svc_map = {s: (s if s in known else known_key.get(pyn(s))) for s in svc_raw}
svc_map = {k: v for k, v in svc_map.items() if v}
svc_rule_keys = list(svc_map)
svc_left = [s for s in svc_raw if s not in svc_map]
svc_map.update(ai_resolve(svc_left, ENTITY, "service_name", sorted(known), RUN_ID,
                          label_text={k: f"{k} ({v})" for k, v in known.items()},
                          instructions="Cloud service name from a Google Cloud or Azure bill. Return the official service name."))
cat_of = F.create_map(*[x for k, v in known.items() for x in (F.lit(k), F.lit(v))])
c = (c.withColumn("service_name", F.coalesce(map_values(F.col("servicename"), svc_map), F.col("servicename")))
      .withColumn("service_method", F.when(F.col("servicename").isin(list(known)), "SOURCE")
                  .when(F.col("servicename").isin(svc_rule_keys), "RULE")
                  .when(F.col("servicename").isin(list(svc_map)), "AI").otherwise("UNRESOLVED"))
      .withColumn("service_category", F.coalesce(F.when(F.col("servicecategory") != "", F.col("servicecategory")),
                                                 cat_of[F.col("service_name")])))

# sub account: one canonical name per id (the CMDB name, else the first name seen)
first_name = (c.groupBy("subaccountid").agg(F.min_by("subaccountname", "charge_date").alias("_first_name")))
c = (c.join(first_name, "subaccountid", "left")
      .withColumn("sub_account_name", F.coalesce("cmdb_account_name", "_first_name"))
      .withColumn("sub_account_renamed", F.col("subaccountname") != F.col("_first_name")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

out = c.select(
    F.col("providername").alias("provider"), F.col("billingaccountid").alias("billing_account_id"),
    F.col("billingaccountname").alias("billing_account_name"), F.col("subaccountid").alias("sub_account_id"),
    "sub_account_name", F.col("subaccountname").alias("sub_account_name_raw"), "scope_key",
    F.col("resourceid").alias("resource_id"), F.col("resourcename").alias("resource_name"),
    F.col("resourcetype").alias("resource_type"), F.col("regionid").alias("region_id"), "service_name",
    F.col("servicename").alias("service_name_raw"), "service_category", F.col("servicesubcategory").alias("service_subcategory"),
    F.col("skumeter").alias("sku_meter"), F.col("skupricedetails").alias("sku_price_details"),
    F.col("chargecategory").alias("charge_category"), F.col("chargefrequency").alias("charge_frequency"),
    F.col("chargedescription").alias("charge_description"), "charge_date", "billing_period_start",
    "billed_cost", "effective_cost", "list_cost", "contracted_cost", "consumed_quantity",
    F.col("consumedunit").alias("consumed_unit"), F.col("pricingcategory").alias("pricing_category"),
    F.col("commitmentdiscountname").alias("commitment_discount_name"), F.col("billingcurrency").alias("currency"),
    "application_code", "cost_center", "allocation_method", "tag_app_raw", "tag_app_code", "tag_app_method",
    "tag_key_variant", "tag_missing", "tag_cc_raw", "tag_app_conflict", "tag_cc_conflict", "is_shadow_it",
    "is_zombie_spend", "cmdb_status", "rg_name_variant", "service_method", "sub_account_renamed", "date_format_fixed",
    F.col("x_exportrunid").alias("export_run_id"), "_source_file", "_row_hash")
n_out = silver_write(out, "silver_cost")
s = silver("silver_cost")
log_step(RUN_ID, "silver", ENTITY, "FOCUS_COST", "silver_cost", rows_out=n_out, rows_in=n_in, quarantined=n_q,
         merged=n_m, amount=s.select(F.sum("billed_cost")).first()[0], notebook=NB)

# completeness: every provider should have usage lines every day
rng = s.agg(F.min("charge_date"), F.max("charge_date")).first()
cal = spark.sql(f"select explode(sequence(date'{rng[0]}', date'{rng[1]}')) as charge_date")
for p in [r[0] for r in s.select("provider").distinct().collect()]:
    have = s.filter((F.col("provider") == p) & (F.col("charge_category") == "Usage")).select("charge_date").distinct()
    for r in cal.join(have, "charge_date", "left_anti").collect():
        log_gap(RUN_ID, ENTITY, "MISSING_DAY", f"{p}|{r[0]}", "no usage lines for this provider and day", layer="silver")
s.groupBy("allocation_method").agg(F.count("*").alias("rows"), F.round(F.sum("effective_cost"), 2).alias("effective_cost")).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
