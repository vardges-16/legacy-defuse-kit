# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0eec7aa2-09fd-454f-9d23-5c1b67ed94f8",
# META       "default_lakehouse_name": "lh_bronze",
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

# # Cost Center Reconciliation & Imputation Pipeline
# This notebook analyzes cloud consumption data, identifies missing `CostCenter` allocations, uses historical exact matching, and applies Microsoft Fabric PySpark AI Functions (`synapse.ml.spark.aifunc`) to intelligently infer missing cost centers.

# CELL ********************

# 1. Environment Setup & Library Imports
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
import synapse.ml.spark.aifunc as aifunc

# Configure global defaults for Fabric PySpark AI Functions (following MS Learn specs)
# You can customize deploymentName or apiType as needed
aifunc.default_conf.set_deployment_name("gpt-4o")
print("PySpark AI Functions configured successfully with global default deployment.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. Load Consumption Dataset from Lakehouse Table
# Source changed to use an existing Lakehouse table instead of a CSV file

table_name = "dbo.FOCUS_COST"

df_raw = spark.table(table_name)

print(f"Loaded {df_raw.count()} rows from table {table_name}")
# df_raw.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3. Data Cleansing, ResourceId Decomposition & Standardization
# In this dataset, CostCenter is stored inside the TAGS JSON string, not as a top-level column.

# Extract CostCenter from TAGS JSON (handle both "CostCenter" and "COSTCENTER" just in case)
cost_center_extracted = F.coalesce(
    F.get_json_object(F.col("TAGS"), "$.CostCenter"),
    F.get_json_object(F.col("TAGS"), "$.COSTCENTER")
)

# Decompose Azure-style ResourceId into subscription and resource group for richer analysis
# Typical pattern:
#   /subscriptions/<subId>/resourceGroups/<rgName>/providers/<provider>/<type>/<name>

df_enriched = df_raw \
    .withColumn(
        "SubscriptionId",
        F.regexp_extract(F.col("ResourceId"), r"/subscriptions/([^/]+)", 1)
    ) \
    .withColumn(
        "ResourceGroup",
        F.regexp_extract(F.col("ResourceId"), r"/resourceGroups/([^/]+)", 1)
    )

# Normalize CostCenter values: treat empty/placeholder strings as null
df_cleaned = df_enriched.withColumn(
    "CostCenter",
    F.when(
        cost_center_extracted.isin("", "NULL", "N/A", "None"),
        None
    ).otherwise(cost_center_extracted)
)

# Split into historical reference (has CostCenter) and target for reconciliation (missing CostCenter)
df_history = df_cleaned.filter(F.col("CostCenter").isNotNull())

# Add a stable technical key for missing rows to support multiple reconciliation passes
df_missing = (
    df_cleaned
    .filter(F.col("CostCenter").isNull())
    .withColumn("_row_id", F.monotonically_increasing_id())
)

print(f"Historical records with valid CostCenter: {df_history.count()}")
print(f"Records missing CostCenter for reconciliation: {df_missing.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3b. Subscription & Resource Group Analysis for Missing CostCenter
"""Quick profiling of ResourceId-derived attributes to understand
where most missing CostCenter records sit (by SubscriptionId and ResourceGroup).
"""

# Distribution of missing CostCenter by SubscriptionId
print("Top subscriptions by count of missing CostCenter:")
missing_by_sub = (
    df_missing
    .groupBy("SubscriptionId")
    .count()
    .orderBy(F.desc("count"))
)
display(missing_by_sub.limit(20))

# Distribution of missing CostCenter by ResourceGroup
print("Top resource groups by count of missing CostCenter:")
missing_by_rg = (
    df_missing
    .groupBy("ResourceGroup")
    .count()
    .orderBy(F.desc("count"))
)
display(missing_by_rg.limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 4. (Disabled) Rule-Based Historical Exact Matching
"""Historically, this cell performed rule-based reconciliation using
ResourceType + ServiceName. For the current scenario, you requested
**LLM-only** reconciliation with three distinct LLM methods.

To ensure all unresolved records go to the LLM, we:
- Keep `df_missing` as-is
- Initialize an empty `df_rule_resolved` for schema compatibility
- Set `df_llm_target = df_missing`
"""

# Create an empty df_rule_resolved with same schema as df_missing,
# plus the ImputationMethod column for downstream union.
df_rule_resolved = (
    df_missing
    .limit(0)
    .withColumn("ImputationMethod", F.lit(None).cast(StringType()))
)

# All missing records go to LLM
df_llm_target = df_missing

print(f"Rule-based reconciliation disabled. Rows sent to LLM: {df_llm_target.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 4b. (Disabled) Additional Rule-Based Reconciliation Methods
"""This cell previously extended rule-based reconciliation using
Historical_ResourceName and Historical_ServiceName.

For the current scenario, you requested **LLM-only** reconciliation
with three distinct LLM methods. Therefore this cell is now a no-op,
kept only for documentation.
"""

print("Additional rule-based reconciliation disabled; all unresolved rows are handled by LLM.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5. LLM-Powered Reconciliation Function using PySpark AI Functions
from synapse.ml.spark import aifunc

# NOTE:
# Runtime 2.0 uses the DataFrame accessor `.ai` with `generate_response(...)`.
# We implement **three** LLM-based methods, each with a different focus
# and assigned a different ImputationMethod value:
#   - LLM_Subscription_RG   (uses SubscriptionId + ResourceGroup heavily)
#   - LLM_Service_Tags      (uses ServiceName/ResourceType + TAGS)
#   - LLM_Generic_Fallback  (generic catch-all prompt)


def _run_llm_method(df_in, message_expr, prompt_text, method_name):
    """Helper to run a single LLM method on a subset of rows.

    Returns an enriched DataFrame with CostCenter + ImputationMethod,
    or an empty DataFrame if df_in is empty.
    """
    if df_in.rdd.isEmpty():
        # Return an empty DF with required columns for unionByName
        return df_in

    df_prompt = df_in.withColumn("message", message_expr)

    df_llm = df_prompt.ai.generate_response(
        prompt=prompt_text,
        output_col="CostCenter_raw",
    )

    enriched = (
        df_llm
        .withColumn("CostCenter", F.trim(F.col("CostCenter_raw")))
        .drop("CostCenter_raw", "message")
        .withColumn("ImputationMethod", F.lit(method_name))
    )
    return enriched


def infer_cost_center_with_llm(spark_df):
    """Infer CostCenter using three LLM-only strategies.

    Each strategy operates on a disjoint subset of rows (using `_row_id`):
      1) LLM_Subscription_RG   – rows with SubscriptionId & ResourceGroup
      2) LLM_Service_Tags      – remaining rows with ServiceName/ResourceType
      3) LLM_Generic_Fallback  – anything still unresolved
    """

    remaining = spark_df
    results = []

    # --- Method 1: LLM_Subscription_RG ---
    df_subrg = remaining.filter(
        F.col("SubscriptionId").isNotNull() & F.col("ResourceGroup").isNotNull()
    )

    msg_subrg = F.concat(
        F.lit(
            "You are a financial allocation assistant. Based primarily on SubscriptionId and "
            "ResourceGroup, determine the correct Cost Center for this resource. Return ONLY the "
            "Cost Center code (for example: CC-FINANCE, CC-ENG, CC-MARKETING, CC-IT).\n"
        ),
        F.lit("Subscription: "), F.coalesce(F.col("SubscriptionId"), F.lit("N/A")), F.lit("\n"),
        F.lit("Resource Group: "), F.coalesce(F.col("ResourceGroup"), F.lit("N/A")), F.lit("\n"),
        F.lit("Resource Name: "), F.coalesce(F.col("ResourceName"), F.lit("N/A")), F.lit("\n"),
        F.lit("Service Name: "), F.coalesce(F.col("ServiceName"), F.lit("N/A")), F.lit("\n"),
        F.lit("Resource Type: "), F.coalesce(F.col("ResourceType"), F.lit("N/A")), F.lit("\n"),
        F.lit("Environment / Tags: "),
        F.coalesce(F.col("TAGS"), F.col("SubAccountName"), F.lit("N/A"))
    )

    prompt_subrg = (
        "For each row, read the 'message' text and use SubscriptionId and ResourceGroup as the "
        "primary signals to choose the Cost Center. If the subscription and resource group are "
        "associated with specific business units, align the Cost Center accordingly. Return ONLY "
        "the Cost Center code, with no extra text."
    )

    df_llm_subrg = _run_llm_method(
        df_subrg,
        msg_subrg,
        prompt_subrg,
        method_name="LLM_Subscription_RG",
    )

    if not df_llm_subrg.rdd.isEmpty():
        results.append(df_llm_subrg)
        remaining = remaining.join(
            df_llm_subrg.select("_row_id").distinct(),
            on="_row_id",
            how="left_anti",
        )

    # --- Method 2: LLM_Service_Tags ---
    df_svc = remaining.filter(
        F.col("ServiceName").isNotNull() | F.col("ResourceType").isNotNull()
    )

    msg_svc = F.concat(
        F.lit(
            "You are assigning Cost Centers based mainly on the type of cloud service and its "
            "tags. Use ServiceName, ResourceType, and TAGS to infer the Cost Center. Return ONLY "
            "the Cost Center code.\n"
        ),
        F.lit("Service Name: "), F.coalesce(F.col("ServiceName"), F.lit("N/A")), F.lit("\n"),
        F.lit("Resource Type: "), F.coalesce(F.col("ResourceType"), F.lit("N/A")), F.lit("\n"),
        F.lit("Subscription: "), F.coalesce(F.col("SubscriptionId"), F.lit("N/A")), F.lit("\n"),
        F.lit("Resource Group: "), F.coalesce(F.col("ResourceGroup"), F.lit("N/A")), F.lit("\n"),
        F.lit("Resource Name: "), F.coalesce(F.col("ResourceName"), F.lit("N/A")), F.lit("\n"),
        F.lit("Environment / Tags: "),
        F.coalesce(F.col("TAGS"), F.col("SubAccountName"), F.lit("N/A"))
    )

    prompt_svc = (
        "For each row, read the 'message' text and use the service characteristics and tags to "
        "select the Cost Center. Services used by engineering, analytics, or marketing teams "
        "should map to their respective Cost Centers. Return ONLY the Cost Center code."
    )

    df_llm_svc = _run_llm_method(
        df_svc,
        msg_svc,
        prompt_svc,
        method_name="LLM_Service_Tags",
    )

    if not df_llm_svc.rdd.isEmpty():
        results.append(df_llm_svc)
        remaining = remaining.join(
            df_llm_svc.select("_row_id").distinct(),
            on="_row_id",
            how="left_anti",
        )

    # --- Method 3: LLM_Generic_Fallback ---
    df_generic = remaining

    msg_generic = F.concat(
        F.lit(
            "You are a financial allocation assistant. Based on ALL available context, choose the "
            "most appropriate Cost Center. Return ONLY the Cost Center code.\n"
        ),
        F.lit("Subscription: "), F.coalesce(F.col("SubscriptionId"), F.lit("N/A")), F.lit("\n"),
        F.lit("Resource Group: "), F.coalesce(F.col("ResourceGroup"), F.lit("N/A")), F.lit("\n"),
        F.lit("Resource Name: "), F.coalesce(F.col("ResourceName"), F.lit("N/A")), F.lit("\n"),
        F.lit("Service Name: "), F.coalesce(F.col("ServiceName"), F.lit("N/A")), F.lit("\n"),
        F.lit("Resource Type: "), F.coalesce(F.col("ResourceType"), F.lit("N/A")), F.lit("\n"),
        F.lit("Environment / Tags: "),
        F.coalesce(F.col("TAGS"), F.col("SubAccountName"), F.lit("N/A"))
    )

    prompt_generic = (
        "For each row, read the 'message' text and use ALL available attributes (subscription, "
        "resource group, service, type, tags) to choose a Cost Center. If uncertain, pick the "
        "best single Cost Center based on the closest match. Return ONLY the Cost Center code."
    )

    df_llm_generic = _run_llm_method(
        df_generic,
        msg_generic,
        prompt_generic,
        method_name="LLM_Generic_Fallback",
    )

    if not df_llm_generic.rdd.isEmpty():
        results.append(df_llm_generic)

    # Union all LLM results
    if results:
        df_all = results[0]
        for df_part in results[1:]:
            df_all = df_all.unionByName(df_part, allowMissingColumns=True)
        return df_all
    else:
        # No rows (unlikely); return empty with expected columns
        return spark_df.limit(0).withColumn("CostCenter", F.lit(None).cast(StringType())) \
            .withColumn("ImputationMethod", F.lit("LLM_Generic_Fallback"))


# Apply LLM inference if there are unassigned records
if df_llm_target.count() > 0:
    df_llm_resolved = infer_cost_center_with_llm(df_llm_target)
else:
    # Empty dataframe schema alignment if all resolved by rules
    df_llm_resolved = df_rule_resolved.limit(0).withColumn("ImputationMethod", F.lit("LLM_Generic_Fallback"))

print("Resolved via LLM_Subscription_RG: ", df_llm_resolved.filter(F.col("ImputationMethod") == "LLM_Subscription_RG").count())
print("Resolved via LLM_Service_Tags: ", df_llm_resolved.filter(F.col("ImputationMethod") == "LLM_Service_Tags").count())
print("Resolved via LLM_Generic_Fallback: ", df_llm_resolved.filter(F.col("ImputationMethod") == "LLM_Generic_Fallback").count())
print(f"Resolved via LLM AI Function (total): {df_llm_resolved.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 6. Combine Reconciled Data, Compare Before/After & Write to Lakehouse

# Combine historical records, rule-resolved records, and LLM-resolved records
# Note: technical key `_row_id` is dropped when selecting columns to match history schema.

df_history_final = df_history.withColumn("ImputationMethod", F.lit("Original"))

df_final_reconciled = (
    df_history_final
    .unionByName(df_rule_resolved.select(df_history_final.columns), allowMissingColumns=True)
    .unionByName(df_llm_resolved.select(df_history_final.columns), allowMissingColumns=True)
)

# Add JSON reasoning/log column explaining why a given CostCenter was chosen
# This is a structured JSON string you can persist and audit later.

df_final_reconciled = df_final_reconciled.withColumn(
    "CostCenterReasonLog",
    F.when(
        F.col("ImputationMethod") == "Original",
        F.to_json(
            F.struct(
                F.lit("Original").alias("reasonType"),
                F.lit("CostCenter already present in source data.").alias("description"),
                F.col("CostCenter").alias("costCenter"),
                F.col("SubscriptionId").alias("subscriptionId"),
                F.col("ResourceGroup").alias("resourceGroup"),
                F.col("ResourceName").alias("resourceName"),
                F.col("ServiceName").alias("serviceName"),
                F.col("ResourceType").alias("resourceType"),
                F.col("TAGS").alias("tags")
            )
        )
    ).when(
        F.col("ImputationMethod") == "Historical_Rule",
        F.to_json(
            F.struct(
                F.lit("Historical_Rule").alias("reasonType"),
                F.lit(
                    "CostCenter taken from historical record with same ResourceType and ServiceName."
                ).alias("description"),
                F.col("CostCenter").alias("costCenter"),
                F.col("SubscriptionId").alias("subscriptionId"),
                F.col("ResourceGroup").alias("resourceGroup"),
                F.col("ResourceName").alias("resourceName"),
                F.col("ServiceName").alias("serviceName"),
                F.col("ResourceType").alias("resourceType"),
                F.col("TAGS").alias("tags")
            )
        )
    ).when(
        F.col("ImputationMethod") == "Historical_ResourceName",
        F.to_json(
            F.struct(
                F.lit("Historical_ResourceName").alias("reasonType"),
                F.lit(
                    "CostCenter taken from historical record with the same ResourceName."
                ).alias("description"),
                F.col("CostCenter").alias("costCenter"),
                F.col("SubscriptionId").alias("subscriptionId"),
                F.col("ResourceGroup").alias("resourceGroup"),
                F.col("ResourceName").alias("resourceName"),
                F.col("ServiceName").alias("serviceName"),
                F.col("ResourceType").alias("resourceType"),
                F.col("TAGS").alias("tags")
            )
        )
    ).when(
        F.col("ImputationMethod") == "Historical_ServiceName",
        F.to_json(
            F.struct(
                F.lit("Historical_ServiceName").alias("reasonType"),
                F.lit(
                    "CostCenter taken from historical record with the same ServiceName."
                ).alias("description"),
                F.col("CostCenter").alias("costCenter"),
                F.col("SubscriptionId").alias("subscriptionId"),
                F.col("ResourceGroup").alias("resourceGroup"),
                F.col("ResourceName").alias("resourceName"),
                F.col("ServiceName").alias("serviceName"),
                F.col("ResourceType").alias("resourceType"),
                F.col("TAGS").alias("tags")
            )
        )
    ).when(
        F.col("ImputationMethod") == "LLM_Subscription_RG",
        F.to_json(
            F.struct(
                F.lit("LLM_Subscription_RG").alias("reasonType"),
                F.lit(
                    "CostCenter inferred by LLM using SubscriptionId and ResourceGroup as primary signals, with other attributes as context."
                ).alias("description"),
                F.col("CostCenter").alias("costCenter"),
                F.col("SubscriptionId").alias("subscriptionId"),
                F.col("ResourceGroup").alias("resourceGroup"),
                F.col("ResourceName").alias("resourceName"),
                F.col("ServiceName").alias("serviceName"),
                F.col("ResourceType").alias("resourceType"),
                F.col("TAGS").alias("tags")
            )
        )
    ).when(
        F.col("ImputationMethod") == "LLM_Service_Tags",
        F.to_json(
            F.struct(
                F.lit("LLM_Service_Tags").alias("reasonType"),
                F.lit(
                    "CostCenter inferred by LLM using ServiceName, ResourceType, and TAGS as primary signals, with subscription & resource group as secondary context."
                ).alias("description"),
                F.col("CostCenter").alias("costCenter"),
                F.col("SubscriptionId").alias("subscriptionId"),
                F.col("ResourceGroup").alias("resourceGroup"),
                F.col("ResourceName").alias("resourceName"),
                F.col("ServiceName").alias("serviceName"),
                F.col("ResourceType").alias("resourceType"),
                F.col("TAGS").alias("tags")
            )
        )
    ).when(
        F.col("ImputationMethod") == "LLM_Generic_Fallback",
        F.to_json(
            F.struct(
                F.lit("LLM_Generic_Fallback").alias("reasonType"),
                F.lit(
                    "CostCenter inferred by LLM using all available attributes with a generic fallback strategy."
                ).alias("description"),
                F.col("CostCenter").alias("costCenter"),
                F.col("SubscriptionId").alias("subscriptionId"),
                F.col("ResourceGroup").alias("resourceGroup"),
                F.col("ResourceName").alias("resourceName"),
                F.col("ServiceName").alias("serviceName"),
                F.col("ResourceType").alias("resourceType"),
                F.col("TAGS").alias("tags")
            )
        )
    )
)

# --- Before/After CostCenter quality metrics ---

# Before: from df_cleaned
before_total = df_cleaned.count()
before_null = df_cleaned.filter(F.col("CostCenter").isNull()).count()
before_non_null = before_total - before_null

# After: from df_final_reconciled
after_total = df_final_reconciled.count()
after_null = df_final_reconciled.filter(F.col("CostCenter").isNull()).count()
after_non_null = after_total - after_null

metrics_rows = [
    ("Before", before_total, before_non_null, before_null),
    ("After", after_total, after_non_null, after_null),
]

metrics_schema = "Stage STRING, TotalRows LONG, NonNullCostCenter LONG, NullCostCenter LONG"
df_cc_metrics = spark.createDataFrame(metrics_rows, schema=metrics_schema)

print("CostCenter completeness before/after reconciliation:")
display(df_cc_metrics)

# Breakdown by imputation method (after)
print("Row counts by ImputationMethod after reconciliation:")
display(df_final_reconciled.groupBy("ImputationMethod").count())

# Sample of fixed CostCenter details (non-original rows) incl. JSON reason log
print("Sample of records with imputed CostCenter (LLM_* methods):")
df_fixed_sample = (
    df_final_reconciled
    .filter(F.col("ImputationMethod").isin(
        "LLM_Subscription_RG", "LLM_Service_Tags", "LLM_Generic_Fallback"
    ))
    .select(
        "SubscriptionId",
        "ResourceGroup",
        "ResourceName",
        "ServiceName",
        "ResourceType",
        "TAGS",
        "CostCenter",
        "ImputationMethod",
        "CostCenterReasonLog"
    )
    .limit(50)
)
display(df_fixed_sample)

# Write reconciled dataset to Delta table in Lakehouse
output_table = "reconciled_consumption"
# df_final_reconciled.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .saveAsTable(output_table)

print(f"Reconciliation complete! Data successfully saved to Delta table: {output_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
