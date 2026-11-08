# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {}
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession

# -------------------------------
# Constants & Session Parameters
# -------------------------------
ADLS_TOKEN_PROVIDER = "com.microsoft.azure.synapse.tokenlibrary.LinkedServiceBasedTokenProvider"

# -------------------------------
# Spark Session Creation
# -------------------------------
spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
sc = spark.sparkContext

# -------------------------------
# Source & Destination Paths
# -------------------------------
storage_account = "saodlepam2446996datalake"
source_container = "landing-zone-azure"
source_path = "consumption-run-02/202401/consumption.parquet"

# -------------------------------
# Spark Session Configuration
# -------------------------------
spark.conf.set("spark.sql.caseSensitive", "true")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
spark.conf.set("spark.sql.mapKeyDedupPolicy", "LAST_WIN")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import synapse.ml.spark.aifunc as aifunc
aifunc.default_conf.set_deployment_name("gpt-5-mini")
aifunc.default_conf.set_reasoning_effort("low")
aifunc.default_conf.reset_temperature()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

# -------------------------------
# Read source parquet from ADLS Gen2
# -------------------------------
# TODO: Set your storage account name used by the linked service

source_uri = f"abfss://{source_container}@{storage_account}.dfs.core.windows.net/{source_path}"

df = spark.read.parquet(source_uri)

# Optional: inspect a few distinct billing account names
billing_df = df.select("BillingAccountName").distinct()
display(billing_df.limit(50))

# -------------------------------
# Use AI model to suggest better billing account names
# -------------------------------
# This uses PySpark AI Functions (configured in the previous cell) to generate
# standardized / cleaner names for each distinct BillingAccountName.

billing_with_suggestions = (
    billing_df
    .withColumnRenamed("BillingAccountName", "original_name")
    .ai.generate_response(
        prompt=(
            "You are standardizing billing account names for analytics reports. "
            "Given the value in {original_name}, suggest a clearer, concise, "
            "human-readable billing account name. Return only the suggested "
            "name, with no explanations."
        ),
        is_prompt_template=True,
        output_col="suggested_name",
    )
)



display(billing_with_suggestions)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lower, sum

# ---------------------------------------------
# Analyze missing Cost Center tags by Resource Group
# ---------------------------------------------
# Assumptions:
# - The column holding tags is named "Tags" and is a string (e.g., "key1=value1;key2=value2")
# - The resource group column is named "ResourceGroup" (rename below if different)

RESOURCE_GROUP_COL = "x_ResourceGroupName"  # change if your column name is different

# Filter rows where Tags is null/empty or does not contain a Cost Center tag (case-insensitive)
missing_cc_df = df.filter(
    (col("Tags").isNull())
    | (col("Tags") == "")
    | (~lower(col("Tags")).contains("CostCenter"))
)

# Summarize counts by resource group for records missing Cost Center tags
missing_cc_by_rg = (
    missing_cc_df
    .groupBy(RESOURCE_GROUP_COL)
    .agg(
        sum("BilledCost").alias("BilledCostSum")
    )
    .orderBy(col("BilledCostSum").desc())

)

display(missing_cc_by_rg)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lower, regexp_extract, when

# ----------------------------------------------------------
# Infer CostCenter for RGs missing it by using Dev/Prod peers
# ----------------------------------------------------------
# Assumptions:
# - Tags is a semi-structured string containing a CostCenter key, e.g.:
#     "CostCenter=12345;Owner=abc" or "costcenter: 12345"
# - Resource group name column is x_ResourceGroupName
# - Dev/Prod are indicated in the RG name by substrings like "-dev", "_dev", "-prod", "_prod"

RG_COL = "x_ResourceGroupName"

# 1) Extract CostCenter from Tags for all rows
costcenter_extracted = regexp_extract(col("Tags"), r"(?i)costcenter\s*[:=]\s*([^;|,]+)", 1)

df_with_cc = df.withColumn(
    "CostCenter",
    when(costcenter_extracted == "", None).otherwise(costcenter_extracted)
)

# 2) Derive environment and base RG name
rg_lower = lower(col(RG_COL))

df_with_env = (
    df_with_cc
    .withColumn(
        "Environment",
        when(rg_lower.contains("dev"), "Dev")
        .when(rg_lower.contains("prod"), "Prod")
        .otherwise("Other")
    )
    # Base name: strip common dev/prod suffix markers; adjust if you use different patterns
    .withColumn(
        "BaseRG",
        regexp_extract(rg_lower, r"^(.+?)([-_](dev|prod))?$", 1)
    )
)

# 3) Known CostCenter per (BaseRG, Environment)
known_cc = (
    df_with_env
    .filter(col("CostCenter").isNotNull())
    .select("BaseRG", "Environment", RG_COL, "CostCenter")
    .dropDuplicates(["BaseRG", "Environment", RG_COL, "CostCenter"])
)

# 4) RGs missing CostCenter
missing_cc_rg = (
    df_with_env
    .filter(col("CostCenter").isNull())
    .select("BaseRG", "Environment", RG_COL)
    .dropDuplicates(["BaseRG", "Environment", RG_COL])
)

# 5) For each missing RG, find counterpart Dev/Prod environment for same BaseRG
#    Example: x-myapp-dev (missing) can borrow from x-myapp-prod (known), and vice versa.

counterpart_suggestions = (
    missing_cc_rg.alias("m")
    .join(
        known_cc.alias("k"),
        on=(col("m.BaseRG") == col("k.BaseRG")) & (col("m.Environment") != col("k.Environment")),
        how="inner",
    )
    .select(
        col(f"m.{RG_COL}").alias("MissingResourceGroup"),
        col("m.Environment").alias("MissingEnv"),
        col(f"k.{RG_COL}").alias("CounterpartResourceGroup"),
        col("k.Environment").alias("CounterpartEnv"),
        col("k.CostCenter").alias("SuggestedCostCenter"),
    )
    .dropDuplicates()
)

display(counterpart_suggestions)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
