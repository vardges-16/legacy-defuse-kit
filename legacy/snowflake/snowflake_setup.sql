-- =====================================================================
-- Legacy Defuse Kit | Snowflake complete setup (FOCUS cost data, USD)
-- Database ZONTIQUE_BILLING, schema RAW
-- Run in a Snowsight SQL worksheet as ACCOUNTADMIN.
--   PART A: run before uploading the file (Run All on part A)
--   PART B: run after uploading consumption.parquet to @STG_COST/focus/
-- Safe to re-run.
-- =====================================================================


-- =====================================================================
-- PART A: setup
-- =====================================================================
USE ROLE ACCOUNTADMIN;

-- A1. Warehouse (exists already; this keeps it small and sleeping when idle)
CREATE WAREHOUSE IF NOT EXISTS LDK_WH
  WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;
USE WAREHOUSE LDK_WH;

-- A2. Spending guard for the trial: warn at 80%, suspend at 100% of 50 credits
CREATE RESOURCE MONITOR IF NOT EXISTS LDK_MONITOR WITH CREDIT_QUOTA = 50
  TRIGGERS ON 80 PERCENT DO NOTIFY
           ON 100 PERCENT DO SUSPEND;
ALTER WAREHOUSE LDK_WH SET RESOURCE_MONITOR = LDK_MONITOR;

-- A3. Database and schema
CREATE DATABASE IF NOT EXISTS ZONTIQUE_BILLING;
CREATE SCHEMA IF NOT EXISTS ZONTIQUE_BILLING.RAW;
USE SCHEMA ZONTIQUE_BILLING.RAW;

-- A4. File format and stage for the Parquet files
CREATE FILE FORMAT IF NOT EXISTS FF_PARQUET TYPE = PARQUET;
CREATE STAGE IF NOT EXISTS STG_COST
  FILE_FORMAT = FF_PARQUET
  COMMENT = 'FOCUS cost exports (folder focus/)';

-- A5. Role for the Fabric mirroring user (user created earlier; grants are idempotent)
CREATE ROLE IF NOT EXISTS FABRIC_MIRROR_ROLE;
GRANT USAGE ON WAREHOUSE LDK_WH TO ROLE FABRIC_MIRROR_ROLE;
GRANT USAGE ON DATABASE ZONTIQUE_BILLING TO ROLE FABRIC_MIRROR_ROLE;
GRANT USAGE, CREATE STREAM ON SCHEMA ZONTIQUE_BILLING.RAW TO ROLE FABRIC_MIRROR_ROLE;
GRANT SELECT ON ALL TABLES    IN SCHEMA ZONTIQUE_BILLING.RAW TO ROLE FABRIC_MIRROR_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ZONTIQUE_BILLING.RAW TO ROLE FABRIC_MIRROR_ROLE;

-- A6. Check: the stage exists (empty until you upload)
SHOW STAGES IN SCHEMA ZONTIQUE_BILLING.RAW;


-- =====================================================================
-- PART B: create the table from the file and load it
-- (upload consumption.parquet to @STG_COST/focus/ first)
-- =====================================================================
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE LDK_WH;
USE SCHEMA ZONTIQUE_BILLING.RAW;

-- B1. The file is in the stage?
LIST @STG_COST/focus/;

-- B2. Table with the file's own 73 columns (order kept, names upper-cased)
CREATE TABLE IF NOT EXISTS FOCUS_COST
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) WITHIN GROUP (ORDER BY ORDER_ID)
    FROM TABLE(
      INFER_SCHEMA(
        LOCATION    => '@STG_COST/focus/',
        FILE_FORMAT => 'FF_PARQUET',
        IGNORE_CASE => TRUE
      )
    )
  );

-- Mirroring needs change tracking
ALTER TABLE FOCUS_COST SET CHANGE_TRACKING = TRUE;

-- B3. Load (COPY skips files it has already loaded, so re-running is safe)
COPY INTO FOCUS_COST
  FROM @STG_COST/focus/
  FILE_FORMAT = (FORMAT_NAME = 'FF_PARQUET')
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- B4. Legacy view (for the Discover stage and the before/after demo):
--     tag-only attribution on billed cost - misses untagged cost and amortization
CREATE OR REPLACE VIEW V_COST_BY_APP AS
SELECT
  TRY_PARSE_JSON(TAGS::VARCHAR):SolutionId::STRING  AS SOLUTION_ID,
  TO_CHAR(CHARGEPERIODSTART, 'YYYY-MM')             AS USAGE_MONTH,
  SUM(BILLEDCOST)                                    AS TOTAL_COST_USD
FROM FOCUS_COST
WHERE TRY_PARSE_JSON(TAGS::VARCHAR):SolutionId IS NOT NULL
GROUP BY 1, 2;

-- Fabric role can read the new table and view
GRANT SELECT ON ALL TABLES IN SCHEMA ZONTIQUE_BILLING.RAW TO ROLE FABRIC_MIRROR_ROLE;
GRANT SELECT ON ALL VIEWS  IN SCHEMA ZONTIQUE_BILLING.RAW TO ROLE FABRIC_MIRROR_ROLE;

-- B5. Checks. Expected for the June 2024 sample:
--     45,051 rows | BilledCost 207,755.81 | EffectiveCost 207,755.81 | 2024-06-01 to 2024-06-30
SELECT COUNT(*)               AS ROWS_,
       SUM(BILLEDCOST)        AS BILLED_USD,
       SUM(EFFECTIVECOST)     AS EFFECTIVE_USD,
       MIN(CHARGEPERIODSTART) AS FIRST_DAY,
       MAX(CHARGEPERIODSTART) AS LAST_DAY
FROM FOCUS_COST;

--     Legacy view total: lower than above (untagged cost missing) - expected
SELECT SUM(TOTAL_COST_USD) AS TAGGED_ONLY_USD FROM V_COST_BY_APP;


-- =====================================================================
-- PART C (later, optional): remove the old test objects
-- First untick RAW.COST_LINE_ITEMS in the Fabric mirror, then run:
-- =====================================================================
-- DROP TABLE IF EXISTS ZONTIQUE_BILLING.RAW.COST_LINE_ITEMS;
-- DROP TABLE IF EXISTS ZONTIQUE_BILLING.RAW.EXCHANGE_RATES;

-- =====================================================================
-- PART D: read-only user for the legacy Power BI report (V_COST_BY_APP only)
-- Password lives in Key Vault kv-ldk-2446996, secret snowflake-legacy-pbi-pwd.
-- =====================================================================
CREATE ROLE IF NOT EXISTS LEGACY_REPORT_ROLE;
GRANT USAGE ON WAREHOUSE LDK_WH TO ROLE LEGACY_REPORT_ROLE;
GRANT USAGE ON DATABASE ZONTIQUE_BILLING TO ROLE LEGACY_REPORT_ROLE;
GRANT USAGE ON SCHEMA ZONTIQUE_BILLING.RAW TO ROLE LEGACY_REPORT_ROLE;
GRANT SELECT ON VIEW ZONTIQUE_BILLING.RAW.V_COST_BY_APP TO ROLE LEGACY_REPORT_ROLE;
CREATE USER IF NOT EXISTS LEGACY_PBI_USER
  PASSWORD = '<from Key Vault>'
  TYPE = LEGACY_SERVICE
  DEFAULT_ROLE = LEGACY_REPORT_ROLE
  DEFAULT_WAREHOUSE = LDK_WH;
GRANT ROLE LEGACY_REPORT_ROLE TO USER LEGACY_PBI_USER;

-- =====================================================================
-- NEW MONTHS: upload the next file to @STG_COST/focus/ and run B3 + B5 again.
-- =====================================================================
