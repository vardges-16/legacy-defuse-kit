-- =====================================================================
-- Legacy Defuse Kit | wh_gold: Gold star schema, rebuilt from lh_silver on every run
-- Run once in the wh_gold SQL editor to create the procedure; the orchestrator then calls EXEC dbo.usp_build_gold.
-- Only tables (no views) so both semantic models can use Direct Lake on OneLake.
-- Amounts keep the Silver column names (billed_cost, effective_cost, budget_cost, storage_cost_usd):
-- nb_reconcile checks Silver amount = Gold amount.
-- =====================================================================
CREATE OR ALTER PROCEDURE dbo.usp_build_gold
AS
BEGIN
    SET NOCOUNT ON;

    -- ---------------------------------------------------------- dimensions
    DROP TABLE IF EXISTS dbo.dim_date;
    CREATE TABLE dbo.dim_date AS
    SELECT CAST(DATEADD(DAY, i, '2024-01-01') AS DATE)                    AS date_key,
           YEAR(DATEADD(DAY, i, '2024-01-01'))                            AS [year],
           MONTH(DATEADD(DAY, i, '2024-01-01'))                           AS month_no,
           CAST(DATEFROMPARTS(YEAR(DATEADD(DAY, i, '2024-01-01')), MONTH(DATEADD(DAY, i, '2024-01-01')), 1) AS DATE) AS month_start,
           CAST(LEFT(CAST(DATENAME(MONTH, DATEADD(DAY, i, '2024-01-01')) AS VARCHAR(20)), 3) + ' ' + CAST(YEAR(DATEADD(DAY, i, '2024-01-01')) AS VARCHAR(4)) AS VARCHAR(8)) AS month_name,
           DATEPART(QUARTER, DATEADD(DAY, i, '2024-01-01'))               AS quarter_no,
           CASE WHEN DATEPART(WEEKDAY, DATEADD(DAY, i, '2024-01-01')) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend
    FROM (SELECT a.v + 10 * b.v + 100 * c.v AS i
          FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) a(v)
          CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) b(v)
          CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(10)) c(v)) n
    WHERE i < 1096;   -- 2024-01-01 .. 2026-12-31

    DROP TABLE IF EXISTS dbo.dim_cost_center;
    CREATE TABLE dbo.dim_cost_center AS
    SELECT cost_center_code, MAX(cost_center_name) AS cost_center_name, MAX(department_code) AS department_code,
           MAX(department_name) AS department_name, MAX(business_code) AS business_code, MAX(business_name) AS business_name,
           MAX(business_unit_code) AS business_unit_code, MAX(business_unit_name) AS business_unit_name,
           CAST(MAX(CAST(cc_has_application AS INT)) AS BIT) AS has_application
    FROM lh_silver.dbo.silver_budget GROUP BY cost_center_code;

    DROP TABLE IF EXISTS dbo.dim_application;
    CREATE TABLE dbo.dim_application AS
    SELECT application_code, application_id, application_name, department, cost_center AS cost_center_code,
           criticality, lifecycle_status, owner_email_hash, owner_email_valid
    FROM lh_silver.dbo.silver_cmdb_applications
    UNION ALL
    SELECT 'UNALLOCATED', NULL, 'Unallocated (no owner)', NULL, NULL, NULL, 'Unknown', NULL, NULL;

    DROP TABLE IF EXISTS dbo.dim_scope;
    CREATE TABLE dbo.dim_scope AS
    SELECT scope_key, MAX(provider) AS provider, MAX(sub_account_name) AS sub_account_name,
           MAX(COALESCE(application_code, 'UNALLOCATED')) AS application_code,
           CAST(MAX(CAST(is_shadow_it AS INT)) AS BIT) AS is_shadow_it, MAX(cmdb_status) AS cmdb_status
    FROM lh_silver.dbo.silver_cost WHERE scope_key IS NOT NULL GROUP BY scope_key;

    DROP TABLE IF EXISTS dbo.dim_service;
    CREATE TABLE dbo.dim_service AS
    SELECT DISTINCT provider, service_name, service_category FROM lh_silver.dbo.silver_cost;

    DROP TABLE IF EXISTS dbo.dim_product;
    CREATE TABLE dbo.dim_product AS
    SELECT product_id, product_name, product_family, edition, license_metric FROM lh_silver.dbo.silver_license_products;

    -- ---------------------------------------------------------- facts
    DROP TABLE IF EXISTS dbo.fact_cost;
    CREATE TABLE dbo.fact_cost AS
    SELECT charge_date AS date_key, billing_period_start, provider, billing_account_name, sub_account_id, sub_account_name,
           scope_key, resource_id, resource_name, resource_type, region_id, service_name, service_category,
           charge_category, charge_frequency, pricing_category, commitment_discount_name,
           COALESCE(application_code, 'UNALLOCATED') AS application_code, cost_center AS cost_center_code, allocation_method,
           billed_cost, effective_cost, list_cost, contracted_cost, consumed_quantity, consumed_unit,
           is_shadow_it, is_zombie_spend, tag_missing, tag_app_conflict, tag_cc_conflict, _row_hash
    FROM lh_silver.dbo.silver_cost;

    DROP TABLE IF EXISTS dbo.fact_budget;
    CREATE TABLE dbo.fact_budget AS
    SELECT budget_month AS month_start, cost_center_code, fiscal_year, budget_cost_usd AS budget_cost, amount_scaled_x1000, _row_hash
    FROM lh_silver.dbo.silver_budget;

    DROP TABLE IF EXISTS dbo.fact_budget_vs_actual;
    CREATE TABLE dbo.fact_budget_vs_actual AS
    SELECT COALESCE(b.month_start, a.month_start) AS month_start, COALESCE(b.cost_center_code, a.cost_center_code) AS cost_center_code,
           b.budget_cost, a.actual_cost, a.actual_cost - b.budget_cost AS variance
    FROM (SELECT budget_month AS month_start, cost_center_code, SUM(budget_cost_usd) AS budget_cost
          FROM lh_silver.dbo.silver_budget GROUP BY budget_month, cost_center_code) b
    FULL OUTER JOIN (
        SELECT DATEFROMPARTS(YEAR(charge_date), MONTH(charge_date), 1) AS month_start, cost_center AS cost_center_code,
               SUM(effective_cost) AS actual_cost
        FROM lh_silver.dbo.silver_cost WHERE cost_center IS NOT NULL
        GROUP BY DATEFROMPARTS(YEAR(charge_date), MONTH(charge_date), 1), cost_center) a
      ON a.month_start = b.month_start AND a.cost_center_code = b.cost_center_code;

    DROP TABLE IF EXISTS dbo.fact_license_installation;
    CREATE TABLE dbo.fact_license_installation AS
    SELECT installation_id, host_name, product_id, product_method, cores, license_model, environment, install_date, last_seen_date,
           host_in_cloud, vm_deleted_still_seen, double_licensing, _row_hash
    FROM lh_silver.dbo.silver_license_installations;

    DROP TABLE IF EXISTS dbo.fact_license_position;
    CREATE TABLE dbo.fact_license_position AS
    SELECT COALESCE(i.product_id, e.product_id) AS product_id, COALESCE(i.byol_cores_installed, 0) AS byol_cores_installed,
           COALESCE(e.cores_entitled_active, 0) AS cores_entitled_active, COALESCE(e.cores_expired, 0) AS cores_expired,
           COALESCE(i.byol_cores_installed, 0) - COALESCE(e.cores_entitled_active, 0) AS core_shortfall,
           e.annual_entitlement_cost
    FROM (SELECT product_id, SUM(cores) AS byol_cores_installed, COUNT(*) AS byol_installations
                  FROM lh_silver.dbo.silver_license_installations
                  WHERE license_model = 'BYOL' AND double_licensing = 0 AND host_in_cloud = 1 AND product_id IS NOT NULL
                  GROUP BY product_id) i
    FULL OUTER JOIN (SELECT product_id, SUM(CASE WHEN is_expired = 0 THEN quantity_cores ELSE 0 END) AS cores_entitled_active,
                        SUM(CASE WHEN is_expired = 1 THEN quantity_cores ELSE 0 END) AS cores_expired,
                        SUM(quantity_cores * unit_cost_usd) AS annual_entitlement_cost
                 FROM lh_silver.dbo.silver_license_entitlements GROUP BY product_id) e
      ON e.product_id = i.product_id;

    DROP TABLE IF EXISTS dbo.fact_backup_cost;
    CREATE TABLE dbo.fact_backup_cost AS
    SELECT month AS month_start, vm_name, resource_group, COALESCE(application_code, 'UNALLOCATED') AS application_code,
           cost_center AS cost_center_code, vm_in_cmdb, protection_state, retained_storage_gb, storage_cost_usd, is_orphaned, _row_hash
    FROM lh_silver.dbo.silver_backup_storage;

    DROP TABLE IF EXISTS dbo.fact_backup_job;
    CREATE TABLE dbo.fact_backup_job AS
    SELECT job_id, job_date AS date_key, vm_name, COALESCE(application_code, 'UNALLOCATED') AS application_code, policy_name,
           status, backup_size_gb, error_code, _row_hash
    FROM lh_silver.dbo.silver_backup_jobs;

    -- one row per Azure VM in the bill: is it protected, and when did it last back up successfully?
    DROP TABLE IF EXISTS dbo.fact_vm_backup_coverage;
    CREATE TABLE dbo.fact_vm_backup_coverage AS
    SELECT vm.vm_name, vm.resource_group, vm.application_code, vm.last_cost_date,
           CASE WHEN vm.resource_group LIKE '%-prod-%' OR vm.resource_group LIKE '%-shared-%' THEN 'prod' ELSE 'non-prod' END AS env_class,
           CAST(CASE WHEN jb.vm_name IS NULL THEN 0 ELSE 1 END AS BIT) AS has_backup_policy,
           jb.last_success, COALESCE(jb.jobs, 0) AS jobs, COALESCE(jb.failed_jobs, 0) AS failed_jobs,
           CAST(CASE WHEN vm.last_cost_date >= '2026-10-30' AND (vm.resource_group LIKE '%-prod-%' OR vm.resource_group LIKE '%-shared-%')
                      AND (jb.last_success IS NULL OR jb.last_success < '2026-10-25') THEN 1 ELSE 0 END AS BIT) AS is_unprotected_prod
    FROM (SELECT LOWER(resource_name) AS vm_name, MAX(scope_key) AS resource_group,
                       MAX(COALESCE(application_code, 'UNALLOCATED')) AS application_code, MAX(charge_date) AS last_cost_date
                FROM lh_silver.dbo.silver_cost WHERE service_name = 'Virtual Machines' GROUP BY LOWER(resource_name)) vm
    LEFT JOIN (SELECT vm_name, MAX(CASE WHEN status IN ('Completed', 'CompletedWithWarnings') THEN job_start END) AS last_success,
                       COUNT(*) AS jobs, SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END) AS failed_jobs
                FROM lh_silver.dbo.silver_backup_jobs GROUP BY vm_name) jb
      ON jb.vm_name = vm.vm_name;
END;