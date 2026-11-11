CREATE TABLE [dbo].[fact_vm_backup_coverage] (
    [vm_name]             VARCHAR (8000) NULL,
    [resource_group]      VARCHAR (8000) NULL,
    [application_code]    VARCHAR (8000) NULL,
    [last_cost_date]      DATE           NULL,
    [env_class]           VARCHAR (8)    NOT NULL,
    [has_backup_policy]   BIT            NULL,
    [last_success]        DATETIME2 (6)  NULL,
    [jobs]                INT            NULL,
    [failed_jobs]         INT            NULL,
    [is_unprotected_prod] BIT            NULL
);


GO