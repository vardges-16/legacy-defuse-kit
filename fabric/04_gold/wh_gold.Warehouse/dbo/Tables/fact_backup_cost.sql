CREATE TABLE [dbo].[fact_backup_cost] (
    [month_start]         DATE           NULL,
    [vm_name]             VARCHAR (8000) NULL,
    [resource_group]      VARCHAR (8000) NULL,
    [application_code]    VARCHAR (8000) NULL,
    [cost_center_code]    VARCHAR (8000) NULL,
    [vm_in_cmdb]          BIT            NULL,
    [protection_state]    VARCHAR (8000) NULL,
    [retained_storage_gb] FLOAT (53)     NULL,
    [storage_cost_usd]    FLOAT (53)     NULL,
    [is_orphaned]         BIT            NULL,
    [_row_hash]           VARCHAR (8000) NULL
);


GO