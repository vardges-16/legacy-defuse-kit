CREATE TABLE [dbo].[fact_backup_job] (
    [job_id]           VARCHAR (8000) NULL,
    [date_key]         DATE           NULL,
    [vm_name]          VARCHAR (8000) NULL,
    [application_code] VARCHAR (8000) NULL,
    [policy_name]      VARCHAR (8000) NULL,
    [status]           VARCHAR (8000) NULL,
    [backup_size_gb]   FLOAT (53)     NULL,
    [error_code]       VARCHAR (8000) NULL,
    [_row_hash]        VARCHAR (8000) NULL
);


GO