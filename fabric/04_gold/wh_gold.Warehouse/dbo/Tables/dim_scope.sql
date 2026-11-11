CREATE TABLE [dbo].[dim_scope] (
    [scope_key]        VARCHAR (8000) NULL,
    [provider]         VARCHAR (8000) NULL,
    [sub_account_name] VARCHAR (8000) NULL,
    [application_code] VARCHAR (8000) NULL,
    [is_shadow_it]     BIT            NULL,
    [cmdb_status]      VARCHAR (8000) NULL
);


GO