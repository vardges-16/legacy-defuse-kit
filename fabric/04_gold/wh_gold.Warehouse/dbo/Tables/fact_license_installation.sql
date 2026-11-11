CREATE TABLE [dbo].[fact_license_installation] (
    [installation_id]       BIGINT         NULL,
    [host_name]             VARCHAR (8000) NULL,
    [product_id]            INT            NULL,
    [product_method]        VARCHAR (8000) NULL,
    [cores]                 INT            NULL,
    [license_model]         VARCHAR (8000) NULL,
    [environment]           VARCHAR (8000) NULL,
    [install_date]          DATE           NULL,
    [last_seen_date]        DATE           NULL,
    [host_in_cloud]         BIT            NULL,
    [vm_deleted_still_seen] BIT            NULL,
    [double_licensing]      BIT            NULL,
    [_row_hash]             VARCHAR (8000) NULL
);


GO