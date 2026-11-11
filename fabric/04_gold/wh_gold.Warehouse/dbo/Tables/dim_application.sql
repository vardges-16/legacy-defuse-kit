CREATE TABLE [dbo].[dim_application] (
    [application_code]  VARCHAR (8000) NULL,
    [application_id]    VARCHAR (8000) NULL,
    [application_name]  VARCHAR (8000) NULL,
    [department]        VARCHAR (8000) NULL,
    [cost_center_code]  VARCHAR (8000) NULL,
    [criticality]       VARCHAR (8000) NULL,
    [lifecycle_status]  VARCHAR (8000) NULL,
    [owner_email_hash]  VARCHAR (8000) NULL,
    [owner_email_valid] BIT            NULL
);


GO