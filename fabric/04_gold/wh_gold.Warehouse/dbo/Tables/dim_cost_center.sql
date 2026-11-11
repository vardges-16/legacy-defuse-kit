CREATE TABLE [dbo].[dim_cost_center] (
    [cost_center_code]   VARCHAR (8000) NULL,
    [cost_center_name]   VARCHAR (8000) NULL,
    [department_code]    VARCHAR (8000) NULL,
    [department_name]    VARCHAR (8000) NULL,
    [business_code]      VARCHAR (8000) NULL,
    [business_name]      VARCHAR (8000) NULL,
    [business_unit_code] VARCHAR (8000) NULL,
    [business_unit_name] VARCHAR (8000) NULL,
    [has_application]    BIT            NULL
);


GO