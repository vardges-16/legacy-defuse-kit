CREATE TABLE [dbo].[fact_license_position] (
    [product_id]              INT        NULL,
    [byol_cores_installed]    INT        NULL,
    [cores_entitled_active]   INT        NULL,
    [cores_expired]           INT        NULL,
    [core_shortfall]          INT        NULL,
    [annual_entitlement_cost] FLOAT (53) NULL
);


GO