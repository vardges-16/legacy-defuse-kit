CREATE TABLE [dbo].[fact_budget] (
    [month_start]         DATE           NULL,
    [cost_center_code]    VARCHAR (8000) NULL,
    [fiscal_year]         INT            NULL,
    [budget_cost]         FLOAT (53)     NULL,
    [amount_scaled_x1000] BIT            NULL,
    [_row_hash]           VARCHAR (8000) NULL
);


GO