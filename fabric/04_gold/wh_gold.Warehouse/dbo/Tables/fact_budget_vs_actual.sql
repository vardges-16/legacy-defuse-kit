CREATE TABLE [dbo].[fact_budget_vs_actual] (
    [month_start]      DATE           NULL,
    [cost_center_code] VARCHAR (8000) NULL,
    [budget_cost]      FLOAT (53)     NULL,
    [actual_cost]      FLOAT (53)     NULL,
    [variance]         FLOAT (53)     NULL
);


GO