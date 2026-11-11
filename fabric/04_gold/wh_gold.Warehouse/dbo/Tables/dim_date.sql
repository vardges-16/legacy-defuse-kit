CREATE TABLE [dbo].[dim_date] (
    [date_key]    DATE        NULL,
    [year]        INT         NULL,
    [month_no]    INT         NULL,
    [month_start] DATE        NULL,
    [month_name]  VARCHAR (8) NULL,
    [quarter_no]  INT         NULL,
    [is_weekend]  INT         NOT NULL
);


GO