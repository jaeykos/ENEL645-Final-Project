CREATE OR REPLACE TABLE just-data-sandbox-oos.feature_engineering.FE305 AS 
WITH last_oos AS (
    SELECT 
        unique_id,
        order_item_name,
        createdTime,
        scenario,
        -- Get the most recent OUT_OF_STOCK time for each item
        LAST_VALUE(
            CASE WHEN scenario = 'OUT_OF_STOCK' THEN createdTime END 
            IGNORE NULLS
        ) OVER (
            PARTITION BY order_item_name 
            ORDER BY createdTime 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS last_oos_time
    FROM just-data-sandbox-oos.feature_engineering.cleaned_data
)
SELECT
    unique_id, 
    order_item_name,
    createdTime,
    scenario,
    TIMESTAMP_DIFF(createdTime, last_oos_time, MINUTE) AS minutes_since_last_out_of_stock

FROM last_oos;