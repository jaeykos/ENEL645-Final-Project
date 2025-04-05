CREATE OR REPLACE TABLE `just-data-sandbox-oos.feature_engineering.FE303` AS
WITH ordered_data AS (
    SELECT
        unique_id,
        createdTime,
        order_item_name,
        order_item_quantity,
        order_item_price_each,
        order_item_price_sum,
        total,
        subtotal,
        scenario,
        LAG(createdTime) OVER (
            PARTITION BY order_item_name 
            ORDER BY createdTime
        ) AS prev_order_time
    FROM `just-data-sandbox-oos.feature_engineering.cleaned_data`
)
SELECT
    unique_id,
    createdTime,
    order_item_name,
    order_item_quantity,
    order_item_price_each,
    order_item_price_sum,
    total,
    subtotal,
    scenario,
    IFNULL(TIMESTAMP_DIFF(createdTime, prev_order_time, MINUTE), NULL) AS minutes_since_last_order
FROM ordered_data;
