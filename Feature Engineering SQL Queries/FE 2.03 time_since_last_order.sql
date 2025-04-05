CREATE OR REPLACE TABLE `just-data-sandbox-oos.ENEL_645.FE203` AS
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
        restaurant_id,
        LAG(createdTime) OVER (
            PARTITION BY restaurant_id, order_item_name 
            ORDER BY createdTime
        ) AS prev_order_time
    FROM `just-data-sandbox-oos.ENEL_645.cleaned_data`
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
    restaurant_id,
    IFNULL(TIMESTAMP_DIFF(createdTime, prev_order_time, HOUR), NULL) AS hour_since_last_order
FROM ordered_data;
