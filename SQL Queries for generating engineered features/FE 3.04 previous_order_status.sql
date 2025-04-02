CREATE OR REPLACE TABLE `just-data-sandbox-oos.feature_engineering.FE304` AS
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
        LAG(scenario) OVER (
            PARTITION BY order_item_name 
            ORDER BY createdTime
        ) AS last_order_status
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
    last_order_status
FROM ordered_data;
