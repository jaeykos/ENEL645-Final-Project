CREATE OR REPLACE TABLE `just-data-sandbox-oos.ENEL_645.FE204` AS
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
        LAG(scenario) OVER (
            PARTITION BY restaurant_id, order_item_name 
            ORDER BY createdTime
        ) AS last_order_status
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
    last_order_status
FROM ordered_data;
