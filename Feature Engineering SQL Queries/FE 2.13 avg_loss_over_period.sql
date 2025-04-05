CREATE OR REPLACE TABLE `just-data-sandbox-oos.feature_engineering.FE213` AS
WITH loss_data AS (
    SELECT
        unique_id,
        restaurant_id,
        order_item_name,
        createdTime,
        scenario,
        CASE 
            WHEN scenario = 'OUT_OF_STOCK' THEN 0.3 * total 
            ELSE NULL 
        END AS loss -- Only compute loss for OUT_OF_STOCK
    FROM `just-data-sandbox-oos.feature_engineering.cleaned_data`
),
aggregated_loss AS (
    SELECT 
        l.unique_id,
        l.restaurant_id,
        l.order_item_name,
        l.createdTime,
        l.scenario,

        -- Avg daily loss over the last 3 days
        SUM(CASE 
            WHEN d.createdTime BETWEEN TIMESTAMP_SUB(l.createdTime, INTERVAL 3 DAY) AND l.createdTime 
            THEN d.loss 
            ELSE 0 
        END) / 3 AS avg_daily_loss_3days,

        -- Avg daily loss over the last week (7 days)
        SUM(CASE 
            WHEN d.createdTime BETWEEN TIMESTAMP_SUB(l.createdTime, INTERVAL 7 DAY) AND l.createdTime 
            THEN d.loss 
            ELSE 0 
        END) / 7 AS avg_daily_loss_week,

        -- Avg daily loss over the last month (30 days)
        SUM(CASE 
            WHEN d.createdTime BETWEEN TIMESTAMP_SUB(l.createdTime, INTERVAL 30 DAY) AND l.createdTime 
            THEN d.loss 
            ELSE 0 
        END) / 30 AS avg_daily_loss_month

    FROM loss_data l
    LEFT JOIN loss_data d 
    ON l.restaurant_id = d.restaurant_id 
    AND l.order_item_name = d.order_item_name
    AND d.createdTime <
     l.createdTime

    GROUP BY l.unique_id, l.restaurant_id, l.order_item_name, l.createdTime, l.scenario
)
SELECT * FROM aggregated_loss
ORDER BY createdTime DESC;
