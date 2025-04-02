CREATE OR REPLACE TABLE `just-data-sandbox-oos.feature_engineering.FE212` AS
WITH profit_data AS (
    SELECT
        unique_id,
        restaurant_id,
        order_item_name,
        createdTime,
        scenario,
        CASE 
            WHEN scenario = 'DELIVERED' THEN 0.3 * total 
            ELSE NULL 
        END AS profit -- Only compute profit for DELIVERED
    FROM `just-data-sandbox-oos.feature_engineering.cleaned_data`
),
aggregated_profit AS (
    SELECT 
        p.unique_id,
        p.restaurant_id,
        p.order_item_name,
        p.createdTime,
        p.scenario,

        -- Avg daily profit over the last 3 days
        SUM(CASE 
            WHEN d.createdTime BETWEEN TIMESTAMP_SUB(p.createdTime, INTERVAL 3 DAY) AND p.createdTime 
            THEN d.profit 
            ELSE 0 
        END) / 3 AS avg_daily_profit_3days,

        -- Avg daily profit over the last week (7 days)
        SUM(CASE 
            WHEN d.createdTime BETWEEN TIMESTAMP_SUB(p.createdTime, INTERVAL 7 DAY) AND p.createdTime 
            THEN d.profit 
            ELSE 0 
        END) / 7 AS avg_daily_profit_week,

        -- Avg daily profit over the last month (30 days)
        SUM(CASE 
            WHEN d.createdTime BETWEEN TIMESTAMP_SUB(p.createdTime, INTERVAL 30 DAY) AND p.createdTime 
            THEN d.profit 
            ELSE 0 
        END) / 30 AS avg_daily_profit_month

    FROM profit_data p
    LEFT JOIN profit_data d 
    ON p.restaurant_id = d.restaurant_id 
    AND p.order_item_name = d.order_item_name
    AND d.createdTime < p.createdTime

    GROUP BY p.unique_id, p.restaurant_id, p.order_item_name, p.createdTime, p.scenario
)
SELECT * FROM aggregated_profit
ORDER BY createdTime DESC;
