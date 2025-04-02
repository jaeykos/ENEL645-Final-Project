CREATE OR REPLACE TABLE `just-data-sandbox-oos.feature_engineering.FE107-110` AS
WITH restocking_events AS (
    SELECT
        unique_id,
        restaurant_id,
        createdTime,
        scenario,
        LAG(scenario) OVER (
            PARTITION BY restaurant_id
            ORDER BY createdTime
        ) AS prev_scenario
    FROM `just-data-sandbox-oos.feature_engineering.cleaned_data`
),
propagate_restock_time AS (
    SELECT 
        t.unique_id,
        t.restaurant_id,
        t.createdTime,
        t.scenario,
        -- Propagate the last restocked time without keeping the marker
        LAST_VALUE(
            CASE 
                WHEN t.prev_scenario = 'OUT_OF_STOCK' AND t.scenario = 'DELIVERED' 
                THEN t.createdTime 
            END 
            IGNORE NULLS
        ) OVER (
            PARTITION BY t.restaurant_id
            ORDER BY t.createdTime ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS restocked_time
    FROM restocking_events t
),
duration_since_last_restock AS (
  SELECT
    t2.unique_id,
    t2.restaurant_id,
    t2.createdTime,
    t2.scenario,
    TIMESTAMP_DIFF(t2.createdTime, t2.restocked_time, MINUTE) as duration_since_last_restock_minutes_per_restaurant
  FROM propagate_restock_time t2
)

SELECT 
    t3.unique_id,
    t3.restaurant_id,
    t3.createdTime,
    t3.scenario,
    (SELECT AVG(t4.duration_since_last_restock_minutes_per_restaurant) 
        FROM duration_since_last_restock t4
        WHERE t4.restaurant_id = t3.restaurant_id
            AND t4.createdTime BETWEEN TIMESTAMP_SUB(t3.createdTime, INTERVAL 7 DAY) 
            AND t3.createdTime
    ) AS avg_restock_time_in_minutes_per_week_per_restaurant,

    (SELECT AVG(t4.duration_since_last_restock_minutes_per_restaurant) 
        FROM duration_since_last_restock t4
        WHERE t4.restaurant_id = t3.restaurant_id
            AND t4.createdTime BETWEEN TIMESTAMP_SUB(t3.createdTime, INTERVAL 30 DAY) 
            AND t3.createdTime
    ) AS avg_restock_time_in_minutes_per_month_per_restaurant,

    (SELECT MAX(t4.duration_since_last_restock_minutes_per_restaurant) 
        FROM duration_since_last_restock t4
        WHERE t4.restaurant_id = t3.restaurant_id
            AND t4.createdTime BETWEEN TIMESTAMP_SUB(t3.createdTime, INTERVAL 7 DAY) 
            AND t3.createdTime
    ) AS max_restock_time_in_minutes_per_week_per_restaurant,

    (SELECT MAX(t4.duration_since_last_restock_minutes_per_restaurant) 
        FROM duration_since_last_restock t4
        WHERE t4.restaurant_id = t3.restaurant_id
            AND t4.createdTime BETWEEN TIMESTAMP_SUB(t3.createdTime, INTERVAL 30 DAY) 
            AND t3.createdTime
    ) AS max_restock_time_in_minutes_per_month_per_restaurant
    
FROM duration_since_last_restock t3
ORDER BY createdTime ASC;
