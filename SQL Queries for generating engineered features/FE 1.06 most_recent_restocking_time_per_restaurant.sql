CREATE OR REPLACE TABLE `just-data-sandbox-oos.feature_engineering.FE106` AS
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


SELECT * 
FROM duration_since_last_restock
ORDER BY createdTime ASC;



