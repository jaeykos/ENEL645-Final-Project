CREATE OR REPLACE TABLE `just-data-sandbox-oos.feature_engineering.FE309` AS
WITH restocking_events AS (
    SELECT
        unique_id,
        order_item_name,
        createdTime,
        scenario,
        LAG(scenario) OVER (
            PARTITION BY order_item_name 
            ORDER BY createdTime
        ) AS prev_scenario
    FROM `just-data-sandbox-oos.feature_engineering.cleaned_data`
),
propagate_restock_time AS (
    SELECT
        t.unique_id, 
        t.order_item_name,
        t.createdTime,
        t.scenario,
        -- Capture the restocking event (DELIVERED after OUT_OF_STOCK)
        CASE 
            WHEN t.prev_scenario = 'OUT_OF_STOCK' AND t.scenario = 'DELIVERED' 
            THEN t.createdTime 
        END AS restocked_marker
    FROM restocking_events t
),
final_restocking AS (
    SELECT
        unique_id,
        order_item_name,
        createdTime,
        scenario,
        -- Propagate the last non-null restocked_time downward
        LAST_VALUE(restocked_marker IGNORE NULLS) OVER (
            PARTITION BY order_item_name 
            ORDER BY createdTime 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS restocked_time
    FROM propagate_restock_time
)
SELECT 
    unique_id,
    order_item_name,
    createdTime,
    scenario,
    restocked_time,
    -- Compute minutes difference between createdTime and restocked_time
    TIMESTAMP_DIFF(createdTime, restocked_time, MINUTE) AS minutes_since_restocked,
    -- Split restocked_time into separate components for ML
    EXTRACT(YEAR FROM restocked_time) AS restocked_year,
    EXTRACT(MONTH FROM restocked_time) AS restocked_month,
    EXTRACT(DAY FROM restocked_time) AS restocked_day,
    EXTRACT(HOUR FROM restocked_time) AS restocked_hour,
    EXTRACT(MINUTE FROM restocked_time) AS restocked_minute
FROM final_restocking
ORDER BY createdTime DESC;
