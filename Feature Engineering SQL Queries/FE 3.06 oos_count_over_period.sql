CREATE OR REPLACE TABLE `just-data-sandbox-oos.feature_engineering.FE306` AS
WITH oos_data AS (
    SELECT
        unique_id,
        order_item_name,
        createdTime,
        scenario,
        CASE WHEN scenario = 'OUT_OF_STOCK' THEN 1 ELSE 0 END AS is_oos
    FROM `just-data-sandbox-oos.feature_engineering.cleaned_data`
),
time_intervals AS (
    SELECT 
        '1hr' AS period, TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) AS start_time, CURRENT_TIMESTAMP() AS end_time
    UNION ALL 
    SELECT '3hr', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR), CURRENT_TIMESTAMP()
    UNION ALL 
    SELECT '6hr', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR), CURRENT_TIMESTAMP()
    UNION ALL 
    SELECT '24hr', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR), CURRENT_TIMESTAMP()
    UNION ALL 
    SELECT 'startOfDay', TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), CURRENT_TIMESTAMP()
    UNION ALL 
    SELECT '3days', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 DAY), CURRENT_TIMESTAMP()
    UNION ALL 
    SELECT 'week', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY), CURRENT_TIMESTAMP()
    UNION ALL 
    SELECT 'month', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY), CURRENT_TIMESTAMP()
),
aggregated_oos AS (
    SELECT 
        o.unique_id,
        o.order_item_name,
        o.createdTime,
        o.scenario,
        ti.period,

        -- OOS count for each period
        SUM(CASE 
            WHEN d.createdTime BETWEEN ti.start_time AND ti.end_time 
                 AND d.unique_id != o.unique_id  
            THEN d.is_oos 
            ELSE 0 
        END) AS oos_count

    FROM oos_data o
    LEFT JOIN oos_data d 
    ON o.order_item_name = d.order_item_name
    AND d.createdTime <= o.createdTime

    JOIN time_intervals ti
    ON d.createdTime BETWEEN ti.start_time AND ti.end_time

    GROUP BY o.unique_id, o.order_item_name, o.createdTime, o.scenario, ti.period
)
SELECT 
    unique_id,
    order_item_name,
    createdTime,
    scenario,
    MAX(CASE WHEN period = '1hr' THEN oos_count ELSE 0 END) AS oos_count_1hr,
    MAX(CASE WHEN period = '3hr' THEN oos_count ELSE 0 END) AS oos_count_3hr,
    MAX(CASE WHEN period = '6hr' THEN oos_count ELSE 0 END) AS oos_count_6hr,
    MAX(CASE WHEN period = '24hr' THEN oos_count ELSE 0 END) AS oos_count_24hr,
    MAX(CASE WHEN period = 'startOfDay' THEN oos_count ELSE 0 END) AS oos_count_startOfDay,
    MAX(CASE WHEN period = '3days' THEN oos_count ELSE 0 END) AS oos_count_3days,
    MAX(CASE WHEN period = 'week' THEN oos_count ELSE 0 END) AS oos_count_week,
    MAX(CASE WHEN period = 'month' THEN oos_count ELSE 0 END) AS oos_count_month

FROM aggregated_oos
GROUP BY unique_id, order_item_name, createdTime, scenario
ORDER BY createdTime DESC;
