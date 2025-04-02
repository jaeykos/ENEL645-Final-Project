CREATE OR REPLACE TABLE `just-data-sandbox-oos.ENEL_645.FE206` AS
WITH oos_data AS (
    SELECT
        unique_id,
        restaurant_id,
        order_item_name,
        createdTime,
        scenario,
        CASE WHEN scenario = 'OUT_OF_STOCK' THEN 1 ELSE 0 END AS is_oos
    FROM `just-data-sandbox-oos.ENEL_645.cleaned_data`
),
aggregated_oos AS (
    SELECT 
        o.unique_id,
        o.restaurant_id,
        o.order_item_name,
        o.createdTime,
        o.scenario,

        -- OOS count in last 1 hour
        SUM(CASE 
            WHEN d.createdTime BETWEEN TIMESTAMP_SUB(o.createdTime, INTERVAL 1 HOUR) AND  TIMESTAMP_SUB(o.createdTime, INTERVAL 1 SECOND) 
            THEN d.is_oos 
            ELSE 0 
        END) AS oos_count_1hr,

        -- OOS count in last 3 hours
        SUM(CASE 
            WHEN d.createdTime BETWEEN TIMESTAMP_SUB(o.createdTime, INTERVAL 3 HOUR) AND  TIMESTAMP_SUB(o.createdTime, INTERVAL 1 SECOND) 
            THEN d.is_oos 
            ELSE 0 
        END) AS oos_count_3hr,

        -- OOS count in last 6 hours
        SUM(CASE 
            WHEN d.createdTime BETWEEN TIMESTAMP_SUB(o.createdTime, INTERVAL 6 HOUR) AND  TIMESTAMP_SUB(o.createdTime, INTERVAL 1 SECOND) 
            THEN d.is_oos 
            ELSE 0 
        END) AS oos_count_6hr,

        -- OOS count in last 24 hours
        SUM(CASE 
            WHEN d.createdTime BETWEEN TIMESTAMP_SUB(o.createdTime, INTERVAL 24 HOUR) AND  TIMESTAMP_SUB(o.createdTime, INTERVAL 1 SECOND) 
            THEN d.is_oos 
            ELSE 0 
        END) AS oos_count_24hr,

        -- OOS count since start of day
        SUM(CASE 
            WHEN DATE(d.createdTime) = DATE(o.createdTime)  AND d.createdTime < o.createdTime
            THEN d.is_oos 
            ELSE 0 
        END) AS oos_count_startOfDay,

        -- OOS count in last 3 days
        SUM(CASE 
            WHEN d.createdTime BETWEEN TIMESTAMP_SUB(o.createdTime, INTERVAL 3 DAY) AND  TIMESTAMP_SUB(o.createdTime, INTERVAL 1 SECOND) 
            THEN d.is_oos 
            ELSE 0 
        END) AS oos_count_3days,

        -- OOS count in last 7 days (week)
        SUM(CASE 
            WHEN d.createdTime BETWEEN TIMESTAMP_SUB(o.createdTime, INTERVAL 7 DAY) AND  TIMESTAMP_SUB(o.createdTime, INTERVAL 1 SECOND) 
            THEN d.is_oos 
            ELSE 0 
        END) AS oos_count_week,

        -- OOS count in last 30 days (month)
        SUM(CASE 
            WHEN d.createdTime BETWEEN TIMESTAMP_SUB(o.createdTime, INTERVAL 30 DAY) AND  TIMESTAMP_SUB(o.createdTime, INTERVAL 1 SECOND) 
            THEN d.is_oos 
            ELSE 0 
        END) AS oos_count_month

    FROM oos_data o
    LEFT JOIN oos_data d 
    ON o.restaurant_id = d.restaurant_id 
    AND o.order_item_name = d.order_item_name
    AND d.createdTime < o.createdTime

    GROUP BY o.unique_id, o.restaurant_id, o.order_item_name, o.createdTime, o.scenario
)
SELECT * FROM aggregated_oos
ORDER BY createdTime DESC;
