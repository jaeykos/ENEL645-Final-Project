CREATE OR REPLACE TABLE `just-data-sandbox-oos.ENEL_645.FE207` AS
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
aggregated_oos_rate AS (
    SELECT 
        o.unique_id,
        o.restaurant_id,
        o.order_item_name,
        o.createdTime,
        o.scenario,

        -- OOS rate in last 1 hour
        COALESCE(SAFE_DIVIDE(
            SUM(CASE WHEN d.createdTime BETWEEN TIMESTAMP_SUB(o.createdTime, INTERVAL 1 HOUR) AND o.createdTime 
                THEN d.is_oos ELSE 0 END),
            COUNT(CASE WHEN d.createdTime BETWEEN TIMESTAMP_SUB(o.createdTime, INTERVAL 1 HOUR) AND o.createdTime 
                THEN 1 ELSE NULL END)
        ), 0) AS oos_rate_1hr,

        -- OOS rate in last 3 hours
        COALESCE(SAFE_DIVIDE(
            SUM(CASE WHEN d.createdTime BETWEEN TIMESTAMP_SUB(o.createdTime, INTERVAL 3 HOUR) AND o.createdTime 
                THEN d.is_oos ELSE 0 END),
            COUNT(CASE WHEN d.createdTime BETWEEN TIMESTAMP_SUB(o.createdTime, INTERVAL 3 HOUR) AND o.createdTime 
                THEN 1 ELSE NULL END)
        ), 0) AS oos_rate_3hr,

        -- OOS rate in last 6 hours
        COALESCE(SAFE_DIVIDE(
            SUM(CASE WHEN d.createdTime BETWEEN TIMESTAMP_SUB(o.createdTime, INTERVAL 6 HOUR) AND o.createdTime 
                THEN d.is_oos ELSE 0 END),
            COUNT(CASE WHEN d.createdTime BETWEEN TIMESTAMP_SUB(o.createdTime, INTERVAL 6 HOUR) AND o.createdTime 
                THEN 1 ELSE NULL END)
        ), 0) AS oos_rate_6hr,

        -- OOS rate in last 24 hours
        COALESCE(SAFE_DIVIDE(
            SUM(CASE WHEN d.createdTime BETWEEN TIMESTAMP_SUB(o.createdTime, INTERVAL 24 HOUR) AND o.createdTime 
                THEN d.is_oos ELSE 0 END),
            COUNT(CASE WHEN d.createdTime BETWEEN TIMESTAMP_SUB(o.createdTime, INTERVAL 24 HOUR) AND o.createdTime 
                THEN 1 ELSE NULL END)
        ), 0) AS oos_rate_24hr,

        -- OOS rate since start of the day
        COALESCE(SAFE_DIVIDE(
            SUM(CASE WHEN DATE(d.createdTime) = DATE(o.createdTime) 
                THEN d.is_oos ELSE 0 END),
            COUNT(CASE WHEN DATE(d.createdTime) = DATE(o.createdTime) 
                THEN 1 ELSE NULL END)
        ), 0) AS oos_rate_startOfDay,

        -- OOS rate in last 3 days
        COALESCE(SAFE_DIVIDE(
            SUM(CASE WHEN d.createdTime BETWEEN TIMESTAMP_SUB(o.createdTime, INTERVAL 3 DAY) AND o.createdTime 
                THEN d.is_oos ELSE 0 END),
            COUNT(CASE WHEN d.createdTime BETWEEN TIMESTAMP_SUB(o.createdTime, INTERVAL 3 DAY) AND o.createdTime 
                THEN 1 ELSE NULL END)
        ), 0) AS oos_rate_3days,

        -- OOS rate in last 7 days (week)
        COALESCE(SAFE_DIVIDE(
            SUM(CASE WHEN d.createdTime BETWEEN TIMESTAMP_SUB(o.createdTime, INTERVAL 7 DAY) AND o.createdTime 
                THEN d.is_oos ELSE 0 END),
            COUNT(CASE WHEN d.createdTime BETWEEN TIMESTAMP_SUB(o.createdTime, INTERVAL 7 DAY) AND o.createdTime 
                THEN 1 ELSE NULL END)
        ), 0) AS oos_rate_week,

        -- OOS rate in last 30 days (month)
        COALESCE(SAFE_DIVIDE(
            SUM(CASE WHEN d.createdTime BETWEEN TIMESTAMP_SUB(o.createdTime, INTERVAL 30 DAY) AND o.createdTime 
                THEN d.is_oos ELSE 0 END),
            COUNT(CASE WHEN d.createdTime BETWEEN TIMESTAMP_SUB(o.createdTime, INTERVAL 30 DAY) AND o.createdTime 
                THEN 1 ELSE NULL END)
        ), 0) AS oos_rate_month

    FROM oos_data o
    LEFT JOIN oos_data d 
    ON o.restaurant_id = d.restaurant_id 
    AND o.order_item_name = d.order_item_name
    AND d.createdTime < o.createdTime

    GROUP BY o.unique_id, o.restaurant_id, o.order_item_name, o.createdTime, o.scenario
)
SELECT * FROM aggregated_oos_rate
ORDER BY createdTime DESC;
