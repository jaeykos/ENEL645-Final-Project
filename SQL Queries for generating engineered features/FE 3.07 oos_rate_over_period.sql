CREATE OR REPLACE TABLE `just-data-sandbox-oos.feature_engineering.FE307` AS
SELECT 
    o.unique_id,
    o.order_item_name,
    o.createdTime,
    o.scenario,

    -- OOS rate in last 1 hour
    SAFE_DIVIDE(
        SUM(CASE WHEN i.createdTime BETWEEN o.one_hour_ago AND o.createdTime THEN i.is_oos ELSE 0 END),
        COUNT(CASE WHEN i.createdTime BETWEEN o.one_hour_ago AND o.createdTime THEN 1 ELSE NULL END)
    ) AS oos_rate_1hr,

    -- OOS rate in last 3 hours
    SAFE_DIVIDE(
        SUM(CASE WHEN i.createdTime BETWEEN o.three_hours_ago AND o.createdTime THEN i.is_oos ELSE 0 END),
        COUNT(CASE WHEN i.createdTime BETWEEN o.three_hours_ago AND o.createdTime THEN 1 ELSE NULL END)
    ) AS oos_rate_3hr,

    -- OOS rate in last 6 hours
    SAFE_DIVIDE(
        SUM(CASE WHEN i.createdTime BETWEEN o.six_hours_ago AND o.createdTime THEN i.is_oos ELSE 0 END),
        COUNT(CASE WHEN i.createdTime BETWEEN o.six_hours_ago AND o.createdTime THEN 1 ELSE NULL END)
    ) AS oos_rate_6hr,

    -- OOS rate in last 24 hours
    SAFE_DIVIDE(
        SUM(CASE WHEN i.createdTime BETWEEN o.one_day_ago AND o.createdTime THEN i.is_oos ELSE 0 END),
        COUNT(CASE WHEN i.createdTime BETWEEN o.one_day_ago AND o.createdTime THEN 1 ELSE NULL END)
    ) AS oos_rate_24hr,

    -- OOS rate since start of the day
    SAFE_DIVIDE(
        SUM(CASE WHEN DATE(i.createdTime) = DATE(o.createdTime) THEN i.is_oos ELSE 0 END),
        COUNT(CASE WHEN DATE(i.createdTime) = DATE(o.createdTime) THEN 1 ELSE NULL END)
    ) AS oos_rate_startOfDay,

    -- OOS rate in last 3 days
    SAFE_DIVIDE(
        SUM(CASE WHEN i.createdTime BETWEEN o.three_days_ago AND o.createdTime THEN i.is_oos ELSE 0 END),
        COUNT(CASE WHEN i.createdTime BETWEEN o.three_days_ago AND o.createdTime THEN 1 ELSE NULL END)
    ) AS oos_rate_3days,

    -- OOS rate in last 7 days (week)
    SAFE_DIVIDE(
        SUM(CASE WHEN i.createdTime BETWEEN o.one_week_ago AND o.createdTime THEN i.is_oos ELSE 0 END),
        COUNT(CASE WHEN i.createdTime BETWEEN o.one_week_ago AND o.createdTime THEN 1 ELSE NULL END)
    ) AS oos_rate_week,

    -- OOS rate in last 30 days (month)
    SAFE_DIVIDE(
        SUM(CASE WHEN i.createdTime BETWEEN o.one_month_ago AND o.createdTime THEN i.is_oos ELSE 0 END),
        COUNT(CASE WHEN i.createdTime BETWEEN o.one_month_ago AND o.createdTime THEN 1 ELSE NULL END)
    ) AS oos_rate_month

FROM `just-data-sandbox-oos.feature_engineering.FE307_temp` o
LEFT JOIN `just-data-sandbox-oos.feature_engineering.FE307_temp` i
ON o.order_item_name = i.order_item_name
AND i.createdTime <= o.createdTime
AND i.unique_id != o.unique_id

GROUP BY o.unique_id, o.order_item_name, o.createdTime, o.scenario
ORDER BY o.createdTime DESC;
