CREATE OR REPLACE TABLE `just-data-sandbox-oos.feature_engineering.FE301` AS
SELECT
    o.unique_id,
    o.createdTime,
    o.order_item_name,

    -- Number of orders placed in last 1 hour
    COUNTIF(i.createdTime BETWEEN o.one_hour_ago AND o.createdTime AND i.unique_id != o.unique_id) AS last_1hr_orders,

    -- Number of orders placed in last 3 hours
    COUNTIF(i.createdTime BETWEEN o.three_hours_ago AND o.createdTime AND i.unique_id != o.unique_id) AS last_3hr_orders,

    -- Number of orders placed in last 6 hours
    COUNTIF(i.createdTime BETWEEN o.six_hours_ago AND o.createdTime AND i.unique_id != o.unique_id) AS last_6hr_orders,

    -- Number of orders placed in last 24 hours
    COUNTIF(i.createdTime BETWEEN o.one_day_ago AND o.createdTime AND i.unique_id != o.unique_id) AS last_24hr_orders,

    -- Number of orders placed since start of the day
    COUNTIF(i.createdTime BETWEEN o.start_of_day AND o.createdTime AND i.unique_id != o.unique_id) AS since_start_of_day_orders,

    -- Number of orders placed in last 3 days
    COUNTIF(i.createdTime BETWEEN o.three_days_ago AND o.createdTime AND i.unique_id != o.unique_id) AS last_3days_orders,

    -- Number of orders placed in last 1 week
    COUNTIF(i.createdTime BETWEEN o.one_week_ago AND o.createdTime AND i.unique_id != o.unique_id) AS last_week_orders,

    -- Number of orders placed in last 1 month
    COUNTIF(i.createdTime BETWEEN o.one_month_ago AND o.createdTime AND i.unique_id != o.unique_id) AS last_month_orders

FROM `just-data-sandbox-oos.feature_engineering.FE301_temp` o
LEFT JOIN `just-data-sandbox-oos.feature_engineering.FE301_temp` i
ON o.order_item_name = i.order_item_name
AND i.createdTime <= o.createdTime

GROUP BY o.unique_id, o.createdTime, o.order_item_name;
