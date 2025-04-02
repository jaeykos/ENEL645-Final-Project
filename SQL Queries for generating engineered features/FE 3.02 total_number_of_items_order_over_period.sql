CREATE OR REPLACE TABLE `just-data-sandbox-oos.feature_engineering.FE302` AS
SELECT
    o.unique_id,
    o.createdTime,
    o.order_item_name,
    o.order_item_quantity,

    -- Aggregated orders within different time frames
    COUNTIF(d.createdTime BETWEEN o.one_hour_ago AND o.createdTime) AS last_1hr_orders,
    COUNTIF(d.createdTime BETWEEN o.three_hours_ago AND o.createdTime) AS last_3hr_orders,
    COUNTIF(d.createdTime BETWEEN o.six_hours_ago AND o.createdTime) AS last_6hr_orders,
    COUNTIF(d.createdTime BETWEEN o.one_day_ago AND o.createdTime) AS last_24hr_orders,
    COUNTIF(d.createdTime BETWEEN o.start_of_day AND o.createdTime) AS since_start_of_day_orders,
    COUNTIF(d.createdTime BETWEEN o.three_days_ago AND o.createdTime) AS last_3days_orders,
    COUNTIF(d.createdTime BETWEEN o.one_week_ago AND o.createdTime) AS last_week_orders,
    COUNTIF(d.createdTime BETWEEN o.one_month_ago AND o.createdTime) AS last_month_orders

FROM `just-data-sandbox-oos.feature_engineering.FE302_temp` o
LEFT JOIN `just-data-sandbox-oos.feature_engineering.FE302_temp` d
    ON d.order_item_name = o.order_item_name
    AND d.unique_id != o.unique_id
GROUP BY
    o.unique_id,
    o.createdTime,
    o.order_item_name,
    o.order_item_quantity;
