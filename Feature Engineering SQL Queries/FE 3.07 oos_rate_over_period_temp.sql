CREATE OR REPLACE TABLE `just-data-sandbox-oos.feature_engineering.FE307_temp` AS
SELECT
    unique_id,
    order_item_name,
    createdTime,
    scenario,
    CASE WHEN scenario = 'OUT_OF_STOCK' THEN 1 ELSE 0 END AS is_oos,
    TIMESTAMP_SUB(createdTime, INTERVAL 1 HOUR) AS one_hour_ago,
    TIMESTAMP_SUB(createdTime, INTERVAL 3 HOUR) AS three_hours_ago,
    TIMESTAMP_SUB(createdTime, INTERVAL 6 HOUR) AS six_hours_ago,
    TIMESTAMP_SUB(createdTime, INTERVAL 24 HOUR) AS one_day_ago,
    TIMESTAMP_SUB(createdTime, INTERVAL 3 DAY) AS three_days_ago,
    TIMESTAMP_SUB(createdTime, INTERVAL 7 DAY) AS one_week_ago,
    TIMESTAMP_SUB(createdTime, INTERVAL 30 DAY) AS one_month_ago
FROM `just-data-sandbox-oos.feature_engineering.cleaned_data`;
