CREATE OR REPLACE TABLE `just-data-sandbox-oos.feature_engineering.FE210-211` AS
WITH time_conversion AS (
  SELECT *,
    UNIX_SECONDS(createdTime) AS created_sec  -- Convert timestamp to seconds for range calculation
  FROM `just-data-sandbox-oos.feature_engineering.FE215`
)

SELECT 
  unique_id,
  restaurant_id,
  order_item_name,
  createdTime,
  scenario,
  prev_restocking_time_in_minutes,
  -- 7-day rolling average using window functions
  AVG(prev_restocking_time_in_minutes) OVER (
    PARTITION BY restaurant_id, order_item_name
    ORDER BY created_sec
    RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW  -- 604800 sec = 7 days
  ) AS avg_restock_time_week,
  
  -- 30-day rolling average
  AVG(prev_restocking_time_in_minutes) OVER (
    PARTITION BY restaurant_id, order_item_name
    ORDER BY created_sec
    RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW  -- 2592000 sec = 30 days
  ) AS avg_restock_time_month
FROM time_conversion
ORDER BY createdTime DESC;
