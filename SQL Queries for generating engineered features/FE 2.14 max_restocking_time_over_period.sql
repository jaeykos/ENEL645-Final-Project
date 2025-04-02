CREATE OR REPLACE TABLE `just-data-sandbox-oos.feature_engineering.FE214` AS
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

  -- Maximum restocking time over the past week (7 days)
  MAX(prev_restocking_time_in_minutes) OVER (
    PARTITION BY restaurant_id, order_item_name
    ORDER BY created_sec
    RANGE BETWEEN 604800 PRECEDING AND 1 PRECEDING  -- 604800 sec = 7 days
  ) AS max_restock_time_week,

  -- Maximum restocking time over the past month (30 days)
  MAX(prev_restocking_time_in_minutes) OVER (
    PARTITION BY restaurant_id, order_item_name
    ORDER BY created_sec
    RANGE BETWEEN 2592000 PRECEDING AND 1 PRECEDING  -- 2592000 sec = 30 days
  ) AS max_restock_time_month,

  -- Maximum restocking time over the past 3 months (90 days)
  MAX(prev_restocking_time_in_minutes) OVER (
    PARTITION BY restaurant_id, order_item_name
    ORDER BY created_sec
    RANGE BETWEEN 7776000 PRECEDING AND 1 PRECEDING  -- 7776000 sec = 90 days
  ) AS max_restock_time_3months,

  -- Maximum restocking time over the past 6 months (180 days)
  MAX(prev_restocking_time_in_minutes) OVER (
    PARTITION BY restaurant_id, order_item_name
    ORDER BY created_sec
    RANGE BETWEEN 15552000 PRECEDING AND 1 PRECEDING -- 15552000 sec = 180 days
  ) AS max_restock_time_6months,

  -- Maximum restocking time over the past year (365 days)
  MAX(prev_restocking_time_in_minutes) OVER (
    PARTITION BY restaurant_id, order_item_name
    ORDER BY created_sec
    RANGE BETWEEN 31536000 PRECEDING AND 1 PRECEDING -- 31536000 sec = 365 days
  ) AS max_restock_time_year

FROM time_conversion
ORDER BY createdTime DESC;
