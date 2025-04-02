CREATE OR REPLACE TABLE `just-data-sandbox-oos.ENEL_645.FE215` AS
WITH event_groups AS (
  SELECT
    unique_id,
    restaurant_id,
    order_item_name,
    createdTime,
    scenario,
    -- Flag new OUT_OF_STOCK sequences
    CASE
      WHEN scenario = 'OUT_OF_STOCK'
      AND LAG(scenario) OVER (PARTITION BY restaurant_id, order_item_name ORDER BY createdTime) != 'OUT_OF_STOCK'
      THEN createdTime
    END AS oos_group_start,
    -- Identify DELIVERED events following OUT_OF_STOCK
    CASE
      WHEN scenario = 'DELIVERED'
      AND LAG(scenario) OVER (PARTITION BY restaurant_id, order_item_name ORDER BY createdTime) = 'OUT_OF_STOCK'
      THEN createdTime
    END AS delivery_time
  FROM `just-data-sandbox-oos.ENEL_645.cleaned_data`
),

calculated_times AS (
  SELECT
    *,
    -- Get first OUT_OF_STOCK timestamp in current sequence
    LAST_VALUE(oos_group_start IGNORE NULLS) OVER (
      PARTITION BY restaurant_id, order_item_name
      ORDER BY createdTime
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS current_oos_start,
    -- Calculate restocking duration at delivery point
    TIMESTAMP_DIFF(
      delivery_time,
      LAST_VALUE(oos_group_start IGNORE NULLS) OVER (
        PARTITION BY restaurant_id, order_item_name
        ORDER BY createdTime
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ),
      HOUR
    ) AS raw_restocking_minutes
  FROM event_groups
)

SELECT
  unique_id,
  restaurant_id,
  order_item_name,
  createdTime,
  scenario,
  -- Propagate restocking duration until next OUT_OF_STOCK sequence
  LAST_VALUE(raw_restocking_minutes IGNORE NULLS) OVER (
    PARTITION BY restaurant_id, order_item_name
    ORDER BY createdTime
    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
  ) AS prev_restocking_time_in_hour
FROM calculated_times
ORDER BY createdTime DESC;
