CREATE OR REPLACE TABLE `just-data-sandbox-oos.feature_engineering.FE2087` AS
WITH time_ranges AS (
  SELECT
    unique_id,
    restaurant_id,
    order_item_name,
    createdTime AS event_time,
    scenario,
    LEAD(createdTime) OVER (PARTITION BY restaurant_id, order_item_name ORDER BY createdTime) AS next_event_time
  FROM `just-data-sandbox-oos.feature_engineering.cleaned_data`
),
overlapping_oos AS (
  SELECT
    c.unique_id,
    c.restaurant_id,
    c.order_item_name,
    c.event_time AS createdTime,
    c.scenario AS current_scenario,  
    p.event_time AS prev_time,  
    p.scenario AS prev_scenario,  
    p.next_event_time AS prev_next_event_time,  
    GREATEST(
      TIMESTAMP_ADD(c.event_time, INTERVAL -7 DAY),  -- Use INTERVAL -7 DAY for 1 week
      p.event_time
    ) AS overlap_start,  
    LEAST(
      c.event_time,
      COALESCE(p.next_event_time, c.event_time)
    ) AS overlap_end,  
    CASE 
      WHEN p.scenario = 'OUT_OF_STOCK' AND TIMESTAMP_DIFF(
        LEAST(c.event_time, COALESCE(p.next_event_time, c.event_time)),
        GREATEST(TIMESTAMP_ADD(c.event_time, INTERVAL -7 DAY), p.event_time),
        MINUTE  
      ) > 0 THEN 
        TIMESTAMP_DIFF(
          LEAST(c.event_time, COALESCE(p.next_event_time, c.event_time)),
          GREATEST(TIMESTAMP_ADD(c.event_time, INTERVAL -7 DAY), p.event_time),
          MINUTE  
        )
      ELSE 0 
    END AS overlapping_minutes  
  FROM time_ranges AS c
  JOIN time_ranges AS p
    ON c.restaurant_id = p.restaurant_id
   AND c.order_item_name = p.order_item_name
   AND p.event_time < c.event_time
   AND p.event_time >= TIMESTAMP_ADD(c.event_time, INTERVAL -7 DAY)
)
SELECT 
  t.unique_id,
  t.restaurant_id,  
  t.order_item_name,  
  t.event_time AS createdTime,  
  t.scenario AS current_scenario,  
  COALESCE(SUM(o.overlapping_minutes), 0) AS oos_duration_last_1week_minutes  
FROM time_ranges AS t
LEFT JOIN overlapping_oos AS o
  ON t.restaurant_id = o.restaurant_id
 AND t.order_item_name = o.order_item_name
 AND t.event_time = o.createdTime
GROUP BY t.unique_id, t.restaurant_id, t.order_item_name, t.event_time, t.scenario
ORDER BY t.restaurant_id, t.order_item_name, t.event_time DESC;
