-- If prev status is in stock, then set OOS to 0
-- If prev status is out of stock, then calculate the time elapsed since earliest oos that comes after the earliest in stock
-- if time elapsed > avg restock time, set OOS to 0. Else, set OOS to 1.

CREATE OR REPLACE TABLE `just-data-sandbox-oos.feature_engineering.FE220` AS

WITH latest_delivery_time as (
  SELECT
    t.uid,
    t.createdTime,
    t.resId,
    t.itemId,
    t.scenario,
    t.irPrevScenario,
    t.irAvgRestockTime1w,
    MAX(j.createdTime) as latest_delivery
  FROM `just-data-sandbox-oos.feature_engineering.FE9_merged_features` t
  LEFT JOIN `just-data-sandbox-oos.feature_engineering.FE9_merged_features` j on 
    j.resId = t.resId
    AND j.itemId = t.itemId
    AND j.createdTime <= t.createdTime 
    AND j.uid <> t.uid
    AND j.scenario = 'DELIVERED'
  GROUP BY t.uid, t.createdTime, t.resId, t.itemId, t.scenario, t.irPrevScenario, t.irAvgRestockTime1w
),

earliest_oos_time_after_latest_delivery as (
  SELECT
    t.uid,
    t.createdTime,
    t.resId,
    t.itemId,
    t.scenario,
    t.irPrevScenario,
    t.irAvgRestockTime1w,
    MIN(j.createdTime) as earliest_oos_time
  FROM latest_delivery_time t
  LEFT JOIN latest_delivery_time j on 
    j.resId = t.resId
    AND j.itemId = t.itemId
    AND j.createdTime <= t.createdTime
    AND j.createdTime > t.latest_delivery 
    AND j.uid <> t.uid
    AND j.scenario = 'OUT_OF_STOCK'
  GROUP BY t.uid, t.createdTime, t.resId, t.itemId, t.scenario, t.irPrevScenario, t.irAvgRestockTime1w
)

-- SELECT * FROM earliest_oos_time_after_latest_delivery

SELECT 
  t.uid as unique_id,
  t.createdTime,
  t.resId,
  t.itemId,
  t.scenario,
  CASE
    WHEN t.irPrevScenario = 'DELIVERED' THEN 0
    ELSE (
      CASE TIMESTAMP_DIFF(t.earliest_oos_time, t.createdTime, MINUTE) > t.irAvgRestockTime1w
        WHEN TRUE THEN 0
        ELSE 1
      END
    )
  END AS deterministic_oos_minutes, 
  CASE
    WHEN t.irPrevScenario = 'DELIVERED' THEN 0
    ELSE (
      CASE TIMESTAMP_DIFF(t.earliest_oos_time, t.createdTime, DAY) > t.irAvgRestockTime1w
        WHEN TRUE THEN 0
        ELSE 1
      END
    )
  END AS deterministic_oos_days

FROM earliest_oos_time_after_latest_delivery AS t
ORDER BY t.createdTime
