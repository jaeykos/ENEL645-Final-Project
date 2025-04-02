CREATE OR REPLACE TABLE `just-data-sandbox-oos.feature_engineering.FE111-112` AS
SELECT 
  t.*,

  COALESCE(
    (SELECT SUM(past.total) * 0.3
    FROM `just-data-sandbox-oos.feature_engineering.raw_data` past
    WHERE 
      past.scenario = 'DELIVERED'
      AND past.restaurant_id = t.restaurant_id
      AND past.createdTime >= TIMESTAMP_SUB(t.createdTime, INTERVAL 3 DAY)
      AND past.createdTime < t.createdTime
    ) / 3,
    0
  ) AS avg_daily_profit_past_3_days,

  COALESCE(
    (SELECT SUM(past.total) * 0.3
    FROM `just-data-sandbox-oos.feature_engineering.raw_data` past
    WHERE 
      past.scenario = 'DELIVERED'
      AND past.restaurant_id = t.restaurant_id
      AND past.createdTime >= TIMESTAMP_SUB(t.createdTime, INTERVAL 7 DAY)
      AND past.createdTime < t.createdTime
    ) / 7,
    0
  ) AS avg_daily_profit_past_week,

  COALESCE(
    (SELECT SUM(past.total) * 0.3
    FROM `just-data-sandbox-oos.feature_engineering.raw_data` past
    WHERE 
      past.scenario = 'DELIVERED'
      AND past.restaurant_id = t.restaurant_id
      AND past.createdTime >= TIMESTAMP_SUB(t.createdTime, INTERVAL 30 DAY)
      AND past.createdTime < t.createdTime
    ) / 30,
    0
  ) AS avg_daily_profit_past_month,

  COALESCE(
    (SELECT COUNT(past.unique_id) * 70
    FROM `just-data-sandbox-oos.feature_engineering.raw_data` past
    WHERE 
      past.scenario = 'OUT_OF_STOCK'
      AND past.restaurant_id = t.restaurant_id
      AND past.createdTime >= TIMESTAMP_SUB(t.createdTime, INTERVAL 3 DAY)
      AND past.createdTime < t.createdTime
    ) / 3,
    0
  ) AS avg_daily_loss_past_3_days,

  COALESCE(
    (SELECT COUNT(past.unique_id) * 70
    FROM `just-data-sandbox-oos.feature_engineering.raw_data` past
    WHERE 
      past.scenario = 'OUT_OF_STOCK'
      AND past.restaurant_id = t.restaurant_id
      AND past.createdTime >= TIMESTAMP_SUB(t.createdTime, INTERVAL 7 DAY)
      AND past.createdTime < t.createdTime
    ) / 7,
    0
  ) AS avg_daily_loss_past_week,

  COALESCE(
    (SELECT COUNT(past.unique_id) * 70
    FROM `just-data-sandbox-oos.feature_engineering.raw_data` past
    WHERE 
      past.scenario = 'OUT_OF_STOCK'
      AND past.restaurant_id = t.restaurant_id
      AND past.createdTime >= TIMESTAMP_SUB(t.createdTime, INTERVAL 30 DAY)
      AND past.createdTime < t.createdTime
    ) / 30,
    0
  ) AS avg_daily_loss_past_month,

FROM `just-data-sandbox-oos.feature_engineering.raw_data` as t
ORDER BY t.createdTime