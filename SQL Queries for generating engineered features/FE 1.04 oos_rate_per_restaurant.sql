-- This query shows the OOS rate over different time periods (that have the restauraunt id as the current row)
-- OOS rate = (# rows that are out of stock) / (total # rows)
CREATE OR REPLACE TABLE `just-data-sandbox-oos.feature_engineering.FE104` AS
SELECT
  *,

  IFNULL(
  SAFE_DIVIDE(
    (
      SELECT
        COUNT(unique_id)
      FROM
        `just-data-sandbox-oos.feature_engineering.raw_data` AS t2
      WHERE
        t2.restaurant_id = t1.restaurant_id
        AND t2.createdTime BETWEEN TIMESTAMP_SUB(t1.createdTime, INTERVAL 1 HOUR) AND t1.createdTime
        AND t2.scenario = 'OUT_OF_STOCK'
        AND t2.unique_id != t1.unique_id
    ),
    (
      SELECT
        COUNT(unique_id)
      FROM
        `just-data-sandbox-oos.feature_engineering.raw_data` AS t2
      WHERE
        t2.restaurant_id = t1.restaurant_id
        AND t2.createdTime BETWEEN TIMESTAMP_SUB(t1.createdTime, INTERVAL 1 HOUR) AND t1.createdTime
        AND t2.unique_id != t1.unique_id
    )
  ), 0) AS oss_rate_past_1_hour_per_restaurant,

  IFNULL(
  SAFE_DIVIDE(
    (
      SELECT
        COUNT(unique_id)
      FROM
        `just-data-sandbox-oos.feature_engineering.raw_data` AS t2
      WHERE
        t2.restaurant_id = t1.restaurant_id
        AND t2.createdTime BETWEEN TIMESTAMP_SUB(t1.createdTime, INTERVAL 3 HOUR) AND t1.createdTime
        AND t2.scenario = 'OUT_OF_STOCK'
        AND t2.unique_id != t1.unique_id
    ),
    (
      SELECT
        COUNT(unique_id)
      FROM
        `just-data-sandbox-oos.feature_engineering.raw_data` AS t2
      WHERE
        t2.restaurant_id = t1.restaurant_id
        AND t2.createdTime BETWEEN TIMESTAMP_SUB(t1.createdTime, INTERVAL 3 HOUR) AND t1.createdTime
        AND t2.unique_id != t1.unique_id
    )
  ), 0) AS oss_rate_past_3_hours_per_restaurant,

  IFNULL(
  SAFE_DIVIDE(
    (
      SELECT
        COUNT(unique_id)
      FROM
        `just-data-sandbox-oos.feature_engineering.raw_data` AS t2
      WHERE
        t2.restaurant_id = t1.restaurant_id
        AND t2.createdTime BETWEEN TIMESTAMP_SUB(t1.createdTime, INTERVAL 6 HOUR) AND t1.createdTime
        AND t2.scenario = 'OUT_OF_STOCK'
        AND t2.unique_id != t1.unique_id
    ),
    (
      SELECT
        COUNT(unique_id)
      FROM
        `just-data-sandbox-oos.feature_engineering.raw_data` AS t2
      WHERE
        t2.restaurant_id = t1.restaurant_id
        AND t2.createdTime BETWEEN TIMESTAMP_SUB(t1.createdTime, INTERVAL 6 HOUR) AND t1.createdTime
        AND t2.unique_id != t1.unique_id
    )
  ), 0) AS oss_rate_past_6_hours_per_restaurant,

  IFNULL(
  SAFE_DIVIDE(
    (
      SELECT
        COUNT(unique_id)
      FROM
        `just-data-sandbox-oos.feature_engineering.raw_data` AS t2
      WHERE
        t2.restaurant_id = t1.restaurant_id
        AND t2.createdTime BETWEEN TIMESTAMP_SUB(t1.createdTime, INTERVAL 1 DAY) AND t1.createdTime
        AND t2.scenario = 'OUT_OF_STOCK'
        AND t2.unique_id != t1.unique_id
    ),
    (
      SELECT
        COUNT(unique_id)
      FROM
        `just-data-sandbox-oos.feature_engineering.raw_data` AS t2
      WHERE
        t2.restaurant_id = t1.restaurant_id
        AND t2.createdTime BETWEEN TIMESTAMP_SUB(t1.createdTime, INTERVAL 1 DAY) AND t1.createdTime
        AND t2.unique_id != t1.unique_id
    )
  ), 0) AS oss_rate_past_1_day_per_restaurant,

  IFNULL(
  SAFE_DIVIDE(
    (
      SELECT
        COUNT(unique_id)
      FROM
        `just-data-sandbox-oos.feature_engineering.raw_data` AS t2
      WHERE
        t2.restaurant_id = t1.restaurant_id
        AND t2.createdTime BETWEEN TIMESTAMP_TRUNC(t1.createdTime, DAY) AND t1.createdTime
        AND t2.scenario = 'OUT_OF_STOCK'
        AND t2.unique_id != t1.unique_id
    ),
    (
      SELECT
        COUNT(unique_id)
      FROM
        `just-data-sandbox-oos.feature_engineering.raw_data` AS t2
      WHERE
        t2.restaurant_id = t1.restaurant_id
        AND t2.createdTime BETWEEN TIMESTAMP_TRUNC(t1.createdTime, DAY) AND t1.createdTime
        AND t2.unique_id != t1.unique_id
    )
  ), 0) AS oss_rate_today_per_restaurant,

  IFNULL(
  SAFE_DIVIDE(
    (
      SELECT
        COUNT(unique_id)
      FROM
        `just-data-sandbox-oos.feature_engineering.raw_data` AS t2
      WHERE
        t2.restaurant_id = t1.restaurant_id
        AND t2.createdTime BETWEEN TIMESTAMP_SUB(t1.createdTime, INTERVAL 3 DAY) AND t1.createdTime
        AND t2.scenario = 'OUT_OF_STOCK'
        AND t2.unique_id != t1.unique_id
    ),
    (
      SELECT
        COUNT(unique_id)
      FROM
        `just-data-sandbox-oos.feature_engineering.raw_data` AS t2
      WHERE
        t2.restaurant_id = t1.restaurant_id
        AND t2.createdTime BETWEEN TIMESTAMP_SUB(t1.createdTime, INTERVAL 3 DAY) AND t1.createdTime
        AND t2.unique_id != t1.unique_id
    )
  ), 0) AS oss_rate_past_3_days_per_restaurant,

  IFNULL(
  SAFE_DIVIDE(
    (
      SELECT
        COUNT(unique_id)
      FROM
        `just-data-sandbox-oos.feature_engineering.raw_data` AS t2
      WHERE
        t2.restaurant_id = t1.restaurant_id
        AND t2.createdTime BETWEEN TIMESTAMP_SUB(t1.createdTime, INTERVAL 7 DAY) AND t1.createdTime
        AND t2.scenario = 'OUT_OF_STOCK'
        AND t2.unique_id != t1.unique_id
    ),
    (
      SELECT
        COUNT(unique_id)
      FROM
        `just-data-sandbox-oos.feature_engineering.raw_data` AS t2
      WHERE
        t2.restaurant_id = t1.restaurant_id
        AND t2.createdTime BETWEEN TIMESTAMP_SUB(t1.createdTime, INTERVAL 7 DAY) AND t1.createdTime
        AND t2.unique_id != t1.unique_id
    )
  ), 0) AS oss_rate_past_1_week_per_restaurant,

  IFNULL(
  SAFE_DIVIDE(
    (
      SELECT
        COUNT(unique_id)
      FROM
        `just-data-sandbox-oos.feature_engineering.raw_data` AS t2
      WHERE
        t2.restaurant_id = t1.restaurant_id
        AND t2.createdTime BETWEEN TIMESTAMP_SUB(t1.createdTime, INTERVAL 30 DAY) AND t1.createdTime
        AND t2.scenario = 'OUT_OF_STOCK'
        AND t2.unique_id != t1.unique_id
    ),
    (
      SELECT
        COUNT(unique_id)
      FROM
        `just-data-sandbox-oos.feature_engineering.raw_data` AS t2
      WHERE
        t2.restaurant_id = t1.restaurant_id
        AND t2.createdTime BETWEEN TIMESTAMP_SUB(t1.createdTime, INTERVAL 30 DAY) AND t1.createdTime
        AND t2.unique_id != t1.unique_id
    )
  ), 0) AS oss_rate_past_1_month_per_restaurant,
  
FROM
  `just-data-sandbox-oos.feature_engineering.raw_data` AS t1
ORDER BY createdTime