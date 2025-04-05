-- predict when items will go out of stock
-- if most recent in-stock time + avg run-out > current time, predict OOS

-- CREATE OR REPLACE TABLE `just-data-sandbox-oos.feature_engineering.FE221` AS
-- `just-data-sandbox-oos.feature_engineering.FE9_merged_features`
-- irTimeSincePrevRestock

CREATE OR REPLACE TABLE `just-data-sandbox-oos.feature_engineering.FE221` AS

WITH OrderedData AS (
    SELECT 
        uid,
        createdTime,
        scenario,
        resId,
        itemId,
        irTimeSincePrevRestock,
        irAvgRestockTime1w,
        LAG(scenario) OVER (
            PARTITION BY resId, itemId 
            ORDER BY createdTime
        ) AS prev_scenario,
        LAG(createdTime) OVER (
            PARTITION BY resId, itemId 
            ORDER BY createdTime
        ) AS prev_createdTime
    FROM `just-data-sandbox-oos.feature_engineering.FE9_merged_features`
),

OutOfStockTimes AS (
    SELECT 
        d.uid,
        d.resId,
        d.itemId,
        d.createdTime,
        d.scenario,
        d.irTimeSincePrevRestock,
        d.irAvgRestockTime1w,
        MIN(o.createdTime) OVER (
            PARTITION BY d.resId, d.itemId
            ORDER BY d.createdTime
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) AS next_out_of_stock_time
    FROM OrderedData d
    LEFT JOIN OrderedData o 
        ON d.resId = o.resId 
        AND d.itemId = o.itemId
        AND o.scenario = 'OUT_OF_STOCK'
        AND o.createdTime > d.createdTime
    WHERE d.scenario = 'DELIVERED'
),

TimeDifferences AS (
    SELECT 
        uid,
        resId,
        itemId,
        createdTime,
        scenario,
        irTimeSincePrevRestock,
        irAvgRestockTime1w,
        TIMESTAMP_DIFF(next_out_of_stock_time, createdTime, MINUTE) AS out_of_stock_time
    FROM OutOfStockTimes
    WHERE next_out_of_stock_time IS NOT NULL
),

AvgOutOfStockTime AS (
    SELECT 
        t.uid,
        t.resId,
        t.itemId,
        t.createdTime,
        t.out_of_stock_time,
        t.scenario,
        t.irTimeSincePrevRestock,
        t.irAvgRestockTime1w,
        AVG(t2.out_of_stock_time) OVER (
            PARTITION BY t.resId, t.itemId 
            ORDER BY t.uid ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS avg_out_of_stock_time
    FROM TimeDifferences t
    LEFT JOIN TimeDifferences t2
        ON t.resId = t2.resId 
        AND t.itemId = t2.itemId
        AND t2.uid < t.uid
)

-- SELECT 
--     f.*,
--     a.avg_out_of_stock_time
-- FROM `just-data-sandbox-oos.feature_engineering.FE9_merged_features` f
-- LEFT JOIN AvgOutOfStockTime a 
--     ON f.uid = a.uid;


SELECT 
  t.uid as unique_id,
  t.createdTime,
  t.resId,
  t.itemId,
  t.scenario,
  (CASE t.irTimeSincePrevRestock > t.irAvgRestockTime1w
    WHEN TRUE THEN 1
    ELSE 0
  END) AS deterministicOOS2

FROM AvgOutOfStockTime AS t