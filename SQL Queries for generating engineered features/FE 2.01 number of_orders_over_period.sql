CREATE OR REPLACE TABLE `just-data-sandbox-oos.ENEL_645.FE201` AS
WITH orders AS (
    SELECT
        unique_id,
        createdTime,
        order_item_name,
        restaurant_id,
        TIMESTAMP_TRUNC(createdTime, DAY) AS start_of_day,
        TIMESTAMP_SUB(createdTime, INTERVAL 1 HOUR) AS one_hour_ago,
        TIMESTAMP_SUB(createdTime, INTERVAL 3 HOUR) AS three_hours_ago,
        TIMESTAMP_SUB(createdTime, INTERVAL 6 HOUR) AS six_hours_ago,
        TIMESTAMP_SUB(createdTime, INTERVAL 24 HOUR) AS one_day_ago,
        TIMESTAMP_SUB(createdTime, INTERVAL 3 DAY) AS three_days_ago,
        TIMESTAMP_SUB(createdTime, INTERVAL 7 DAY) AS one_week_ago,
        TIMESTAMP_SUB(createdTime, INTERVAL 30 DAY) AS one_month_ago
    FROM `just-data-sandbox-oos.ENEL_645.cleaned_data`
)
SELECT
    o.unique_id,
    o.createdTime,
    o.order_item_name,
    o.restaurant_id,
    
    -- Number of orders placed in last 1 hour
    (SELECT COUNT(*) 
     FROM orders 
     WHERE order_item_name = o.order_item_name 
       AND restaurant_id = o.restaurant_id 
       AND createdTime BETWEEN o.one_hour_ago AND o.createdTime
       AND createdTime < o.createdTime) AS last_1hr_orders,

    -- Number of orders placed in last 3 hours
    (SELECT COUNT(*) 
     FROM orders 
     WHERE order_item_name = o.order_item_name 
       AND restaurant_id = o.restaurant_id 
       AND createdTime BETWEEN o.three_hours_ago AND o.createdTime
       AND createdTime < o.createdTime) AS last_3hr_orders,

    -- Number of orders placed in last 6 hours
    (SELECT COUNT(*) 
     FROM orders 
     WHERE order_item_name = o.order_item_name 
       AND restaurant_id = o.restaurant_id 
       AND createdTime BETWEEN o.six_hours_ago AND o.createdTime
       AND createdTime < o.createdTime) AS last_6hr_orders,

    -- Number of orders placed in last 24 hours
    (SELECT COUNT(*) 
     FROM orders 
     WHERE order_item_name = o.order_item_name 
       AND restaurant_id = o.restaurant_id 
       AND createdTime BETWEEN o.one_day_ago AND o.createdTime
       AND createdTime < o.createdTime) AS last_24hr_orders,

    -- Number of orders placed since start of the day
    (SELECT COUNT(*) 
     FROM orders 
     WHERE order_item_name = o.order_item_name 
       AND restaurant_id = o.restaurant_id 
       AND createdTime BETWEEN o.start_of_day AND o.createdTime
       AND createdTime < o.createdTime) AS since_start_of_day_orders,

    -- Number of orders placed in last 3 days
    (SELECT COUNT(*) 
     FROM orders 
     WHERE order_item_name = o.order_item_name 
       AND restaurant_id = o.restaurant_id 
       AND createdTime BETWEEN o.three_days_ago AND o.createdTime
       AND createdTime < o.createdTime) AS last_3days_orders,

    -- Number of orders placed in last 1 week
    (SELECT COUNT(*) 
     FROM orders 
     WHERE order_item_name = o.order_item_name 
       AND restaurant_id = o.restaurant_id 
       AND createdTime BETWEEN o.one_week_ago AND o.createdTime
       AND createdTime < o.createdTime) AS last_week_orders,

    -- Number of orders placed in last 1 month
    (SELECT COUNT(*) 
     FROM orders 
     WHERE order_item_name = o.order_item_name 
       AND restaurant_id = o.restaurant_id 
       AND createdTime BETWEEN o.one_month_ago AND o.createdTime
       AND createdTime < o.createdTime) AS last_month_orders

FROM orders o;
