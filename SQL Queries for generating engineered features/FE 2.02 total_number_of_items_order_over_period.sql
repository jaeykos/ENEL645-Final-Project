CREATE OR REPLACE TABLE `just-data-sandbox-oos.ENEL_645.FE202` AS
WITH orders AS (
    SELECT
        unique_id,
        createdTime,
        order_item_name,
        restaurant_id,
        order_item_quantity,
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
    o.order_item_quantity,
    
-- Total orders in last 1 hour
    COALESCE((SELECT SUM(order_item_quantity) 
     FROM orders 
     WHERE order_item_name = o.order_item_name 
       AND restaurant_id = o.restaurant_id 
       AND createdTime BETWEEN o.one_hour_ago AND o.createdTime
       AND createdTime < o.createdTime), 0) AS last_1hr_orders,

    -- Total orders in last 3 hours
    COALESCE((SELECT SUM(order_item_quantity) 
     FROM orders 
     WHERE order_item_name = o.order_item_name 
       AND restaurant_id = o.restaurant_id 
       AND createdTime BETWEEN o.three_hours_ago AND o.createdTime
       AND createdTime < o.createdTime), 0) AS last_3hr_orders,

    -- Total orders in last 6 hours
    COALESCE((SELECT SUM(order_item_quantity) 
     FROM orders 
     WHERE order_item_name = o.order_item_name 
       AND restaurant_id = o.restaurant_id 
       AND createdTime BETWEEN o.six_hours_ago AND o.createdTime
       AND createdTime < o.createdTime), 0) AS last_6hr_orders,

    -- Total orders in last 24 hours
    COALESCE((SELECT SUM(order_item_quantity) 
     FROM orders 
     WHERE order_item_name = o.order_item_name 
       AND restaurant_id = o.restaurant_id 
       AND createdTime BETWEEN o.one_day_ago AND o.createdTime
       AND createdTime < o.createdTime), 0) AS last_24hr_orders,

    -- Total orders since start of the day
    COALESCE((SELECT SUM(order_item_quantity) 
     FROM orders 
     WHERE order_item_name = o.order_item_name 
       AND restaurant_id = o.restaurant_id 
       AND createdTime BETWEEN o.start_of_day AND o.createdTime
       AND createdTime < o.createdTime), 0) AS since_start_of_day_orders,

    -- Total orders in last 3 days
    COALESCE((SELECT SUM(order_item_quantity) 
     FROM orders 
     WHERE order_item_name = o.order_item_name 
       AND restaurant_id = o.restaurant_id 
       AND createdTime BETWEEN o.three_days_ago AND o.createdTime
       AND createdTime < o.createdTime), 0) AS last_3days_orders,

    -- Total orders in last 1 week
    COALESCE((SELECT SUM(order_item_quantity) 
     FROM orders 
     WHERE order_item_name = o.order_item_name 
       AND restaurant_id = o.restaurant_id 
       AND createdTime BETWEEN o.one_week_ago AND o.createdTime
       AND createdTime < o.createdTime), 0) AS last_week_orders,

    -- Total orders in last 1 month
    COALESCE((SELECT SUM(order_item_quantity) 
     FROM orders 
     WHERE order_item_name = o.order_item_name 
       AND restaurant_id = o.restaurant_id 
       AND createdTime BETWEEN o.one_month_ago AND o.createdTime
       AND createdTime < o.createdTime), 0) AS last_month_orders

FROM orders o;
