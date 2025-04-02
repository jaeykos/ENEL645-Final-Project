CREATE OR REPLACE TABLE `just-data-sandbox-oos.feature_engineering.FE0` AS
SELECT 
    *,
    FORMAT_TIMESTAMP('%A', createdTime) AS weekday,          -- Full weekday name (Monday, Tuesday)
    FORMAT_TIMESTAMP('%d', createdTime) AS day_of_month,     -- Day of the month
    EXTRACT(HOUR FROM createdTime) AS hour,                 -- Hour (0-23)
    FORMAT_TIMESTAMP('%p', createdTime) AS AM_PM,           -- AM or PM
    CASE 
        WHEN EXTRACT(HOUR FROM createdTime) BETWEEN 5 AND 11 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM createdTime) BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN EXTRACT(HOUR FROM createdTime) BETWEEN 17 AND 20 THEN 'Evening'
        ELSE 'Night'
    END AS time_of_day,                                      -- Categorizing time
    CASE 
        WHEN EXTRACT(DAYOFWEEK FROM createdTime) IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS weekday_weekend,                                  -- Weekday or Weekend
    FORMAT_TIMESTAMP('%B', createdTime) AS month,           -- Full month name
    EXTRACT(QUARTER FROM createdTime) AS quarter            -- Quarter (1,2,3,4)
FROM`just-data-sandbox-oos.feature_engineering.raw_data`;
