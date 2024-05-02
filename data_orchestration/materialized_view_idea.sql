-- Databricks notebook source
-- MAGIC %run ./_include

-- COMMAND ----------

SELECT * FROM weather

-- COMMAND ----------

SELECT * FROM users

-- COMMAND ----------

WITH titles AS (
  SELECT explode(flatten(from_json(result:documents, 'array<array<string>>'))) AS title 
  FROM news_recommendations
)
SELECT title, count(*) AS count FROM titles
GROUP BY title
ORDER BY count DESC

-- COMMAND ----------

SELECT convert_timezone("UTC", "Asia/Tokyo", CURRENT_TIMESTAMP()) AS current_date_japan;

-- COMMAND ----------

SELECT location, count(*) user_count FROM users
GROUP BY location

-- COMMAND ----------


WITH user_locations AS (
  SELECT location, count(*) user_count FROM users
  GROUP BY location
),
daily_weather AS (
  SELECT
  cast(convert_timezone("UTC", "Asia/Tokyo", timestamp) as DATE) as DATE,
  location, temperature_2m as temp, rain FROM weather
),
daily_weather_stats AS (
  SELECT max(date) as date, location, min(temp) as temp_min, max(temp) as temp_max, round(avg(temp), 2) as temp_avg FROM daily_weather
  WHERE date = cast(convert_timezone("UTC", "Asia/Tokyo", current_date()) as DATE)
  GROUP BY location
)
SELECT w.*, u.user_count FROM daily_weather_stats w
LEFT JOIN user_locations u ON w.location = u.location
