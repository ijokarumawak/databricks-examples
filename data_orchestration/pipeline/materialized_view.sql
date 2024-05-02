-- Databricks notebook source
CREATE LIVE VIEW user_locations AS
SELECT location, count(*) user_count FROM LIVE.users_anonymized
WHERE __END_AT is null
GROUP BY location

-- COMMAND ----------

CREATE LIVE VIEW daily_weather AS
SELECT
cast(convert_timezone("UTC", "Asia/Tokyo", timestamp) as DATE) as DATE,
location, temperature_2m as temp, rain FROM ${source_schema}.weather

-- COMMAND ----------

CREATE LIVE VIEW daily_weather_stats AS
SELECT max(date) as date, location, min(temp) as temp_min, max(temp) as temp_max, round(avg(temp), 2) as temp_avg FROM LIVE.daily_weather
WHERE date = cast(convert_timezone("UTC", "Asia/Tokyo", current_date()) as DATE)
GROUP BY location

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE daily_stats AS
SELECT w.*, u.user_count FROM LIVE.daily_weather_stats w
LEFT JOIN LIVE.user_locations u ON w.location = u.location
