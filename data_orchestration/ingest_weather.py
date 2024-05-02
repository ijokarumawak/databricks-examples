# Databricks notebook source
# MAGIC %pip install openmeteo-requests
# MAGIC %pip install requests-cache retry-requests

# COMMAND ----------

# MAGIC %run ./_include

# COMMAND ----------

locations_df = spark.table("locations").toPandas()

# COMMAND ----------

display(locations_df)

# COMMAND ----------

import openmeteo_requests

import requests_cache
import pandas as pd
from retry_requests import retry

# キャッシュとエラー時のリトライ設定を持つOpen-Meteo APIクライアントをセットアップします
cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# 必要な天気変数がすべてここにリストされていることを確認します
# 時間ごとまたは日ごとの変数の順序は、以下で正しく割り当てるために重要です
url = "https://api.open-meteo.com/v1/forecast"
params = {
    "latitude": locations_df["lat"].tolist(),
    "longitude": locations_df["lon"].tolist(),
    "hourly": ["temperature_2m", "rain"],
    "timezone": "Asia/Tokyo",
    "past_days": 1,
    "forecast_days": 1,
}
responses = openmeteo.weather_api(url, params=params)

weather_dfs = []
for i, response in enumerate(responses):
    print(f"座標 {response.Latitude()}°N {response.Longitude()}°E")
    print(f"標高 {response.Elevation()} m asl")
    print(f"タイムゾーン {response.Timezone()} {response.TimezoneAbbreviation()}")
    print(f"GMT+0とのタイムゾーンの差 {response.UtcOffsetSeconds()} 秒")

    # 時間ごとのデータを処理します。変数の順序はリクエストで要求したものと同じでなければなりません。
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_rain = hourly.Variables(1).ValuesAsNumpy()

    hourly_data = {
        "timestamp": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left",
        )
    }
    hourly_data["location"] = locations_df.iloc[i]['name']
    hourly_data["temperature_2m"] = hourly_temperature_2m
    hourly_data["rain"] = hourly_rain

    hourly_dataframe = pd.DataFrame(data=hourly_data)
    print(hourly_dataframe)
    weather_dfs.append(hourly_dataframe)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS weather (
# MAGIC   timestamp TIMESTAMP,
# MAGIC   location STRING,
# MAGIC   temperature_2m FLOAT,
# MAGIC   rain FLOAT
# MAGIC )

# COMMAND ----------

spark.createDataFrame(pd.concat(weather_dfs)).createOrReplaceTempView("new_weather")

result = spark.sql("""
MERGE INTO weather AS target
USING new_weather AS source
ON target.timestamp = source.timestamp AND target.location = source.location
WHEN MATCHED
    AND target.temperature_2m <> source.temperature_2m
    AND target.rain <> source.rain THEN
    UPDATE SET target.temperature_2m = source.temperature_2m, target.rain = source.rain
WHEN NOT MATCHED THEN
    INSERT (timestamp, location, temperature_2m, rain) VALUES (source.timestamp, source.location, source.temperature_2m, source.rain)
""")

# COMMAND ----------

display(result)
