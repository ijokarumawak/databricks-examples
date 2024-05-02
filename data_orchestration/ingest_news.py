# Databricks notebook source
# MAGIC %run ./_include

# COMMAND ----------

import requests

url = f"https://newsapi.org/v2/top-headlines?country=jp&apiKey={dbutils.secrets.get(secret_scope, 'news_api_key')}"

response = requests.get(url)

print(response.json())

# COMMAND ----------

import pandas as pd

# レスポンスJSONから記事を抽出する
articles = response.json().get('articles', [])

# 記事をpandas DataFrameに変換する
articles_df = pd.DataFrame(articles)

# COMMAND ----------

display(articles_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS news (
# MAGIC     source struct<id: STRING, name: STRING>,
# MAGIC     author STRING,
# MAGIC     title STRING,
# MAGIC     description STRING,
# MAGIC     url STRING,
# MAGIC     urlToImage STRING,
# MAGIC     publishedAt STRING,
# MAGIC     content STRING
# MAGIC )

# COMMAND ----------

spark.createDataFrame(articles_df).createOrReplaceTempView("new_news")

result = spark.sql("""
MERGE INTO news AS target
USING new_news AS source
ON target.url = source.url
WHEN NOT MATCHED THEN
    INSERT (source, author, title, description, url, urlToImage, publishedAt, content) VALUES (source.source, source.author, source.title, source.description, source.url, source.urlToImage, source.publishedAt, source.content)
""")

display(result)
dbutils.jobs.taskValues.set("num_inserted_rows", result.head()["num_inserted_rows"])
