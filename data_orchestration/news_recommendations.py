# Databricks notebook source
# MAGIC %pip install chromadb

# COMMAND ----------

# MAGIC %run ./_include

# COMMAND ----------

import mlflow

mlflow.set_registry_uri("databricks-uc")

model_uri = "models:/query_chroma@champion"

recommend_news = mlflow.pyfunc.spark_udf(
    spark,
    model_uri=model_uri,
    result_type="string",
    params={
        "collection_name": "news",
        "n": 3,
        "openai_api_key": dbutils.secrets.get(secret_scope, 'openai_api_key')
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ニュースのレコメンド
# MAGIC 性別、ロケーション、ニュースカテゴリからニュースをおすすめする。

# COMMAND ----------

from pyspark.sql.functions import concat, concat_ws, struct, col, lit

df = (spark.table("users_anonymized").select(
        col("id").alias("user_id"),
        concat_ws(" ", concat("generations", lit("代")), "gender", "location", "news_category").alias("query"),
        recommend_news(col("query")).alias("result")
    ))

display(df)

# COMMAND ----------

df.write.saveAsTable("news_recommendations", mode="overwrite")
