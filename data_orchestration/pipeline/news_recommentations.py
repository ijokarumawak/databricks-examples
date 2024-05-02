# Databricks notebook source
# Unity Catalog に接続するには databricks distribution の mlflow が必要
%pip install mlflow[databricks]

# COMMAND ----------

# MAGIC %md
# MAGIC # DLT パイプラインから Unity Catalog のモデルを実行
# MAGIC
# MAGIC 残念ながらタイムアップ。 2024-05-07 時点では artifact の参照がうまくできなかった。
# MAGIC
# MAGIC > OSError: No such file or directory: '/local_disk0/.ephemeral_nfs/repl_tmp_data/ReplId-4d0d3-a75f9-968d8-6/mlflow/models/tmpk5brso3x/.'
# MAGIC

# COMMAND ----------

# run は DLT パイプラインではサポートされていないので注意
# %run ../_include

# COMMAND ----------

# MAGIC %md
# MAGIC # モデルをロード
# MAGIC 2024-05-07 時点では、MLflowモデルを利用する場合、Unity Catalog を有効化したパイプラインでは[プレビューチャネルの利用が必要](https://docs.databricks.com/en/delta-live-tables/transform.html#use-mlflow-models-in-a-delta-live-tables-pipeline)。
# MAGIC

# COMMAND ----------

import mlflow

mlflow.set_registry_uri("databricks-uc")

secret_scope = spark.conf.get("secret_scope")
recommend_news = mlflow.pyfunc.spark_udf(
    spark,
    model_uri=spark.conf.get("model_uri"),
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

import dlt
from pyspark.sql.functions import concat_ws, struct

@dlt.table
def user_news_recommendations():
    return (dlt.read("users").select(
        concat_ws(" ", "gender", "location", "news_category").alias("query"),
        recommend_news(struct(["query"])).alias("recommendations")))
