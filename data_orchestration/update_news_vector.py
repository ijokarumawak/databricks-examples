# Databricks notebook source
# MAGIC %pip install chromadb

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_include

# COMMAND ----------

import os
import chromadb

db_path=f"{os.getcwd()}/chroma_db"
chroma_client = chromadb.PersistentClient(path=db_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## OpenAI のエンべディングを利用
# MAGIC 2024-05-02 時点の Chroma デフォルトエンべディングモデル [all-MiniLM-L6-v2](https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2) は日本語に対応していないので、代わりに OpenAI のエンべディングを利用する。

# COMMAND ----------

import chromadb.utils.embedding_functions as embedding_functions
openai_ef = embedding_functions.OpenAIEmbeddingFunction(
                api_key=dbutils.secrets.get(secret_scope, 'openai_api_key'),
                model_name="text-embedding-3-small"
            )

# COMMAND ----------

def get_chroma_collection(name):
  collection = chroma_client.get_or_create_collection(name=name, embedding_function=openai_ef)
  return collection

# COMMAND ----------

volume_name = "news_streaming_checkpoint"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume_name};")
checkpoint_location = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"

# COMMAND ----------

import dateutil.parser as dp

# newsテーブルからストリーミングで読み込む
query = (spark.readStream.format("delta")
         .option("skipChangeCommits", "true")
         .table("news")
         .select("url", "title", "author", "publishedAt"))

collection = get_chroma_collection("news")

# Chromaコレクションにニュース記事を登録する関数
def process_batch(batch_df):
    display(batch_df)
    batch_df.toPandas().apply(lambda row: collection.add(
      documents=[row["title"]],
      metadatas=[{"publishedAt": int(dp.parse(row["publishedAt"]).strftime("%s"))}],
      ids=[row["url"]]), axis=1)

# マイクロバッチでニュース記事を処理
processed_query = (query.writeStream
                   .foreachBatch(lambda batch_df, batch_id: process_batch(batch_df))
                   .option("checkpointLocation", checkpoint_location)
                   .trigger(availableNow=True)
                   .start())

# 今回の実行で対象となるニュース記事を処理し終わるまで待つ
processed_query.awaitTermination()

# COMMAND ----------

import datetime

three_days_ago = datetime.datetime.now() - datetime.timedelta(days=3)

results = collection.query(
    query_texts=["ファッション"],
    n_results=3,
    where={"publishedAt": {"$gt": int(three_days_ago.strftime("%s"))}}
)

print(results)

[{"id": x[0], "title": x[1], "publishedAt": datetime.datetime.fromtimestamp(x[2]["publishedAt"]).isoformat(), "distance": x[3]} for x in zip(results["ids"][0], results["documents"][0], results["metadatas"][0], results["distances"][0])]

# COMMAND ----------


