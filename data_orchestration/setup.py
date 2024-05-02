# Databricks notebook source
# MAGIC %run ./_include

# COMMAND ----------

import pandas as pd

locations = [
  ["札幌", 43.06, 141.35],
  ["東京", 35.6895, 139.69],
  ["横浜", 35.4333, 139.65],
  ["大阪", 34.6931, 135.502],
  ["福岡", 33.5908, 130.45],
  ["沖縄", 26.2042, 127.685]
]
locations_df = pd.DataFrame(locations, columns=["name", "lat", "lon"])

# Convert pandas dataframe to Spark dataframe
locations_spark_df = spark.createDataFrame(locations_df)

# Create or replace a table named 'locations'
locations_spark_df.write.mode('overwrite').saveAsTable('locations')

# COMMAND ----------

# MAGIC %md
# MAGIC ## シークレットスコープの作成
# MAGIC 次のセルを実行して表示された `databricks secrets` コマンドをコピーします。
# MAGIC Web terminal を開き、コマンドをペーストしてシークレットスコープを作成します。

# COMMAND ----------

html = f"""
<p>シークレットスコープ作成コマンド:</p>
<pre>databricks secrets create-scope {secret_scope}</pre>
"""

dbutils.displayHTML(html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## シークレットの登録
# MAGIC 次のセルを実行して表示された `databricks secrets` コマンドをコピーします。
# MAGIC Web terminal を開き、コマンドをペーストしてシークレットを登録します。

# COMMAND ----------

html = f"""
<p>シークレット登録コマンド:</p>
<pre>databricks secrets put-secret {secret_scope} news_api_key</pre>
<pre>databricks secrets put-secret {secret_scope} openai_api_key</pre>
"""

dbutils.displayHTML(html)

# COMMAND ----------

for key in ["news_api_key", "openai_api_key"]:
  assert dbutils.secrets.get(secret_scope, key) and len(dbutils.secrets.get(secret_scope, key)) > 0, f"{key} シークレットが登録されていません"

print("シークレットの確認が正常に完了しました！")
