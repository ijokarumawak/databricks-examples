# Databricks notebook source
# 利用するカタログ名、スキーマ名を指定
catalog_name = "default"
schema_name = "databricks_examples"

# COMMAND ----------

sql = f"USE {catalog_name}.{schema_name};"
spark.sql(sql)

# COMMAND ----------

current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
secret_scope = f"{current_user}_data_orchestration_example"
