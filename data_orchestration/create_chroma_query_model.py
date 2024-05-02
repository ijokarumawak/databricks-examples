# Databricks notebook source
# MAGIC %md
# MAGIC Reference:
# MAGIC https://mlflow.org/docs/latest/llms/custom-pyfunc-for-llms/notebooks/custom-pyfunc-advanced-llm.html

# COMMAND ----------

# MAGIC %pip install chromadb

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_include

# COMMAND ----------

import numpy as np
import pandas as pd

import mlflow
from mlflow.models.signature import ModelSignature
from mlflow.types import ColSpec, DataType, ParamSchema, ParamSpec, Schema

mlflow.set_registry_uri("databricks-uc")

input_schema = Schema(
    [
        ColSpec(DataType.string, "query"),
    ]
)
output_schema = Schema([ColSpec(DataType.string, "result")])

parameters = ParamSchema(
    [
        ParamSpec("collection_name", DataType.string, None),
        ParamSpec("n", DataType.integer, 3),
        ParamSpec("n_days_ago", DataType.integer, 3),
        ParamSpec("openai_api_key", DataType.string, None)
    ]
)

signature = ModelSignature(inputs=input_schema, outputs=output_schema, params=parameters)

input_example = pd.DataFrame({"query": ["What is the benefit of using a vector database?"]})

# COMMAND ----------

import json
import datetime
import chromadb
import chromadb.utils.embedding_functions as embedding_functions

class QueryChroma(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        self.chroma_client = chromadb.PersistentClient(path=context.artifacts["database"])


    def predict(self, context, model_input, params=None):
        openai_ef = embedding_functions.OpenAIEmbeddingFunction(
                        api_key=params["openai_api_key"],
                        model_name="text-embedding-3-small"
                    )
        self.collection = self.chroma_client.get_or_create_collection(params["collection_name"], embedding_function=openai_ef)

        def query(q):
            n_days_ago = datetime.datetime.now() - datetime.timedelta(days=params["n_days_ago"])
            results = self.collection.query(
                query_texts=q,
                n_results=params["n"],
                where={"publishedAt": {"$gt": int(n_days_ago.strftime("%s"))}}
            )

            result = [{"id": x[0], "title": x[1], "publishedAt": datetime.datetime.fromtimestamp(x[2]["publishedAt"]).isoformat(), "distance": x[3]} for x in zip(results["ids"][0], results["documents"][0], results["metadatas"][0], results["distances"][0])]

            return json.dumps(result, ensure_ascii=False)

        return model_input.applymap(query)

# COMMAND ----------

import os

model_name = "query_chroma"
with mlflow.start_run():
    model_info = mlflow.pyfunc.log_model(
        model_name,
        python_model=QueryChroma(),
        artifacts={"database": "chroma_db"},
        extra_pip_requirements=["chromadb"],
        input_example=input_example,
        signature=signature,
        registered_model_name=model_name
    )

    # Chromaデータベースファイルをアーティファクトに保存
    mlflow.log_artifacts(f"{os.getcwd()}/chroma_db", "chroma_db")

# COMMAND ----------

from mlflow.tracking.client import MlflowClient
mlflow_client = MlflowClient()

def get_latest_model_version(model_name):
  model_version_infos = mlflow_client.search_model_versions("name = '%s'" % model_name)
  return max([int(model_version_info.version) for model_version_info in model_version_infos])

# COMMAND ----------

model_fq_name = f"{catalog_name}.{schema_name}.{model_name}"
latest_version = get_latest_model_version(model_fq_name)
mlflow_client.set_registered_model_alias(model_fq_name, "champion", latest_version)

# COMMAND ----------

# MAGIC %md
# MAGIC モデルを利用

# COMMAND ----------

loaded_model = mlflow.pyfunc.spark_udf(
    spark,
    model_uri=f"models:/{model_name}@champion",
    result_type="string",
    params={
        "collection_name": "news",
        "n": 3,
        "n_days_ago": 2,
        "openai_api_key": dbutils.secrets.get(secret_scope, 'openai_api_key')
    }
)

# COMMAND ----------

from pyspark.sql.functions import struct, col

# Generating sample Spark Dataframe with one row and a column 'value'
df = spark.createDataFrame([("ファッション",), ("芸能",)], ["query"])

# Applying the model to make predictions
df = df.withColumn('result', loaded_model(struct(*map(col, df.columns))))
display(df)
