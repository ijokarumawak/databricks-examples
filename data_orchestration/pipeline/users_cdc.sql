-- Databricks notebook source
-- MAGIC %md
-- MAGIC # ユーザ更新データファイルを増分的に読み込み
-- MAGIC Change Data Capture
-- MAGIC
-- MAGIC https://docs.databricks.com/en/delta-live-tables/sql-ref.html

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE users_cdc_raw
AS SELECT * FROM cloud_files("${source}", "csv")

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE users_cdc_clean
(CONSTRAINT valid_change_type EXPECT (
  change_type IN ("INSERT", "UPDATE", "DELETE")
) ON VIOLATION DROP ROW)
AS SELECT
  change_type,
  id,
  cast(timestamp AS TIMESTAMP),
  cast(date_of_birth AS DATE),
  gender,
  last_name,
  first_name,
  location,
  news_category
  FROM STREAM(LIVE.users_cdc_raw)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE users;

APPLY CHANGES INTO LIVE.users
FROM STREAM(LIVE.users_cdc_clean)
KEYS (id)
APPLY AS DELETE WHEN change_type = "DELETE"
SEQUENCE BY timestamp
COLUMNS * EXCEPT (change_type);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE users_cdc_anonymized
AS SELECT
change_type, id, timestamp,
floor((year(current_date()) - YEAR(date_of_birth)) / 10) * 10 as generations,
gender, location, news_category
FROM STREAM(LIVE.users_cdc_clean)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE users_anonymized;

APPLY CHANGES INTO LIVE.users_anonymized
FROM STREAM(LIVE.users_cdc_anonymized)
KEYS (id)
APPLY AS DELETE WHEN change_type = "DELETE"
SEQUENCE BY timestamp
COLUMNS * EXCEPT (change_type)
STORED AS SCD TYPE 2;
