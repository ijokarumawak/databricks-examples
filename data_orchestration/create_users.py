# Databricks notebook source
# MAGIC %run ./_include

# COMMAND ----------

dbutils.widgets.text("n", "10", "Number of users")

# COMMAND ----------

# TODO: csv files cannot be bundled within a dbc file
import pandas as pd
last_names = pd.read_csv("./data/most_popular_last_names.csv")
male_names = pd.read_csv("./data/most_popular_first_names_male.csv")
female_names = pd.read_csv("./data/most_popular_first_names_female.csv")
news_categories = pd.read_csv("./data/most_popular_news_categories.csv")

# COMMAND ----------

locations = [row["name"] for row in spark.table("locations").select("name").collect()]

# COMMAND ----------

import random
import datetime

def random_date_of_birth(current_age):
  current_year = datetime.datetime.now().year
  birth_year = current_year - current_age
  birth_month = random.randint(1, 12)
  if birth_month == 2:
      birth_day = random.randint(1, 28)
  elif birth_month in [4, 6, 9, 11]:
      birth_day = random.randint(1, 30)
  else:
      birth_day = random.randint(1, 31)

  return datetime.date(birth_year, birth_month, birth_day).isoformat()

# COMMAND ----------

import random
import uuid

n = int(dbutils.widgets.get("n"))
users = []
for i in range(0, n):
  gender = random.choice(["男性", "女性"])
  first_name_df = female_names if gender == "女性" else male_names
  first_r = random.randint(0, len(first_name_df) - 1)
  last_r = random.randint(0, len(last_names) - 1)
  last_name = last_names.iloc[last_r]['kanji']
  first_name = first_name_df.iloc[first_r]['kanji']
  news_r = random.randint(0, len(news_categories) - 1)
  news_category = news_categories.iloc[news_r]['kanji']
  location = random.choice(locations)
  age = random.randint(18, 60)
  users.append(["INSERT", str(uuid.uuid4()), datetime.datetime.now().isoformat(), random_date_of_birth(age), gender, last_name, first_name, location, news_category])

users_df = pd.DataFrame(users, columns=['change_type', 'id', 'timestamp', 'date_of_birth', 'gender', 'last_name', 'first_name', 'location', 'news_category'])

# COMMAND ----------

volume_name = "users_cdc"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume_name};")

# COMMAND ----------

users_df.to_csv(f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.csv", index=False)
