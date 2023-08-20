# Databricks notebook source
# MAGIC %run ../../includes/copy_dataset

# COMMAND ----------

spark.conf.set("spark.databricks.delta.optimizeWrite", True)
spark.conf.set("spark.databricks.delta.autoCompact", True)

# COMMAND ----------

dbutils.widgets.text("number_of_files", "1")
num_files = int(dbutils.widgets.get("number_of_files"))

bookstore.load_new_data(num_files)
