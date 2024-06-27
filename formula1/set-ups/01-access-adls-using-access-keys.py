# Databricks notebook source
# spark.conf.set(
#     "fs.azure.account.key.invoicesystem.dfs.core.windows.net", #"G3oEPO2OyzIxtBkbEEE9ODf7I91nBveRRwXv2EVmJdpywVZ9pOU+SulPoTzizrYV5bKnQchrFkR+AStWAh2xQ==")

# COMMAND ----------

spark.conf.set("fs.azure.account.key.invoicesystem.dfs.core.windows.net", dbutils.secrets.get(scope='invoice-scope', key='invoice-account-key'))


# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@invoicesystem.dfs.core.windows.net"))


# COMMAND ----------

display(spark.read.csv("abfss://demo@invoicesystem.dfs.core.windows.net/circuits.csv"))
