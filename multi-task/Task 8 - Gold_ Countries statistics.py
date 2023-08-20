# Databricks notebook source
# MAGIC %run ../../includes/copy_dataset  

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create view if not exists countries_stats_vw as
# MAGIC select country, 
# MAGIC date_trunc("DD", order_timestamp ) as order_date,
# MAGIC count(order_id) as order_count,
# MAGIC sum(quantity) as books_count
# MAGIC from customers_orders
# MAGIC group by country, date_trunc("DD", order_timestamp )
# MAGIC

# COMMAND ----------


