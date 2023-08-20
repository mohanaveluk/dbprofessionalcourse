# Databricks notebook source
# MAGIC %run ../../includes/copy_dataset  

# COMMAND ----------

#create gold tables
#dbutils.ls.rm("dbfs:/mnt/demo_pro/checkpoints/authors_stats", True)

from pyspark.sql import functions as F

query = (spark.readStream
            .table("books_sales")
            .withWatermark("order_timestamp", "10 minutes")
            .groupBy(F.window("order_timestamp", "5 minutes").alias("time"), "author")
            .agg(F.count("order_id").alias("orders_count"), F.avg("quantity").alias("avg_quantity"))
         .writeStream
            .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/authors_stats")
            .trigger(availableNow=True)
            .table("authors_stats")
)

query.awaitTermination()
