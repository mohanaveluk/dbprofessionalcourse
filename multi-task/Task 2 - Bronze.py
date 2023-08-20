# Databricks notebook source
# MAGIC %run ../../includes/copy_dataset

# COMMAND ----------

from pyspark.sql import functions as F

def process_bronze():
    schema = "key binary, value  binary, topic  string, partition long,  offset long, timestamp long"

    query = (
                spark.readStream.format("cloudFiles")
                    .option("cloudFiles.format", "json")
                    .schema(schema)
                    .load(f"{dataset_bookstore}/kafka-raw")
                    .withColumn("timestamp", (F.col("timestamp")/1000).cast("timestamp"))
                    .withColumn("year_month", F.date_format("timestamp", "yyyy-MM"))
                
                .writeStream
                    .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/bonze")
                    .option("mergeSchema", True)
                    .partitionBy("topic", "year_month")
                    .trigger(availableNow=True)
                    .table("bronze")
            )
    query.awaitTermination()

# COMMAND ----------

process_bronze()
