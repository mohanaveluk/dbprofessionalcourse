# Databricks notebook source
# MAGIC %run ../../includes/copy_dataset  

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as F


                                                   
def batch_upsert4(microBatchDF, batchId):
    window = Window.partitionBy("order_id", "customer_id").orderBy(F.col("_commit_timestamp").desc())

    (microBatchDF.filter(F.col("_change_type").isin(["insert", "update_postimage"]))
                .withColumn("rank", F.rank().over(window))
                .filter("rank = 1")
                .drop("rank", "_change_type", "_commit_version")
                .withColumnRenamed("_commit_timestamp", "processed_timestamp")
                .createOrReplaceTempView("ranked_updates")
                )

    query = """
            merge into customers_orders c
            using ranked_updates r
            on r.order_id = c.order_id and r.customer_id = c.customer_id
            when matched and c.processed_timestamp < r.processed_timestamp then
                update set *
            when not matched then
                insert *
            """
    microBatchDF.sparkSession.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table if exists customers_orders;
# MAGIC create table if not exists customers_orders
# MAGIC (
# MAGIC   order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country STRING, row_time TIMESTAMP, processed_timestamp TIMESTAMP
# MAGIC );

# COMMAND ----------

def process_customers_orders():
    orders_df = spark.readStream.table("orders_silver")

    cdf_customers_df = (spark.readStream
                        .option("readChangeData", True)
                        .option("startingVersion", 2)
                        .table("customers_silver"))
    query = (orders_df
        .join(cdf_customers_df, ["customer_id"], "inner")
        .writeStream
        .foreachBatch(batch_upsert4)
        .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/customers_orders")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()

process_customers_orders() 

# COMMAND ----------


