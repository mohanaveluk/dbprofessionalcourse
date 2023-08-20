# Databricks notebook source
# MAGIC %run ../includes/copy_dataset

# COMMAND ----------




# COMMAND ----------

from pyspark.sql import functions as F

json_schema = "order_id string, order_timestamp timestamp, customer_id string, quantity int, total int, books ARRAY<STRUCT<book_id string, quantity int, subtotal int>>"

deduped_df = (
    spark.readStream.table("bronze")
    .filter("topic = 'orders'")
    .select (F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
    .select("v.*")
    .withWatermark("order_timestamp", "30 seconds")
    .dropDuplicates(["order_id", "order_timestamp"]))

# COMMAND ----------

def upsert_data(microBatchDf, batch):
    microBatchDf.createOrReplaceTempView("orders_microbatch")
    
    
    sql_query = """
        merge into orders_silver a
        using orders_microbatch mb
        on a.order_id = mb.order_id and a.order_timestamp = mb.order_timestamp
        WHEN not matched then insert *
    """
    microBatchDf.sparkSession.sql(sql_query)

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table if exists orders_silver;
# MAGIC create table if not exists orders_silver (
# MAGIC   order_id string, order_timestamp timestamp, customer_id string, quantity int, total int, books ARRAY<STRUCT<book_id string, quantity int, subtotal int>>
# MAGIC )

# COMMAND ----------

query = (deduped_df
         .writeStream
         .foreachBatch(upsert_data)
         .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")
         .trigger(availableNow=True)
         .start()
         )
query.awaitTermination()
