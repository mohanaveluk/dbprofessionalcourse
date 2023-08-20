# Databricks notebook source
# MAGIC %run ../../includes/copy_dataset

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# COMMAND ----------

schema_customer = "customer_id string, email string, first_name string, last_name string, gender string, street string, city string, country_code string,  row_status string, row_time timestamp"

def batch_upsert(microBatchDF, batchId):
    window = Window.partitionBy("customer_id").orderBy(F.col("row_time").desc())

    (microBatchDF.filter(F.col("row_status").isin(["insert", "update"]))
                .withColumn("rank", F.rank().over(window))
                .filter("rank == 1")
                .drop("rank")
                .createOrReplaceTempView("ranked_updates")
                )

    query = """
            merge into customers_silver c
            using ranked_updates r
            on r.customer_id = c.customer_id
            when matched and c.row_time < r.row_time then
                update set *
            when not matched then
                insert *
            """
    microBatchDF.sparkSession.sql(query)

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC create table if not exists customers_silver
# MAGIC (customer_id string, email string, first_name string, last_name string, gender string, street string, city string, country string,  row_status string, row_time timestamp)

# COMMAND ----------

df_country_lookup = spark.read.json(f"{dataset_bookstore}/country_lookup")

# COMMAND ----------

query = (spark.readStream.table("bronze")
                .filter("topic = 'customers'")
                .select(F.from_json(F.col("value").cast("string"), schema_customer).alias("v"))
                .select("v.*")
                .join(F.broadcast(df_country_lookup), F.col("country_code") == F.col("code"), "inner")
              .writeStream
              .foreachBatch(batch_upsert)  
              .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/customers_silver")
              .trigger(availableNow=True)
              .start()
        )

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC alter table customers_silver
# MAGIC set tblproperties (delta.enableChangeDataFeed = true);
