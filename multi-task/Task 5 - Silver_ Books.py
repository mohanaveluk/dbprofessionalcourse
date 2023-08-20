# Databricks notebook source
# MAGIC %run ../includes/copy_dataset

# COMMAND ----------

from pyspark.sql import functions as F
def type2_upsert(microBatchDF, batch):
    microBatchDF.createOrReplaceTempView("updates")
    
    sql_query = """ 
        merge into books_silver
        using (
            select updates.book_id as merge_key, updates.* from updates
            union all
            select null as merge_key, updates.* from updates
            
            join books_silver on updates.book_id = books_silver.book_id
            where books_silver.current = true and updates.price <> books_silver.price
        ) staged_updates
        on books_silver.book_id = merge_key
        when matched and books_silver.current = true and books_silver.price <> staged_updates.price then
        update set current = false, end_date = staged_updates.updated
        when not matched then
        insert (book_id, title, author, price, current, effective_date, end_date)
        values (staged_updates.book_id, staged_updates.title, staged_updates.author, staged_updates.price, true, staged_updates.updated, null)
    """ 
    microBatchDF.sparkSession.sql(sql_query)

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table if exists books_silver;
# MAGIC
# MAGIC create table if not exists books_silver
# MAGIC (
# MAGIC   book_id string, title string, author string, price double, current boolean, effective_date timestamp, end_date timestamp
# MAGIC );

# COMMAND ----------

from pyspark.sql import functions as F

def process_books():
    json_schema = "book_id string, title string, author string, price double, updated timestamp"
    #{"book_id":"B21","title":"Turing's Vision: The Birth of Computer Science","author":"Chris Bernhardt","price":35,"updated":"2021-11-14 17:12:42.335"}

    query = (
        spark.readStream
            .table("bronze")
            .filter("topic = 'books'")
            .select (F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
            .select("v.*")
        .writeStream
            .foreachBatch(type2_upsert)
            .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/books_silver")
            .trigger(availableNow=True)
            .start()
        )
    query.awaitTermination()

process_books()

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table current_books
# MAGIC as select book_id, title, author, price 
# MAGIC from books_silver
# MAGIC where current is true

# COMMAND ----------



# COMMAND ----------


