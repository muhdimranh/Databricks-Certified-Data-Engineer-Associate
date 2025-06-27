# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exploring The Source dDirectory

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

dbutils.fs.rm(f"{dataset_bookstore}/orders-raw/05.parquet")
dbutils.fs.rm(f"dbfs:/checkpoint/orders_silver", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Auto Loader

# COMMAND ----------

(spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", "dbfs:/checkpoint/orders")
            .load(f"{dataset_bookstore}/orders-raw")
            .createOrReplaceTempView("orders_raw_temp")
                )

# COMMAND ----------

df = spark.read.table("orders_raw_temp")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Enriching Raw Data

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name

orders_raw_temp = spark.read.table("orders_raw_temp")
(orders_raw_temp
 .withColumn("arrival_time", current_timestamp())
 .withColumn("source_file", input_file_name())
 .createOrReplaceTempView("orders_tmp")
)

# COMMAND ----------

orders_tmp_view = spark.read.table("orders_tmp")
display(orders_tmp_view)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Bronze Table

# COMMAND ----------

(spark.read
    .table("orders_tmp")
    .writeStream
    .format("delta")
    .option("checkpointLocation", "dbfs:/checkpoint/orders_bronze")
    .outputMode("append")
    .table("orders_bronze"))



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM order_bronze

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Creating Static Lookup Table

# COMMAND ----------

(spark.read
        .json(f"{dataset_bookstore}/customers-json")
        .createOrReplaceTempView("customers_lookup"))

# COMMAND ----------

df = spark.table("customers_lookup")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Silver Table

# COMMAND ----------

(spark.readStream
        .table("orders_bronze")
        .createOrReplaceTempView("orders_bronze_tmp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_enriched_tmp AS (
# MAGIC   SELECT
# MAGIC     order_id
# MAGIC     ,o.customer_id
# MAGIC     ,quantity
# MAGIC     ,c.profile:first_name as f_name
# MAGIC     ,c.profile:last_name as l_name
# MAGIC     ,cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) as order_timestamp
# MAGIC     ,books
# MAGIC   FROM orders_bronze_tmp o
# MAGIC   INNER JOIN customers_lookup c
# MAGIC   ON o.customer_id = c.customer_id
# MAGIC   WHERE quantity > 0
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_enriched_tmp

# COMMAND ----------

# from pyspark.sql.functions import col
# orders_bronze_tmp = spark.read.table("orders_bronze_tmp")

# orders_bronze_tmp = (orders_bronze_tmp
#                      .select("order_id", "quantity", col("profile.last_name").alias("l_name"))
#                      .join("customers_lookup", on = "customer_id", how="inner")
#                      .where("quantity>0")
#                     )

# display(orders_bronze_tmp)

# COMMAND ----------

(spark.read.table("orders_enriched_tmp")
            .writeStream
            .format("delta")
            .option("checkpointLocation", "dbfs:/checkpoint/orders_silver")
            .option("mergeSchema", "true")
            .outputMode("append")
            .table("orders_silver")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM orders_silver

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Gold Table

# COMMAND ----------

(spark.readStream
  .table("orders_silver")
  .createOrReplaceTempView("orders_silver_tmp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW daily_customer_books_tmp AS (
# MAGIC   SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
# MAGIC   FROM orders_silver_tmp
# MAGIC   GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
# MAGIC   )

# COMMAND ----------

(spark.read.table("daily_customer_books_tmp")
            .writeStream
            .format("delta")
            .option("checkpointLocation", "dbfs:/checkpoint/orders_gold")
            .outputMode("complete")
            .trigger(availableNow=True)
            .table("daily_customer_books"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM daily_customer_books

# COMMAND ----------

load_new_data(all=True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Stopping active streams

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping stream: " + s.id)
    s.stop()
    s.awaitTermination()

# COMMAND ----------


