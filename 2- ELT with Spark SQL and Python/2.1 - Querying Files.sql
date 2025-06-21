-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying JSON 

-- COMMAND ----------

-- MAGIC %run
-- MAGIC ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

SELECT *, input_file_name() AS source_file FROM json.`${dataset.bookstore}/customers-json/*.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying text Format

-- COMMAND ----------

SELECT *, input_file_name() AS source_file FROM text.`${dataset.bookstore}/customers-json/*.json`

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Querying binaryFile Format

-- COMMAND ----------

SELECT *, input_file_name() AS source_file FROM binaryFile.`${dataset.bookstore}/customers-json/*.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Querying CSV 

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS books_csv
  (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  header= "true",
  delimiter = ";",
  encoding = "utf-8-bin"
)
LOCATION '${dataset.bookstore}/books-csv'

-- COMMAND ----------

SELECT * FROM books_csv

-- COMMAND ----------

DESCRIBE EXTENDED books_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Limitations of Non-Delta Tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.read.table("books_csv")
-- MAGIC     .write
-- MAGIC     .mode("append")
-- MAGIC     .format("csv")
-- MAGIC     .option("header", "true")
-- MAGIC     .option("delimiter", ";")
-- MAGIC     .save(f"{dataset_bookstore}/books-csv"))

-- COMMAND ----------

SELECT * FROM books_csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{dataset_bookstore}/books-csv"))

-- COMMAND ----------

REFRESH TABLE books_csv

-- COMMAND ----------

SELECT * FROM books_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CTAS Statements

-- COMMAND ----------

CREATE TABLE books_delta AS
SELECT * FROM books_csv

-- COMMAND ----------

SELECT * FROM books_delta

-- COMMAND ----------

DESCRIBE EXTENDED books_delta

-- COMMAND ----------


