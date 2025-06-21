-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run
-- MAGIC ../Includes/Copy-Datasets

-- COMMAND ----------

CREATE TABLE orders AS
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Overwriting Tables

-- COMMAND ----------

CREATE OR REPLACE TABLE orders AS
SELECT * FROM parquet.`${dataset.bookstore}/orders`;
SELECT * FROM orders

-- COMMAND ----------

DESCRIBE HISTORY orders

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Appending Data

-- COMMAND ----------

INSERT INTO orders
SELECT * FROM parquet.`${dataset.bookstore}/orders-new`;
SELECT * FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Merging Data

-- COMMAND ----------

CREATE TABLE customers AS
SELECT * FROM json.`${dataset.bookstore}/customers-json`;

SELECT * FROM customers

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW temp_view_customer_update
AS SELECT * FROM json.`${dataset.bookstore}/customers-json-new`;

SELECT * FROM temp_view_customer_update

-- COMMAND ----------

MERGE INTO customers c
USING temp_view_customer_update u
ON c.customer_id = u.customer_id
WHEN
  MATCHED AND c.email IS NULL AND u.email IS NOT NULL 
  THEN UPDATE SET c.email = u.email, c.updated = u.updated
WHEN
  NOT MATCHED THEN INSERT *

-- COMMAND ----------

SELECT * FROM customers

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW books_update (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING csv
OPTIONS (
  path = "${dataset.bookstore}/books-csv-new",
  header = "true",
  delimiter = ";"
);

-- COMMAND ----------

SELECT * FROM books_delta

-- COMMAND ----------

SELECT * FROM books_update

-- COMMAND ----------

MERGE INTO books_delta b
USING books_update u
ON b.book_id = u.book_id AND b.title = u.title
WHEN MATCHED AND b.title != u.title
  THEN UPDATE SET *
WHEN NOT MATCHED AND u.category = "Computer Science"
  THEN INSERT *


-- COMMAND ----------

SELECT * FROM books_delta

-- COMMAND ----------


