-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Parsing JSON Data

-- COMMAND ----------

SELECT * FROM customers;

-- COMMAND ----------

SELECT customer_id, email, profile:first_name, profile:address:street AS street
FROM customers

-- COMMAND ----------

SELECT profile
FROM customers
LIMIT 1

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW parsed_customers AS
SELECT customer_id, from_json(profile,  schema_of_json('{"first_name":"Susana","last_name":"Gonnely","gender":"Female","address":{"street":"760 Express Court","city":"Obrenovac","country":"Serbia"}}')) AS profile_struct FROM customers;

SELECT * FROM parsed_customers

-- COMMAND ----------

DESCRIBE parsed_customers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Explode Function

-- COMMAND ----------

SELECT customer_id, profile_struct.first_name, profile_struct.address.country FROM parsed_customers;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customers_final AS
SELECT customer_id, profile_struct.*, profile_struct.address.* FROM parsed_customers;

SELECT * FROM customers_final

-- COMMAND ----------

SELECT order_id, customer_id, explode(books) FROM orders

-- COMMAND ----------

SELECT 
  customer_id,
  collect_set(order_id) AS order_set,
  collect_set(books.book_id) AS books_set
FROM orders
GROUP BY customer_id


-- COMMAND ----------

SELECT 
  customer_id,
  collect_set(books.book_id) AS books_set_before,
  array_distinct(flatten(collect_set(books.book_id))) AS books_set_after
FROM orders
GROUP BY customer_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Collecting Rows

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##Flatten Arrays

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##Join Operations

-- COMMAND ----------

CREATE OR REPLACE VIEW orders_enriched AS
SELECT * 
  FROM (
    SELECT *, explode(books) AS book FROM orders
  ) o
INNER JOIN
  books_delta b
ON o.book.book_id = b.book_id;
  
SELECT * FROM orders_enriched

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Set Operations

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Reshaping Data with Pivot

-- COMMAND ----------

CREATE OR REPLACE TABLE transactions AS

SELECT * 
FROM (
  SELECT
    customer_id,
    book.book_id AS book_id,
    book.quantity AS quantity
  FROM orders_enriched
)
PIVOT (
  SUM(quantity) FOR book_id IN ('B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08')
);

SELECT * FROM transactions

-- COMMAND ----------


