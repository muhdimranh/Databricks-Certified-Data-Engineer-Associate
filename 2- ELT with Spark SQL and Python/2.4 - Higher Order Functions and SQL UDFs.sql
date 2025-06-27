-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Filtering Arrays

-- COMMAND ----------

SELECT
  order_id,
  books,
  FILTER(books, i -> i.quantity >= 2) AS multiple_copies
FROM orders

-- COMMAND ----------

WITH cte AS (
  SELECT
    order_id,
    books,
    FILTER(books, i -> i.quantity >= 2) AS multiple_copies
  FROM orders
)

SELECT
  order_id
  ,multiple_copies
FROM cte
WHERE size(multiple_copies) > 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Transforming Arrays

-- COMMAND ----------

WITH cte AS (
SELECT
  order_id
  ,books
  ,TRANSFORM(
    books
    ,b -> CAST(b.subtotal * 0.8 AS INT) 
  ) subtotal_after_discount
FROM orders
)

SELECT
  *
  ,explode(subtotal_after_discount)
FROM cte

-- COMMAND ----------

WITH cte AS (
  SELECT
    *
    ,TRANSFORM (
      books
      ,b -> CAST(b.subtotal AS INT)
    ) AS casted
  FROM orders
)

SELECT
  customer_id
  ,books
  ,TRANSFORM (
    casted
    ,c -> c * 0.8
  ) AS subtotal_after_discount
FROM cte


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## User Defined Functions (UDF)

-- COMMAND ----------

CREATE OR REPLACE FUNCTION get_url (email STRING)
RETURNS STRING

RETURN concat("https://www.", split(email, "@")[1])

-- COMMAND ----------

SELECT
  email
  ,get_url(email) AS domain
FROM customers

-- COMMAND ----------

DESCRIBE FUNCTION get_url

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED get_url

-- COMMAND ----------

CREATE OR REPLACE FUNCTION site_type(email STRING)
RETURNS STRING

RETURN
  CASE
    WHEN get_url(email) LIKE "%.com" THEN "Commercial Business"
    WHEN get_url(email) LIKE "%.org" THEN "Non-profit Organization"
    WHEN get_url(email) LIKE "%.edu" THEN "Educational Institution"
    ELSE concat("Unknown extention for domain: ", split(email, "@")[1])
  END;
  

-- COMMAND ----------

SELECT
  email
  ,site_type(email) AS site_type
FROM customers

-- COMMAND ----------

DROP FUNCTION get_url;
DROP FUNCTION site_type

-- COMMAND ----------


