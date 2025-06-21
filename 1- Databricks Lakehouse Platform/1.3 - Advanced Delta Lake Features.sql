-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Delta Time Travel

-- COMMAND ----------

USE CATALOG hive_metastore

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

SELECT
  *
FROM
  employees
VERSION AS OF 4

-- COMMAND ----------

SELECT
  *
FROM
  employees@v5

-- COMMAND ----------

TRUNCATE TABLE employees

-- COMMAND ----------

SELECT
  *
FROM
  employees

-- COMMAND ----------

RESTORE TABLE employees TO VERSION AS OF 5

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## OPTIMIZE Command

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

OPTIMIZE employees
ZORDER BY id

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/user/hive/warehouse/employees/'))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.head('dbfs:/user/hive/warehouse/employees/_delta_log/00000000000000000012.json'))

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## VACUUM Command

-- COMMAND ----------

VACUUM employees

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/user/hive/warehouse/employees'))

-- COMMAND ----------

VACUUM employees RETAIN 0 HOURS

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled=false

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/user/hive/warehouse/employees'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Dropping Tables

-- COMMAND ----------

DROP TABLE employees

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/user/hive/warehouse/employees'))

-- COMMAND ----------


