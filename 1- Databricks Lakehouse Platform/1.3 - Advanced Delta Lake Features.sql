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

USE CATALOG hive_metastore

-- COMMAND ----------

DESCRIBE DETAIL demo.employees

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('abfss://unity-catalog-storage@dbstoragegaxfo5jgyngc2.dfs.core.windows.net/3697186193429978/__unitystorage/schemas/c600fa8d-d29d-4e98-b89a-3cdf687f952a/tables/dbfabb23-21fb-4079-9821-4fcf8e87039c'))

-- COMMAND ----------

CREATE TABLE test
(id INT, name STRING)
LOCATION 'dbfs:/new-location/'

-- COMMAND ----------

CREATE TABLE test_2
(id INT, name STRING)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('/new-location'))

-- COMMAND ----------

DESCRIBE HISTORY test

-- COMMAND ----------

INSERT INTO test VALUES
(1, "Imran"),
(2, "Ronaldo")

-- COMMAND ----------

INSERT INTO test_2 VALUES
(1, "Imran"),
(2, "Ronaldo")

-- COMMAND ----------

DESCRIBE DETAIL test_2

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('/new-location'))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('/user/hive/warehouse/test_2'))

-- COMMAND ----------

DROP TABLE test

-- COMMAND ----------

SELECT * FROM test

-- COMMAND ----------

DESCRIBE EXTENDED test

-- COMMAND ----------


