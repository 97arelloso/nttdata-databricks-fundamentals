-- Databricks notebook source
-- MAGIC %md
-- MAGIC # VERSIONES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1.- Cuando toca hablar de versiones toca ver el historial de cambios de la tabla. Vamos a trabajar sobre la tabla **departamentos_delta** que hemos creado antes.
-- MAGIC
-- MAGIC [Databricks Describe Table](https://docs.databricks.com/en/delta/history.html#retrieve-delta-table-history)

-- COMMAND ----------

DESCRIBE HISTORY nttdata_databricks_lab.schema_alejandro.departamentos_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # TIME TRAVEL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2.- Vamos a leer la primera inserción de la tabla.
-- MAGIC
-- MAGIC [Read Version](https://docs.databricks.com/en/delta/history.html#delta-time-travel-syntax)

-- COMMAND ----------

SELECT * FROM nttdata_databricks_lab.schema_alejandro.departamentos_delta VERSION AS OF 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3.- Ahora vamos a leer la versión tras el primer update/delete, pero vamos a leer por fecha.
-- MAGIC
-- MAGIC [Read Version](https://docs.databricks.com/en/delta/history.html#delta-time-travel-syntax)

-- COMMAND ----------

SELECT * FROM nttdata_databricks_lab.schema_alejandro.departamentos_delta TIMESTAMP AS OF '2025-06-26T11:12:19.000+00:00'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4.- Por último vamos a restaurar la tabla a la versión de la primera inserción.
-- MAGIC
-- MAGIC [Restore Version](https://docs.databricks.com/en/delta/history.html#restore-a-delta-table-to-an-earlier-state)

-- COMMAND ----------

SELECT * FROM nttdata_databricks_lab.schema_alejandro.departamentos_delta

-- COMMAND ----------

RESTORE TABLE nttdata_databricks_lab.schema_alejandro.departamentos_delta TO VERSION AS OF 1

-- COMMAND ----------

SELECT * FROM nttdata_databricks_lab.schema_alejandro.departamentos_delta
