-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DEMO OPTIMIZE & VACUUM

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1.- Creamos una tabla llamada **prueba_optimize** que tenga la columna ID numérica.
-- MAGIC
-- MAGIC [Databricks Create Table](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html)

-- COMMAND ----------

drop table nttdata_databricks_lab.schema_alejandro.prueba_optimize

-- COMMAND ----------

CREATE TABLE nttdata_databricks_lab.schema_alejandro.prueba_optimize (
  ID INT
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2.- Escribimos en la tabla con un repartition para que escriba más de un parquet.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.range(0, 5)
-- MAGIC df.repartition(3).write.format("delta").mode("overwrite").insertInto("nttdata_databricks_lab.schema_alejandro.prueba_optimize")

-- COMMAND ----------

describe detail nttdata_databricks_lab.schema_alejandro.prueba_optimize

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3.- Ejecutamos el comando optimize que nos comprime los archivos parquet.

-- COMMAND ----------

OPTIMIZE nttdata_databricks_lab.schema_alejandro.prueba_optimize
ZORDER BY (ID)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4.- Ahora vamos a eliminar los archivos sobrantes, que ya no se utilizan. Para ello ejecutaremos el comando vacuum.
-- MAGIC
-- MAGIC _Este paso va a fallar._

-- COMMAND ----------

VACUUM nttdata_databricks_lab.schema_alejandro.prueba_optimize RETAIN 0 HOURS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5.- Cambiamos las propiedades de la tabla para quitarle la propiedad de retención de archivos.

-- COMMAND ----------

ALTER TABLE nttdata_databricks_lab.schema_alejandro.prueba_optimize SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = '0 days');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6.- Ahora sí que podemos ejecutar el vacuum para eliminar los archivos.

-- COMMAND ----------

VACUUM nttdata_databricks_lab.schema_alejandro.prueba_optimize

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7.- Para comprobar qué hemos hecho, vamos a ver el historial de cambios de la tabla, además de comprobar el bucket de S3.
-- MAGIC
-- MAGIC [Databricks Describe Table](https://docs.databricks.com/en/delta/history.html#retrieve-delta-table-history)

-- COMMAND ----------

DESCRIBE HISTORY schema_alejandro.prueba_optimize
