-- Databricks notebook source
-- MAGIC %md
-- MAGIC 1.- Crear la tabla "**conteos_tablas**" con formato delta sobre nuestro esquema con las siguientes columnas:
-- MAGIC - NOMBRE_TABLA (nombre de la tabla)
-- MAGIC - NUM_REGISTROS (conteo de la tabla, numérico)
-- MAGIC - COMENTARIO (comentario en caso de no haber insertado registros en la tabla)
-- MAGIC - FEC_INSERT (fecha en la que se ejecutará el insert)
-- MAGIC
-- MAGIC [Databricks Create Table](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html)

-- COMMAND ----------

DROP TABLE nttdata_databricks_lab.schema_alejandro.conteos_tablas

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS nttdata_databricks_lab.schema_alejandro.conteos_tablas 
(NOMBRE_TABLA STRING, NUM_REGISTROS INT, COMENTARIO STRING, FEC_INSERT TIMESTAMP)
USING delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2.- Rescatar las variables del notebook "6.1.- Workflows".
-- MAGIC
-- MAGIC [Share information between tasks](https://docs.databricks.com/en/jobs/share-task-context.html)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tabla = dbutils.jobs.taskValues.get(taskKey="Conteo", key="tabla", default="error", debugValue=None)
-- MAGIC conteo = dbutils.jobs.taskValues.get(taskKey="Conteo", key="conteo", default="0", debugValue=None)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3.- Insertar los datos obtenidos en la tabla "conteos_tablas".

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC from pyspark.sql.types import IntegerType
-- MAGIC
-- MAGIC columns = ["NOMBRE_TABLA", "NUM_REGISTROS", "COMENTARIO"]
-- MAGIC data = [(tabla, int(conteo), "Conteo de registros")]
-- MAGIC
-- MAGIC df = spark.createDataFrame(data, columns) \
-- MAGIC     .withColumn("FEC_INSERT", F.current_timestamp())
-- MAGIC
-- MAGIC df = df.withColumn("NUM_REGISTROS", df["NUM_REGISTROS"].cast(IntegerType()))
-- MAGIC
-- MAGIC df.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable("nttdata_databricks_lab.schema_alejandro.conteos_tablas")
