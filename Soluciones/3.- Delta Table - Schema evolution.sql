-- Databricks notebook source
-- MAGIC %md
-- MAGIC # SCHEMA EVOLUTION

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1.- Para estos ejercicios vamos a tomar como referencia la tabla **departamentos_delta**. Veamos qué estructura tiene.
-- MAGIC
-- MAGIC [Databricks Describe Table](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-describe-table.html)

-- COMMAND ----------

DESCRIBE nttdata_databricks_lab.schema_alejandro.departamentos_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2.- Nos surge la necesidad de cambiar el nombre de la columna NAME a DEPT_NAME y FLOOR a DEPT_FLOOR.
-- MAGIC
-- MAGIC [Databricks Rename Column](https://docs.databricks.com/en/delta/column-mapping.html#rename-a-column)

-- COMMAND ----------

ALTER TABLE nttdata_databricks_lab.schema_alejandro.departamentos_delta SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')

-- COMMAND ----------

ALTER TABLE nttdata_databricks_lab.schema_alejandro.departamentos_delta RENAME COLUMN NAME TO DEPT_NAME; 
ALTER TABLE nttdata_databricks_lab.schema_alejandro.departamentos_delta RENAME COLUMN FLOOR TO DEPT_FLOOR;

-- COMMAND ----------

DESCRIBE nttdata_databricks_lab.schema_alejandro.departamentos_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3.- Se ha decidido incluir una columna nueva, así que vamos a crearla:
-- MAGIC - INSERT_DATE (STRING)
-- MAGIC
-- MAGIC [Databricks Alter Table](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-alter-table.html)

-- COMMAND ----------

ALTER TABLE nttdata_databricks_lab.schema_alejandro.departamentos_delta
ADD COLUMNS INSERT_DATE STRING

-- COMMAND ----------

DESCRIBE nttdata_databricks_lab.schema_alejandro.departamentos_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4.- Ahora nos piden que quieren eliminar la columna INSERT_DATE, que carece de utilidad.
-- MAGIC
-- MAGIC [Databricks Drop Column](https://docs.databricks.com/en/delta/column-mapping.html#drop-columns)

-- COMMAND ----------

ALTER TABLE nttdata_databricks_lab.schema_alejandro.departamentos_delta
DROP COLUMNS INSERT_DATE

-- COMMAND ----------

DESCRIBE nttdata_databricks_lab.schema_alejandro.departamentos_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # CHECK CONSTRAINTS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5.- Nos surge ahora la necesidad de meter una condición a la tabla a la hora de insertar los datos. No queremos que el piso sea mayor que 30.
-- MAGIC
-- MAGIC _Este paso va a fallar dado que hay una fila que no cumple la norma. Podemos modificarla para que cumpla la norma._
-- MAGIC
-- MAGIC [Databricks Check Constraints](https://docs.databricks.com/en/tables/constraints.html#set-a-check-constraint-in-databricks)

-- COMMAND ----------

SELECT * FROM nttdata_databricks_lab.schema_alejandro.departamentos_delta

-- COMMAND ----------

ALTER TABLE nttdata_databricks_lab.schema_alejandro.departamentos_delta 
ADD CONSTRAINT pisoMenorOIgualQue30 CHECK (DEPT_FLOOR <= 30)

-- COMMAND ----------

ALTER TABLE nttdata_databricks_lab.schema_alejandro.departamentos_delta 
ADD CONSTRAINT pisoMenorOIgualQue31 CHECK (DEPT_FLOOR <= 31)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6.- Para probar esta funcionalidad, vamos a insertar un par de filas: una que cumpla la condición y otra que no.
-- MAGIC
-- MAGIC [Databricks Insert Into](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-dml-insert-into.html#insert-into)

-- COMMAND ----------

INSERT INTO nttdata_databricks_lab.schema_alejandro.departamentos_delta VALUES (100, 'HR', 31);
INSERT INTO nttdata_databricks_lab.schema_alejandro.departamentos_delta VALUES (101, 'HR', 32);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7.- Vamos a ver las propiedades de la tabla para comprobar que esté metida la condición.

-- COMMAND ----------

SHOW TBLPROPERTIES nttdata_databricks_lab.schema_alejandro.departamentos_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # SCHEMA ENFORCEMENT

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 8.- Vamos a crear un DataFrame que tenga las siguientes columnas: nombre (string) y edad (int). Y vamos a insertar un par de filas sobre la ruta /tmp/parquet_table_**nombre**.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC columns = ["nombre", "edad"]
-- MAGIC data = [("Celia", 20), ("Paula", 47), ("Ander", 12)]
-- MAGIC df = spark.createDataFrame(data, columns)
-- MAGIC display(df)
-- MAGIC
-- MAGIC df.write.format("parquet").mode("overwrite").save("/tmp/parquet_table_alejandro")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 9.- Ahora vamos a insertar sobre esa misma tabla otro par de registros, pero las columnas van a ser: nombre (string) y apellido (string).

-- COMMAND ----------

-- MAGIC %python
-- MAGIC columns = ["nombre", "apellido"]
-- MAGIC data = [("Edu", "Garcia"), ("Maria", "Sanchez")]
-- MAGIC df = spark.createDataFrame(data, columns)
-- MAGIC display(df)
-- MAGIC
-- MAGIC df.write.format("parquet").mode("append").save("/tmp/parquet_table_alejandro")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 10.- Leemos los datos a ver con qué nos encontramos.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.format("parquet").load("/tmp/parquet_table_alejandro").show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.option("mergeSchema", "true").format("parquet").load("/tmp/parquet_table_alejandro").show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 11.- Ahora vamos a generar el mismo DataFrame que antes, pero lo vamos a insertar en una tabla delta en la ruta /tmp/delta_table_**nombre**.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/tmp/delta_table_alejandro", recurse=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC columns = ["nombre", "edad"]
-- MAGIC data = [("Celia", 20), ("Paula", 47), ("Ander", 12)]
-- MAGIC df = spark.createDataFrame(data, columns)
-- MAGIC display(df)
-- MAGIC
-- MAGIC df.write.format("delta").mode("overwrite").save("/tmp/delta_table_alejandro")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 12.- Ahora vamos a insertar sobre esa misma tabla otro par de registros, pero las columnas van a ser: nombre (string) y apellido (string).
-- MAGIC
-- MAGIC _Este paso tiene que fallar._

-- COMMAND ----------

-- MAGIC %python
-- MAGIC columns = ["nombre", "apellido"]
-- MAGIC data = [("Edu", "Garcia"), ("Maria", "Sanchez")]
-- MAGIC df = spark.createDataFrame(data, columns)
-- MAGIC display(df)
-- MAGIC
-- MAGIC df.write.format("delta").mode("append").save("/tmp/delta_table_alejandro")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 14.- Para poder escribir en la tabla necesitaremos especificarlo explícitamente, así que vamos a probar a hacerlo.
-- MAGIC
-- MAGIC [Databricks Merge Schema](https://docs.databricks.com/en/delta/update-schema.html#enable-schema-evolution)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC columns = ["nombre", "apellido"]
-- MAGIC data = [("Edu", "Garcia"), ("Maria", "Sanchez")]
-- MAGIC df = spark.createDataFrame(data, columns)
-- MAGIC display(df)
-- MAGIC
-- MAGIC df.write.option("mergeSchema", "true").format("delta").mode("append").save("/tmp/delta_table_alejandro")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 15.- Leemos los datos a ver con qué nos encontramos.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.option("mergeSchema", "true").format("delta").load("/tmp/delta_table_alejandro").show()
