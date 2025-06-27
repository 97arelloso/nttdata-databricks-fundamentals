-- Databricks notebook source
-- MAGIC %md
-- MAGIC 1.- Para comenzar, vamos a crear un esquema propio (_schema_nombre_). Sobre este esquema vamos a realizar todos los ejercicios y, así, no se pisarán las tablas entre esquemas. Luego, decirle a databricks que use el esquema que acabamos de crear. De este modo, aunque se nos olvide escribir el esquema antes de la tabla, databricks sabrá que nos referimos a nuestro esquema.
-- MAGIC
-- MAGIC [Databricks Create Schema](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-schema.html)
-- MAGIC
-- MAGIC [Databricks Use Schema](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-use-schema.html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2.- Crear la tabla "**departamentos_delta**" con formato delta sobre nuestro esquema con las siguientes columnas:
-- MAGIC - ID (identificador único, numérico)
-- MAGIC - NAME (nombre del departamento)
-- MAGIC - FLOOR (piso en el que se encuentra el departamento, numérico)
-- MAGIC
-- MAGIC [Databricks Create Table](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3.- Insertar los siguientes departamentos a la tabla generada en el paso anterior:
-- MAGIC - 1 | Finanzas | 4
-- MAGIC - 2 | D&A | 23
-- MAGIC - 3 | RRHH | 2
-- MAGIC - 4 | Cafetería | 18
-- MAGIC - 5 | Ciberseguridad | 31
-- MAGIC
-- MAGIC [Databricks Insert Into](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-dml-insert-into.html#insert-into)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4.- Identificar la tabla ya creada "**schema_alejandro.departamentos_external**" con formato parquet, apuntando a "s3://databricks-workspace-stack-be532-bucket/tablas_externas/departamentos_parquet" con las siguientes columnas:
-- MAGIC - ID (identificador único, numérico)
-- MAGIC - NAME (nombre del departamento)
-- MAGIC - FLOOR (piso en el que se encuentra el departamento, numérico)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5.- Insertar los siguientes departamentos a la tabla generada en el paso anterior:
-- MAGIC - Finanzas | 4
-- MAGIC - D&A | 23
-- MAGIC - RRHH | 2
-- MAGIC - Cafetería | 18
-- MAGIC - Ciberseguridad | 31
-- MAGIC
-- MAGIC [Databricks Insert Into](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-dml-insert-into.html#insert-into)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6.- Ahora vamos a intentar actualizar la tabla departamentos_delta de vuestro esquema para que elimine/actualice la fila de ID 4.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7.- Ahora hacer lo mismo que en el paso anterior pero para  la tabla schema_alejandro.departamentos_external.
-- MAGIC
-- MAGIC _Este paso va a fallar._

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > Tanto eliminar como actualizar registros de una tabla externa no está permitido, sin embargo, una tabla delta sí que permite este tipo de operaciones.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 8.- Vamos a ver qué información nos proporcionan cada una de las tablas.
-- MAGIC
-- MAGIC [Databricks Describe Table](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-describe-table.html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 9.- Ahora, con la ruta sacada en el paso anterior, se va a sacar un listado de los archivos que hay en esa ruta.
-- MAGIC
-- MAGIC _Al intentar acceder a los archivos de la tabla managed va a fallar._
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 10.- Ahora vamos a sacar el historial de cambios de la tabla.
-- MAGIC
-- MAGIC [Databricks Describe History](https://www.databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 11.- Ya que no podemos sacar un listado de archivos, podemos ver qué ha pasado con los archivos parquet si entramos en la ruta del s3.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 12.- Para comprobar esto, vamos a leer el Transaction Log. 
