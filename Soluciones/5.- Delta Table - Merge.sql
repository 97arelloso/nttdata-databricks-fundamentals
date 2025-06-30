-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Merge

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1.- Vamos a crear una tabla análoga a _departamentos_delta_ llamada **departamentos_delta_new**, pero vacía.
-- MAGIC
-- MAGIC [Databricks Create Table](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html)

-- COMMAND ----------

DROP TABLE nttdata_databricks_lab.schema_alejandro.departamentos_delta_new

-- COMMAND ----------

CREATE TABLE nttdata_databricks_lab.schema_alejandro.departamentos_delta_new
AS SELECT * FROM nttdata_databricks_lab.schema_alejandro.departamentos_delta
WHERE 1 = 2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2.- Ahora vamos a insertar los siguientes datos:
-- MAGIC - Filas sin cambios:
-- MAGIC   - 1 | Finanzas | 4
-- MAGIC - Filas actualizadas:
-- MAGIC   - 2 | D&A | 13
-- MAGIC   - 3 | Recursos Humanos | 2
-- MAGIC - Filas nuevas:
-- MAGIC   - 11 | Utilities | 12
-- MAGIC   - 12 | Banking | 10
-- MAGIC
-- MAGIC [Databricks Insert Into](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-dml-insert-into.html#insert-into)

-- COMMAND ----------

INSERT INTO nttdata_databricks_lab.schema_alejandro.departamentos_delta_new
(ID, DEPT_NAME, DEPT_FLOOR) 
VALUES
  (1, "Finanzas", 4), (2, "D&A", 13), (3, "Recursos Humanos", 2), (11, "Utilities", 12), (12, "Banking", 10)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3.- Supongamos que estos datos son nuevos y hay que insertarlos en la tabla original. Se puede hacer un update, un insert y un delete por separado, pero hay una herramienta adecuada para esta tarea, y se trata del **merge**.
-- MAGIC
-- MAGIC [Databricks Merge](https://docs.databricks.com/en/sql/language-manual/delta-merge-into.html)

-- COMMAND ----------

MERGE INTO nttdata_databricks_lab.schema_alejandro.departamentos_delta old
USING nttdata_databricks_lab.schema_alejandro.departamentos_delta_new new
ON old.id = new.id
WHEN MATCHED THEN
  UPDATE SET
    id = new.id,
    dept_name = new.dept_name,
    dept_floor = new.dept_floor
WHEN NOT MATCHED
  THEN INSERT (
    id,
    dept_name,
    dept_floor
  )
  VALUES (
    new.id,
    new.dept_name,
    new.dept_floor
  )
WHEN NOT MATCHED BY SOURCE THEN
  DELETE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **EJERCICIO EXTRA**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4.- Vamos a regresar la tabla _departamentos_delta_ a la versión anterior al merge, como hemos visto en el ejercicio _3.- Delta Table - Time Travel_.
-- MAGIC
-- MAGIC [Restore Version](https://docs.databricks.com/en/delta/history.html#restore-a-delta-table-to-an-earlier-state)

-- COMMAND ----------

DESCRIBE HISTORY nttdata_databricks_lab.schema_alejandro.departamentos_delta

-- COMMAND ----------

RESTORE TABLE nttdata_databricks_lab.schema_alejandro.departamentos_delta TO VERSION AS OF 12

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5.- Ahora vamos a hacer el mismo merge pero utilizando python.
-- MAGIC
-- MAGIC [Databricks Merge](https://docs.databricks.com/en/delta/merge.html)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from delta.tables import *
-- MAGIC
-- MAGIC deltaTableOld = DeltaTable.forName(spark, "nttdata_databricks_lab.schema_alejandro.departamentos_delta")
-- MAGIC deltaTableNew = DeltaTable.forName(spark, "nttdata_databricks_lab.schema_alejandro.departamentos_delta_new")
-- MAGIC dfNew = deltaTableNew.toDF()
-- MAGIC
-- MAGIC deltaTableOld \
-- MAGIC   .alias("old") \
-- MAGIC   .merge(
-- MAGIC     dfNew.alias("new"),
-- MAGIC     "old.id = new.id") \
-- MAGIC   .whenMatchedUpdate(set = 
-- MAGIC     {
-- MAGIC       "id": "new.id",
-- MAGIC       "dept_name": "new.dept_name",
-- MAGIC       "dept_floor": "new.dept_floor"
-- MAGIC     }
-- MAGIC   ) \
-- MAGIC   .whenNotMatchedInsert(values = 
-- MAGIC     {
-- MAGIC       "id": "new.id",
-- MAGIC       "dept_name": "new.dept_name",
-- MAGIC       "dept_floor": "new.dept_floor"
-- MAGIC     }
-- MAGIC   ) \
-- MAGIC   .whenNotMatchedBySourceDelete() \
-- MAGIC   .execute()
