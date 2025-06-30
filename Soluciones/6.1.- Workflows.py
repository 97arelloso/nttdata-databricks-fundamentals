# Databricks notebook source
# MAGIC %md
# MAGIC 1.- Este notebook se ejecutará en el Workflow. Desde el workflow le pasaremos hardcodeada la variable "schema_**nombre**.departamentos_delta", sobre la cual ejecutaremos un conteo.
# MAGIC
# MAGIC [Get task parameter](https://docs.databricks.com/en/dev-tools/databricks-utils.html#widgets-utility-dbutilswidgets)

# COMMAND ----------

tabla = dbutils.widgets.get("args")
print(tabla)
conteo = spark.sql(f"select count(*) from {tabla}")

# COMMAND ----------

# MAGIC %md
# MAGIC 2.- Ahora vamos a guardar ese resultado en una variable para pasársela a otra actividad/task. También vamos a pasar el nombre de la tabla.
# MAGIC
# MAGIC [Share information between tasks](https://docs.databricks.com/en/jobs/share-task-context.html)

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "tabla", value = tabla)
dbutils.jobs.taskValues.set(key = "conteo", value = conteo.first()[0])
