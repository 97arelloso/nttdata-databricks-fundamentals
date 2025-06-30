-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Genie

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creamos un esquema para almacenar los datos.

-- COMMAND ----------

CREATE SCHEMA nttdata_databricks_lab.airbnb_madrid

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Vamos a tener un volumen en el que carguemos los datos de los airbnb de Madrid.
-- MAGIC
-- MAGIC /Volumes/nttdata_databricks_lab/airbnb_madrid/airbnb_madrid

-- COMMAND ----------

CREATE VOLUME nttdata_databricks_lab.airbnb_madrid.airbnb_madrid

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creamos una carpeta dentro del volumen para almacenar los datos.

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC mkdir /Volumes/nttdata_databricks_lab/airbnb_madrid/airbnb_madrid/listing

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Descargamos los datos de muestra que nos ofrece airbnb en la carpeta que acabamos de crear.

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC cd /Volumes/nttdata_databricks_lab/airbnb_madrid/airbnb_madrid/listing
-- MAGIC wget https://data.insideairbnb.com/spain/comunidad-de-madrid/madrid/2025-03-05/data/listings.csv.gz

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Descomprimimos en archivo descargado.

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC gzip -d /Volumes/nttdata_databricks_lab/airbnb_madrid/airbnb_madrid/listing/listings.csv.gz

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Comprobamos que esté ahí el archivo.

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC cd /Volumes/nttdata_databricks_lab/airbnb_madrid/airbnb_madrid/listing
-- MAGIC ls -sh 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Leemos el archivo y lo almacenamos en un DataFrame. Adicionalmente, transformamos la columna price para quitar el símbolo del dolar que nos molesta.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import regexp_replace, col
-- MAGIC
-- MAGIC dfAirbnbMadrid = spark.read.csv("/Volumes/nttdata_databricks_lab/airbnb_madrid/airbnb_madrid/listing/listings.csv", header=True, inferSchema=True, multiLine=True, escape='"')
-- MAGIC dfAirbnbmadrid = dfAirbnbMadrid.withColumn("price", regexp_replace(col("price"), "\\$", "").cast("decimal(10,2)"))
-- MAGIC display(dfAirbnbMadrid)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Escribimos el DataFrame en el esquema que hemos generado para ello.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dfAirbnbMadrid.write.format("delta").mode("overwrite").saveAsTable("nttdata_databricks_lab.airbnb_madrid.listings")
