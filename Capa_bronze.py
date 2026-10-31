# Databricks notebook source
# MAGIC %md
# MAGIC ###Carga de librerias, funciones y definicion de rutas 

# COMMAND ----------

from pyspark.sql import functions as F

ruta = "/Volumes/aerolinea/fuentes/landing"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Lectura datos 

# COMMAND ----------

vuelos_bronze = (
    spark.read.csv(
        ruta + "/vuelos_diarios.csv", 
        header=True,  
      inferSchema=True,
    )
    .withColumn("archivo_origen", F.lit("vuelos_diarios.csv"))
    .withColumn("fecha_ingesta", F.current_timestamp())
)

display(vuelos_bronze.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Lectura aeronaves

# COMMAND ----------

aeronaves_bronze = (
    spark.read.csv(
        ruta + "/aeronaves.csv", 
        header=True,  
      inferSchema=True,
    )
    .withColumn("archivo_origen", F.lit("aeronaves.csv"))
    .withColumn("fecha_ingesta", F.current_timestamp())
)

display(vuelos_bronze.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Lecctura aeropuertos

# COMMAND ----------

aeropuertos_bronze = (
    spark.read.csv(
        ruta + "/aeropuertos.csv", 
        header=True,  
      inferSchema=True,
    )
    .withColumn("archivo_origen", F.lit("aeropuertos.csv"))
    .withColumn("fecha_ingesta", F.current_timestamp())
)

display(vuelos_bronze.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Lectura manteniminetos 

# COMMAND ----------

mantenimientos_bronze = (
    spark.read.csv(
        ruta + "/mantenimientos_rds.csv", 
        header=True,  
      inferSchema=True,
    )
    .withColumn("archivo_origen", F.lit("mantenimientos_rds.csv"))
    .withColumn("fecha_ingesta", F.current_timestamp())
)

display(vuelos_bronze.limit(5))

# COMMAND ----------

vuelos_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Escritura capa Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC USE aerolinea.bronze;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table vuelos

# COMMAND ----------

vuelos_bronze.write.format("delta").mode("overwrite").saveAsTable("vuelos")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vuelos limit 2

# COMMAND ----------



aeronaves_bronze.write.format("delta").mode("overwrite").saveAsTable("aeronaves")

aeropuertos_bronze.write.format("delta").mode("overwrite").saveAsTable("aeropuertos")

mantenimientos_bronze.write.format("delta").mode("overwrite").saveAsTable("mantenimientos")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Consulta de verificcacion

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'vuelos' AS fuente, COUNT(*) AS registros FROM aerolinea.bronze.vuelos
# MAGIC UNION ALL 
# MAGIC SELECT 'aeronaves', COUNT(*) FROM aerolinea.bronze.aeronaves
# MAGIC UNION ALL 
# MAGIC SELECT 'aeropuertos', COUNT(*) FROM aerolinea.bronze.aeropuertos
# MAGIC UNION ALL 
# MAGIC SELECT 'mantenimientos', COUNT(*) FROM aerolinea.bronze.mantenimientos