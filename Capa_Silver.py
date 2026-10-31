# Databricks notebook source
# MAGIC %md
# MAGIC ###Tratamiento de datos 

# COMMAND ----------

from pyspark.sql import functions as f 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tipificacion de tablas 

# COMMAND ----------

# MAGIC %sql
# MAGIC USE aerolinea.bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW aeronaves_tipadas AS
# MAGIC SELECT DISTINCT
# MAGIC        UPPER(TRIM(aeronave_id)) AS aeronave_id,
# MAGIC        TRIM(modelo) AS modelo,
# MAGIC        TRIM(fabricante) AS fabricante,
# MAGIC        TRY_CAST(anio_fabricacion AS INT) AS anio_fabricacion,
# MAGIC        archivo_origen,
# MAGIC        fecha_ingesta
# MAGIC FROM aeronaves

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aeronaves_tipadas limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TIPIFICACION DE AEROPUERTOS
# MAGIC CREATE OR REPLACE TEMP VIEW aeropuertos_tipados AS
# MAGIC SELECT DISTINCT
# MAGIC        UPPER(TRIM(aeropuerto_id)) AS aeropuerto_id,
# MAGIC        TRIM(nombre) AS nombre,
# MAGIC        trim(ciudad) AS ciudad,
# MAGIC        TRIM(pais) AS pais,
# MAGIC        try_cast(lat AS DOUBLE) AS lat,
# MAGIC        try_cast(lon AS DOUBLE) AS lon,
# MAGIC       
# MAGIC        fecha_ingesta
# MAGIC FROM aeropuertos

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificacion aeropuertos
# MAGIC select * from aeropuertos_tipados limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vuelos limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC --TIPIFICACION VUELOS
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW vuelos_tipados AS
# MAGIC SELECT DISTINCT
# MAGIC     UPPER(TRIM(vuelo_id)) AS vuelo_id,
# MAGIC     TRY_CAST(fecha AS DATE) AS fecha,
# MAGIC     TRIM(origen_id) AS origen_id,
# MAGIC     TRIM(destino_id) AS destino_id,
# MAGIC     TRIM(aeronave_id) AS aeronave_id,
# MAGIC     LOWER(TRIM(estado)) AS estado,
# MAGIC     TRY_CAST(duracion_min AS INTEGER) AS duracion_min,
# MAGIC     UPPER(TRIM(origen_id)) = UPPER(TRIM(destino_id)) AS ruta_por_revisar,
# MAGIC     fecha_ingesta
# MAGIC FROM vuelos

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vuelos_tipados limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW mantenimientos_tipados AS
# MAGIC SELECT DISTINCT
# MAGIC        UPPER(TRIM(mantenimiento_id)) AS mantenimiento_id,
# MAGIC        UPPER(TRIM(aeronave_id)) AS aeronave_id,
# MAGIC        TRY_CAST(fecha AS DATE) AS fecha,
# MAGIC        LOWER(TRIM(tipo)) AS tipo,
# MAGIC        TRY_CAST(costo_usd AS DECIMAL(14,2)) AS costo_usd,
# MAGIC        TRY_CAST(duracion_hr AS DECIMAL(10,2)) AS duracion_hr,
# MAGIC        archivo_origen
# MAGIC FROM mantenimientos

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mantenimientos_tipados limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC ###Reglas contenido 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW control_contenido AS
# MAGIC SELECT 'aeronaves' AS conjunto, COUNT(*) AS errores
# MAGIC FROM aeronaves_tipadas
# MAGIC WHERE aeronave_id IS NULL OR aeronave_id = '' OR modelo IS NULL OR modelo = ''
# MAGIC    OR fabricante IS NULL OR fabricante = '' OR anio_fabricacion IS NULL
# MAGIC    OR anio_fabricacion < 1900 OR anio_fabricacion > YEAR(current_date())
# MAGIC UNION ALL
# MAGIC SELECT 'aeropuertos', COUNT(*) FROM aeropuertos_tipados
# MAGIC WHERE aeropuerto_id IS NULL OR aeropuerto_id = '' OR nombre IS NULL OR nombre = ''
# MAGIC    OR ciudad IS NULL OR ciudad = '' OR pais IS NULL OR pais = ''
# MAGIC    OR lat IS NULL OR lon IS NULL OR isnan(lat) OR isnan(lon)
# MAGIC    OR lat NOT BETWEEN -90 AND 90 OR lon NOT BETWEEN -180 AND 180
# MAGIC UNION ALL
# MAGIC SELECT 'vuelos', COUNT(*) FROM vuelos_tipados
# MAGIC WHERE vuelo_id IS NULL OR vuelo_id = '' OR fecha IS NULL
# MAGIC    OR aeronave_id IS NULL OR aeronave_id = ''
# MAGIC    OR origen_id IS NULL OR origen_id = '' OR destino_id IS NULL OR destino_id = ''
# MAGIC    OR estado IS NULL OR estado NOT IN ('a_tiempo','retrasado','cancelado')
# MAGIC    OR duracion_min < 0
# MAGIC    OR (estado <> 'cancelado' AND (duracion_min IS NULL OR duracion_min = 0))
# MAGIC UNION ALL
# MAGIC SELECT 'mantenimientos', COUNT(*) FROM mantenimientos_tipados
# MAGIC WHERE mantenimiento_id IS NULL OR mantenimiento_id = '' OR fecha IS NULL
# MAGIC    OR aeronave_id IS NULL OR aeronave_id = '' OR tipo IS NULL
# MAGIC    OR tipo NOT IN ('preventivo','correctivo','inspección')
# MAGIC    OR costo_usd IS NULL OR costo_usd < 0 OR duracion_hr IS NULL OR duracion_hr <= 0

# COMMAND ----------

contenido=spark.table("control_contenido")
display(contenido)
assert contenido.filter("errores>0").count()==0, "Se encontraron errores en el proceso de ingestacion antes de ingestar a silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Revision de conflictos por llave 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW control_llaves AS
# MAGIC SELECT 'aeronaves' AS conjunto, COUNT(*) AS errores FROM (
# MAGIC   SELECT aeronave_id FROM aeronaves_tipadas GROUP BY aeronave_id HAVING COUNT(*) > 1
# MAGIC ) a
# MAGIC UNION ALL
# MAGIC SELECT 'aeropuertos', COUNT(*) FROM (
# MAGIC   SELECT aeropuerto_id FROM aeropuertos_tipados GROUP BY aeropuerto_id HAVING COUNT(*) > 1
# MAGIC ) b
# MAGIC UNION ALL
# MAGIC SELECT 'vuelos', COUNT(*) FROM (
# MAGIC   SELECT vuelo_id FROM vuelos_tipados GROUP BY vuelo_id HAVING COUNT(*) > 1
# MAGIC ) c
# MAGIC UNION ALL
# MAGIC SELECT 'mantenimientos', COUNT(*) FROM (
# MAGIC   SELECT mantenimiento_id FROM mantenimientos_tipados GROUP BY mantenimiento_id HAVING COUNT(*) > 1
# MAGIC ) d

# COMMAND ----------

llaves = spark.table("control_llaves")
display(llaves)
assert llaves.filter("errores > 0").count() == 0, "Hay llaves con registros diferentes."


# COMMAND ----------

# MAGIC %md
# MAGIC ###Revision de referncias y coherencia temporal 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW control_referencias AS
# MAGIC SELECT 'vuelo_aeronave' AS regla, COUNT(*) AS errores
# MAGIC FROM vuelos_tipados v LEFT ANTI JOIN aeronaves_tipadas a ON v.aeronave_id = a.aeronave_id
# MAGIC UNION ALL
# MAGIC SELECT 'vuelo_origen', COUNT(*)
# MAGIC FROM vuelos_tipados v LEFT ANTI JOIN aeropuertos_tipados a ON v.origen_id = a.aeropuerto_id
# MAGIC UNION ALL
# MAGIC SELECT 'vuelo_destino', COUNT(*)
# MAGIC FROM vuelos_tipados v LEFT ANTI JOIN aeropuertos_tipados a ON v.destino_id = a.aeropuerto_id
# MAGIC UNION ALL
# MAGIC SELECT 'mantenimiento_aeronave', COUNT(*)
# MAGIC FROM mantenimientos_tipados m LEFT ANTI JOIN aeronaves_tipadas a ON m.aeronave_id = a.aeronave_id
# MAGIC UNION ALL
# MAGIC SELECT 'fabricacion_posterior_vuelo', COUNT(*)
# MAGIC FROM vuelos_tipados v JOIN aeronaves_tipadas a ON v.aeronave_id = a.aeronave_id
# MAGIC WHERE a.anio_fabricacion > YEAR(v.fecha)
# MAGIC UNION ALL
# MAGIC SELECT 'fabricacion_posterior_mantenimiento', COUNT(*)
# MAGIC FROM mantenimientos_tipados m JOIN aeronaves_tipadas a ON m.aeronave_id = a.aeronave_id
# MAGIC WHERE a.anio_fabricacion > YEAR(m.fecha)

# COMMAND ----------

referencias = spark.table("control_referencias")
display(referencias)
assert referencias.filter("errores > 0").count() == 0, "Revisar las referencias o los años de fabricación."

# COMMAND ----------

# MAGIC %md
# MAGIC ### Observaciones visibles

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS rutas_por_revisar FROM vuelos_tipados WHERE ruta_por_revisar;
# MAGIC SELECT vuelo_id, fecha, origen_id, destino_id, estado
# MAGIC FROM vuelos_tipados WHERE ruta_por_revisar ORDER BY vuelo_id ;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingesta a  Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC Use aerolinea.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE aeronaves USING DELTA AS SELECT * FROM aeronaves_tipadas;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE mantenimientos USING DELTA AS SELECT * FROM mantenimientos_tipados;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE aeropuertos USING DELTA AS SELECT * FROM aeropuertos_tipados;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE vuelos USING DELTA AS SELECT * FROM vuelos_tipados;
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aeropuertos limit 5 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vuelos limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aeronaves limit 5 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mantenimientos limit 5 

# COMMAND ----------

# MAGIC %md
# MAGIC