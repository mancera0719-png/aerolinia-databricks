# Databricks notebook source
# MAGIC %md
# MAGIC ###Cracion de insights
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE aerolinea.gold.vuelos_enriquecidos
# MAGIC USING DELTA AS
# MAGIC SELECT v.vuelo_id, v.fecha,
# MAGIC        CAST(date_trunc('MONTH', v.fecha) AS DATE) AS mes,
# MAGIC        v.aeronave_id, a.modelo, a.fabricante, a.anio_fabricacion,
# MAGIC        YEAR(v.fecha) - a.anio_fabricacion AS edad_aeronave,
# MAGIC        v.origen_id, o.nombre AS origen_nombre, o.ciudad AS origen_ciudad, o.pais AS origen_pais,
# MAGIC        v.destino_id, d.nombre AS destino_nombre, d.ciudad AS destino_ciudad, d.pais AS destino_pais,
# MAGIC        v.estado, v.duracion_min, v.estado <> 'cancelado' AS operado,
# MAGIC        v.ruta_por_revisar
# MAGIC FROM aerolinea.silver.vuelos v
# MAGIC LEFT JOIN aerolinea.silver.aeronaves a ON v.aeronave_id = a.aeronave_id
# MAGIC LEFT JOIN aerolinea.silver.aeropuertos o ON v.origen_id = o.aeropuerto_id
# MAGIC LEFT JOIN aerolinea.silver.aeropuertos d ON v.destino_id = d.aeropuerto_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM aerolinea.gold.vuelos_enriquecidos ORDER BY vuelo_id LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Eneriquecer tabla de mantenimientos 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE aerolinea.gold.mantenimientos_enriquecidos
# MAGIC USING DELTA AS
# MAGIC SELECT m.mantenimiento_id, m.fecha,
# MAGIC        CAST(date_trunc('MONTH', m.fecha) AS DATE) AS mes,
# MAGIC        m.aeronave_id, a.modelo, a.fabricante, a.anio_fabricacion,
# MAGIC        YEAR(m.fecha) - a.anio_fabricacion AS edad_aeronave,
# MAGIC        m.tipo, m.costo_usd, m.duracion_hr, m.archivo_origen
# MAGIC FROM aerolinea.silver.mantenimientos m
# MAGIC LEFT JOIN aerolinea.silver.aeronaves a ON m.aeronave_id = a.aeronave_id

# COMMAND ----------

vuelos = spark.table("aerolinea.gold.vuelos_enriquecidos").count()
mantenimientos = spark.table("aerolinea.gold.mantenimientos_enriquecidos").count()
assert vuelos == spark.table("aerolinea.silver.vuelos").count(), "La unión multiplicó o perdió vuelos."
assert mantenimientos == spark.table("aerolinea.silver.mantenimientos").count(), "La unión multiplicó o perdió intervenciones."
print("Vuelos enriquecidos:", vuelos)
print("Mantenimientos enriquecidos:", mantenimientos)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Puntualidad por grupo

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE aerolinea.gold.kpi_puntualidad_vuelos
# MAGIC USING DELTA AS
# MAGIC SELECT modelo, fabricante, origen_pais, mes,
# MAGIC        COUNT(*) AS total_vuelos,
# MAGIC        SUM(CASE WHEN operado THEN 1 ELSE 0 END) AS vuelos_operados,
# MAGIC        SUM(CASE WHEN estado = 'a_tiempo' THEN 1 ELSE 0 END) AS vuelos_a_tiempo,
# MAGIC        SUM(CASE WHEN estado = 'retrasado' THEN 1 ELSE 0 END) AS vuelos_retrasados,
# MAGIC        SUM(CASE WHEN estado = 'cancelado' THEN 1 ELSE 0 END) AS vuelos_cancelados,
# MAGIC        ROUND(100.0 * SUM(CASE WHEN estado = 'a_tiempo' THEN 1 ELSE 0 END)
# MAGIC          / NULLIF(SUM(CASE WHEN operado THEN 1 ELSE 0 END), 0), 2) AS puntualidad_operados_pct,
# MAGIC        ROUND(100.0 * SUM(CASE WHEN estado = 'cancelado' THEN 1 ELSE 0 END)
# MAGIC          / COUNT(*), 2) AS cancelacion_pct,
# MAGIC        ROUND(AVG(CASE WHEN operado THEN duracion_min END), 2) AS duracion_promedio_operados_min,
# MAGIC        SUM(CASE WHEN ruta_por_revisar THEN 1 ELSE 0 END) AS rutas_por_revisar
# MAGIC FROM aerolinea.gold.vuelos_enriquecidos
# MAGIC GROUP BY modelo, fabricante, origen_pais, mes

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM aerolinea.gold.kpi_puntualidad_vuelos ORDER BY modelo, origen_pais 

# COMMAND ----------

# MAGIC %md
# MAGIC ##Puntualidad global 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(total_vuelos) AS vuelos,
# MAGIC        SUM(vuelos_operados) AS operados,
# MAGIC        SUM(vuelos_a_tiempo) AS a_tiempo,
# MAGIC        SUM(vuelos_cancelados) AS cancelados,
# MAGIC        ROUND(100.0 * SUM(vuelos_a_tiempo) / NULLIF(SUM(vuelos_operados), 0), 2) AS puntualidad_pct,
# MAGIC        ROUND(100.0 * SUM(vuelos_cancelados) / SUM(total_vuelos), 2) AS cancelacion_pct
# MAGIC FROM aerolinea.gold.kpi_puntualidad_vuelos

# COMMAND ----------

# MAGIC %md
# MAGIC ## Costos de mantenimiento 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE aerolinea.gold.kpi_costo_mantenimiento
# MAGIC USING DELTA AS
# MAGIC SELECT aeronave_id, modelo, fabricante, tipo, mes,
# MAGIC        COUNT(*) AS total_mantenimientos,
# MAGIC        SUM(costo_usd) AS costo_total_usd,
# MAGIC        ROUND(AVG(costo_usd), 2) AS costo_promedio_usd,
# MAGIC        SUM(duracion_hr) AS duracion_total_hr,
# MAGIC        ROUND(AVG(duracion_hr), 2) AS duracion_promedio_hr
# MAGIC FROM aerolinea.gold.mantenimientos_enriquecidos
# MAGIC GROUP BY aeronave_id, modelo, fabricante, tipo, mes

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tipo,
# MAGIC        SUM(total_mantenimientos) AS intervenciones,
# MAGIC        SUM(costo_total_usd) AS costo_total_usd
# MAGIC FROM aerolinea.gold.kpi_costo_mantenimiento
# MAGIC GROUP BY tipo
# MAGIC ORDER BY costo_total_usd DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE aerolinea.gold.resumen_aeronave_mes
# MAGIC USING DELTA AS
# MAGIC WITH actividad AS (
# MAGIC   SELECT aeronave_id, mes, COUNT(*) AS total_vuelos,
# MAGIC          SUM(CASE WHEN operado THEN 1 ELSE 0 END) AS vuelos_operados
# MAGIC   FROM aerolinea.gold.vuelos_enriquecidos GROUP BY aeronave_id, mes
# MAGIC ), mantenimiento AS (
# MAGIC   SELECT aeronave_id, mes, COUNT(*) AS total_mantenimientos,
# MAGIC          SUM(costo_usd) AS costo_total_usd
# MAGIC   FROM aerolinea.gold.mantenimientos_enriquecidos GROUP BY aeronave_id, mes
# MAGIC )
# MAGIC SELECT COALESCE(v.aeronave_id, m.aeronave_id) AS aeronave_id,
# MAGIC        COALESCE(v.mes, m.mes) AS mes,
# MAGIC        COALESCE(v.total_vuelos, 0) AS total_vuelos,
# MAGIC        COALESCE(v.vuelos_operados, 0) AS vuelos_operados,
# MAGIC        COALESCE(m.total_mantenimientos, 0) AS total_mantenimientos,
# MAGIC        COALESCE(m.costo_total_usd, 0) AS costo_total_usd
# MAGIC FROM actividad v FULL OUTER JOIN mantenimiento m
# MAGIC   ON v.aeronave_id = m.aeronave_id AND v.mes = m.mes

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM aerolinea.gold.resumen_aeronave_mes ORDER BY aeronave_id, mes

# COMMAND ----------

# MAGIC %md
# MAGIC ##Consturccion para ML

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE aerolinea.gold.base_ml_mantenimiento
# MAGIC USING DELTA AS
# MAGIC SELECT mantenimiento_id, fecha, aeronave_id,
# MAGIC        tipo, modelo, fabricante, edad_aeronave, costo_usd
# MAGIC FROM aerolinea.gold.mantenimientos_enriquecidos

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM aerolinea.gold.base_ml_mantenimiento ORDER BY mantenimiento_id LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC Construccio

# COMMAND ----------

# MAGIC %md
# MAGIC