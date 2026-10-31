# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT 'bronze.vuelos' AS tabla, COUNT(*) AS registros FROM aerolinea.bronze.vuelos
# MAGIC UNION ALL SELECT 'silver.vuelos', COUNT(*) FROM aerolinea.silver.vuelos
# MAGIC UNION ALL SELECT 'gold.vuelos_enriquecidos', COUNT(*) FROM aerolinea.gold.vuelos_enriquecidos
# MAGIC UNION ALL SELECT 'gold.mantenimientos_enriquecidos', COUNT(*) FROM aerolinea.gold.mantenimientos_enriquecidos
# MAGIC UNION ALL SELECT 'gold.base_ml_mantenimiento', COUNT(*) FROM aerolinea.gold.base_ml_mantenimiento

# COMMAND ----------

vuelos = spark.table("aerolinea.gold.vuelos_enriquecidos").count()
assert vuelos == 100, "El conteo no coincide con la instantánea de clase."
assert spark.table("aerolinea.gold.mantenimientos_enriquecidos").count() == 100
assert spark.table("aerolinea.gold.base_ml_mantenimiento").count() == 100

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reconciliación de conteos y costos

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW control_reconciliacion AS
# MAGIC SELECT 'vuelos_por_grupo' AS regla,
# MAGIC        (SELECT COUNT(*) FROM aerolinea.silver.vuelos) AS esperado,
# MAGIC        (SELECT SUM(total_vuelos) FROM aerolinea.gold.kpi_puntualidad_vuelos) AS obtenido
# MAGIC UNION ALL
# MAGIC SELECT 'mantenimientos_por_grupo',
# MAGIC        (SELECT COUNT(*) FROM aerolinea.silver.mantenimientos),
# MAGIC        (SELECT SUM(total_mantenimientos) FROM aerolinea.gold.kpi_costo_mantenimiento)
# MAGIC UNION ALL
# MAGIC SELECT 'vuelos_por_aeronave_mes',
# MAGIC        (SELECT COUNT(*) FROM aerolinea.silver.vuelos),
# MAGIC        (SELECT SUM(total_vuelos) FROM aerolinea.gold.resumen_aeronave_mes)
# MAGIC UNION ALL
# MAGIC SELECT 'mantenimientos_por_aeronave_mes',
# MAGIC        (SELECT COUNT(*) FROM aerolinea.silver.mantenimientos),
# MAGIC        (SELECT SUM(total_mantenimientos) FROM aerolinea.gold.resumen_aeronave_mes)
# MAGIC UNION ALL
# MAGIC SELECT 'costo_por_grupo',
# MAGIC        (SELECT SUM(costo_usd) FROM aerolinea.silver.mantenimientos),
# MAGIC        (SELECT SUM(costo_total_usd) FROM aerolinea.gold.kpi_costo_mantenimiento)
# MAGIC UNION ALL
# MAGIC SELECT 'costo_por_aeronave_mes',
# MAGIC        (SELECT SUM(costo_usd) FROM aerolinea.silver.mantenimientos),
# MAGIC        (SELECT SUM(costo_total_usd) FROM aerolinea.gold.resumen_aeronave_mes)

# COMMAND ----------

reconciliacion = spark.table("control_reconciliacion")
display(reconciliacion)
assert reconciliacion.filter("obtenido IS NULL OR esperado <> obtenido").count() == 0, "Un resumen no coincide con el detalle."

# COMMAND ----------

# MAGIC %md
# MAGIC ###Revisión de las tasas

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(total_vuelos) AS total,
# MAGIC        SUM(vuelos_a_tiempo) AS a_tiempo, SUM(vuelos_retrasados) AS retrasados,
# MAGIC        SUM(vuelos_cancelados) AS cancelados, SUM(vuelos_operados) AS operados,
# MAGIC        SUM(rutas_por_revisar) AS rutas_por_revisar,
# MAGIC        ROUND(100.0 * SUM(vuelos_a_tiempo) / NULLIF(SUM(vuelos_operados), 0), 2) AS puntualidad_pct
# MAGIC FROM aerolinea.gold.kpi_puntualidad_vuelos

# COMMAND ----------

estados = spark.sql("""
SELECT SUM(total_vuelos) AS total, SUM(vuelos_operados) AS operados,
       SUM(vuelos_a_tiempo) AS a_tiempo, SUM(vuelos_retrasados) AS retrasados,
       SUM(vuelos_cancelados) AS cancelados
FROM aerolinea.gold.kpi_puntualidad_vuelos
""").first()
assert estados["a_tiempo"] + estados["retrasados"] == estados["operados"]
assert estados["operados"] + estados["cancelados"] == estados["total"]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Consulta, plan, archivos e historial

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT origen_pais, COUNT(*) AS vuelos_operados
# MAGIC FROM aerolinea.gold.vuelos_enriquecidos
# MAGIC WHERE operado
# MAGIC GROUP BY origen_pais

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN FORMATTED
# MAGIC SELECT origen_pais, COUNT(*) AS operados
# MAGIC FROM aerolinea.gold.vuelos_enriquecidos
# MAGIC WHERE operado
# MAGIC GROUP BY origen_pais

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Describe detail tabla vuelos
display(spark.sql("DESCRIBE DETAIL aerolinea.gold.vuelos_enriquecidos"))

# COMMAND ----------

# DBTITLE 1,Historial de versiones tabla vuelos
display(spark.sql("DESCRIBE HISTORY aerolinea.gold.vuelos_enriquecidos"))

# COMMAND ----------

