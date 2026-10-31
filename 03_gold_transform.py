from pyspark.sql import functions as F
from pyspark.sql import SparkSession

# =========================
# Configuración
# =========================
CATALOG = "airline_mantenimiento"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

spark = SparkSession.builder.getOrCreate()

# =========================
# 0. Asegurar schema Gold
# =========================

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

# =========================
# 1. Leer Silver
# =========================

vuelos_slv = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.vuelos")
aeronaves_slv = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.aeronaves")
aeropuertos_slv = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.aeropuertos")
mantenimientos_slv = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.mantenimientos")

# El Silver en .py no guarda ruta_por_revisar ni archivo_origen.
# Si la columna no viene, se calcula aquí o simplemente no se incluye.
if "ruta_por_revisar" not in vuelos_slv.columns:
    vuelos_slv = vuelos_slv.withColumn(
        "ruta_por_revisar",
        F.upper(F.trim(F.col("origen_id"))) == F.upper(F.trim(F.col("destino_id")))
    )

cols_extra_mant = ["archivo_origen"] if "archivo_origen" in mantenimientos_slv.columns else []

# =========================
# 2. Transformaciones Gold
# =========================

#Tablas auxiliares para no chocar nombres en los joins
aeronaves_info = aeronaves_slv.select("aeronave_id", "modelo", "fabricante", "anio_fabricacion")

origen_info = aeropuertos_slv.select(
    F.col("aeropuerto_id").alias("origen_id"),
    F.col("nombre").alias("origen_nombre"),
    F.col("ciudad").alias("origen_ciudad"),
    F.col("pais").alias("origen_pais")
)

destino_info = aeropuertos_slv.select(
    F.col("aeropuerto_id").alias("destino_id"),
    F.col("nombre").alias("destino_nombre"),
    F.col("ciudad").alias("destino_ciudad"),
    F.col("pais").alias("destino_pais")
)

#Gold vuelos enriquecidos
vuelos_enriquecidos = (
    vuelos_slv
    .join(aeronaves_info, on="aeronave_id", how="left")
    .join(origen_info, on="origen_id", how="left")
    .join(destino_info, on="destino_id", how="left")
    .withColumn("mes", F.trunc(F.col("fecha"), "month"))
    .withColumn("edad_aeronave", F.year(F.col("fecha")) - F.col("anio_fabricacion"))
    .withColumn("operado", F.col("estado") != "cancelado")
    .select("vuelo_id",
            "fecha",
            "mes",
            "aeronave_id",
            "modelo",
            "fabricante",
            "anio_fabricacion",
            "edad_aeronave",
            "origen_id",
            "origen_nombre",
            "origen_ciudad",
            "origen_pais",
            "destino_id",
            "destino_nombre",
            "destino_ciudad",
            "destino_pais",
            "estado",
            "duracion_min",
            "operado",
            "ruta_por_revisar"
    )
)

#Gold mantenimientos enriquecidos
mantenimientos_enriquecidos = (
    mantenimientos_slv
    .join(aeronaves_info, on="aeronave_id", how="left")
    .withColumn("mes", F.trunc(F.col("fecha"), "month"))
    .withColumn("edad_aeronave", F.year(F.col("fecha")) - F.col("anio_fabricacion"))
    .select("mantenimiento_id",
            "fecha",
            "mes",
            "aeronave_id",
            "modelo",
            "fabricante",
            "anio_fabricacion",
            "edad_aeronave",
            "tipo",
            "costo_usd",
            "duracion_hr",
            *cols_extra_mant
    )
)

#KPI puntualidad por modelo, fabricante, país de origen y mes
kpi_puntualidad_vuelos = (
    vuelos_enriquecidos
    .groupBy("modelo", "fabricante", "origen_pais", "mes")
    .agg(
        F.count("*").alias("total_vuelos"),
        F.sum(F.when(F.col("operado"), 1).otherwise(0)).alias("vuelos_operados"),
        F.sum(F.when(F.col("estado") == "a_tiempo", 1).otherwise(0)).alias("vuelos_a_tiempo"),
        F.sum(F.when(F.col("estado") == "retrasado", 1).otherwise(0)).alias("vuelos_retrasados"),
        F.sum(F.when(F.col("estado") == "cancelado", 1).otherwise(0)).alias("vuelos_cancelados"),
        F.round(F.avg(F.when(F.col("operado"), F.col("duracion_min"))), 2).alias("duracion_promedio_operados_min"),
        F.sum(F.when(F.col("ruta_por_revisar"), 1).otherwise(0)).alias("rutas_por_revisar")
    )
    .withColumn(
        "puntualidad_operados_pct",
        F.when(F.col("vuelos_operados") > 0,
               F.round(100.0 * F.col("vuelos_a_tiempo") / F.col("vuelos_operados"), 2))
    )
    .withColumn(
        "cancelacion_pct",
        F.round(100.0 * F.col("vuelos_cancelados") / F.col("total_vuelos"), 2)
    )
    .select("modelo",
            "fabricante",
            "origen_pais",
            "mes",
            "total_vuelos",
            "vuelos_operados",
            "vuelos_a_tiempo",
            "vuelos_retrasados",
            "vuelos_cancelados",
            "puntualidad_operados_pct",
            "cancelacion_pct",
            "duracion_promedio_operados_min",
            "rutas_por_revisar"
    )
)

#KPI costo de mantenimiento por aeronave, tipo y mes
kpi_costo_mantenimiento = (
    mantenimientos_enriquecidos
    .groupBy("aeronave_id", "modelo", "fabricante", "tipo", "mes")
    .agg(
        F.count("*").alias("total_mantenimientos"),
        F.sum("costo_usd").alias("costo_total_usd"),
        F.round(F.avg("costo_usd"), 2).alias("costo_promedio_usd"),
        F.sum("duracion_hr").alias("duracion_total_hr"),
        F.round(F.avg("duracion_hr"), 2).alias("duracion_promedio_hr")
    )
)

#Resumen por aeronave y mes (vuelos + mantenimientos)
actividad = (
    vuelos_enriquecidos
    .groupBy("aeronave_id", "mes")
    .agg(
        F.count("*").alias("total_vuelos"),
        F.sum(F.when(F.col("operado"), 1).otherwise(0)).alias("vuelos_operados")
    )
)

mantenimiento = (
    mantenimientos_enriquecidos
    .groupBy("aeronave_id", "mes")
    .agg(
        F.count("*").alias("total_mantenimientos"),
        F.sum("costo_usd").alias("costo_total_usd")
    )
)

resumen_aeronave_mes = (
    actividad
    .join(mantenimiento, on=["aeronave_id", "mes"], how="full_outer")
    .fillna(0, subset=["total_vuelos", "vuelos_operados", "total_mantenimientos", "costo_total_usd"])
    .select("aeronave_id",
            "mes",
            "total_vuelos",
            "vuelos_operados",
            "total_mantenimientos",
            "costo_total_usd"
    )
)

#Base para ML de mantenimiento
base_ml_mantenimiento = (
    mantenimientos_enriquecidos
    .select("mantenimiento_id",
            "fecha",
            "aeronave_id",
            "tipo",
            "modelo",
            "fabricante",
            "edad_aeronave",
            "costo_usd"
    )
)

# =========================
# 3. Validaciones de Calidad
# =========================

total_vuelos_slv = vuelos_slv.count()
total_vuelos_gld = vuelos_enriquecidos.count()
total_mant_slv = mantenimientos_slv.count()
total_mant_gld = mantenimientos_enriquecidos.count()

print("Vuelos Silver:", total_vuelos_slv, "| Vuelos enriquecidos:", total_vuelos_gld)
print("Mantenimientos Silver:", total_mant_slv, "| Mantenimientos enriquecidos:", total_mant_gld)

assert total_vuelos_gld == total_vuelos_slv, "La unión multiplicó o perdió vuelos."
assert total_mant_gld == total_mant_slv, "La unión multiplicó o perdió intervenciones."

# =========================
# 4. Guardar en Gold
# =========================

vuelos_enriquecidos.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.vuelos_enriquecidos")

mantenimientos_enriquecidos.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.mantenimientos_enriquecidos")

kpi_puntualidad_vuelos.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.kpi_puntualidad_vuelos")

kpi_costo_mantenimiento.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.kpi_costo_mantenimiento")

resumen_aeronave_mes.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.resumen_aeronave_mes")

base_ml_mantenimiento.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.base_ml_mantenimiento")

# =========================
# 5. Resumen de indicadores
# =========================

#Puntualidad global
kpi_gold = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.kpi_puntualidad_vuelos")
(
    kpi_gold
    .agg(
        F.sum("total_vuelos").alias("vuelos"),
        F.sum("vuelos_operados").alias("operados"),
        F.sum("vuelos_a_tiempo").alias("a_tiempo"),
        F.sum("vuelos_cancelados").alias("cancelados")
    )
    .withColumn("puntualidad_pct",
                F.when(F.col("operados") > 0,
                       F.round(100.0 * F.col("a_tiempo") / F.col("operados"), 2)))
    .withColumn("cancelacion_pct",
                F.round(100.0 * F.col("cancelados") / F.col("vuelos"), 2))
    .show()
)

#Costo total por tipo de mantenimiento
(
    spark.table(f"{CATALOG}.{GOLD_SCHEMA}.kpi_costo_mantenimiento")
    .groupBy("tipo")
    .agg(
        F.sum("total_mantenimientos").alias("intervenciones"),
        F.sum("costo_total_usd").alias("costo_total_usd")
    )
    .orderBy(F.col("costo_total_usd").desc())
    .show()
)

print("Gold Delta completada")
print(f"Tablas creadas/actualizadas: {CATALOG}.{GOLD_SCHEMA}.vuelos_enriquecidos, mantenimientos_enriquecidos, "
      "kpi_puntualidad_vuelos, kpi_costo_mantenimiento, resumen_aeronave_mes, base_ml_mantenimiento")
