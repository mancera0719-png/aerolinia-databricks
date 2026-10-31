# Aerolínea: mantenimiento y vuelos en Databricks

Proyecto de clase con arquitectura medallion (Bronze, Silver y Gold) en Databricks. Los datos son de una aerolínea: vuelos, aeronaves, aeropuertos y mantenimientos.

Todos los archivos vienen de la carpeta `/Workspace/Aerolinia` del workspace.

## Notebooks (trabajo en clase)

Los notebooks están exportados en formato fuente de Databricks (`.py`), así que se pueden volver a importar al workspace tal cual.

| Archivo | Qué hace |
|---|---|
| `preparacion_entorno.py` | Crea el catálogo `aerolinea`, los esquemas `fuentes`, `bronze`, `silver` y `gold`, y el volume `landing` donde se suben los CSV. |
| `Capa_bronze.py` | Lee los cuatro CSV del volume y los guarda como tablas Delta en `aerolinea.bronze`. |
| `Capa_Silver.py` | Tipifica y limpia los datos, revisa reglas de contenido, llaves repetidas, referencias y coherencia de fechas, y escribe en `aerolinea.silver`. |
| `Capa_gold.py` | Une las tablas y arma los indicadores de puntualidad, costos de mantenimiento, el resumen por aeronave y mes, y la base para ML. |
| `04_Control_Aerolinea.py` | Verifica conteos, cuadra los totales de Gold contra Silver y revisa el historial de las tablas Delta. |

## Scripts para el job

Son los mismos pasos escritos como scripts de PySpark para correrlos en un job. Escriben en el catálogo `airline_mantenimiento`.

| Archivo | Tarea del job |
|---|---|
| `01_bronze_ingestion.py` | bronze |
| `02_silver_transform.py` | silver (depende de bronze) |
| `03_gold_transform.py` | gold (depende de silver) |

El job `Job aerolinea py bronze silver gold` corre las tres tareas en ese orden con cómputo serverless.

## Tablas Gold que se generan

- `vuelos_enriquecidos`
- `mantenimientos_enriquecidos`
- `kpi_puntualidad_vuelos`
- `kpi_costo_mantenimiento`
- `resumen_aeronave_mes`
- `base_ml_mantenimiento`

## Datos de origen

Los CSV (`vuelos_diarios.csv`, `aeronaves.csv`, `aeropuertos.csv` y `mantenimientos_rds.csv`) están en el volume `/Volumes/aerolinea/fuentes/landing`.
