# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS aerolinea;
# MAGIC USE CATALOG aerolinea

# COMMAND ----------

# MAGIC %md
# MAGIC ###Creacion de esquemas arquitectura medallion

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS aerolinea.fuentes;
# MAGIC CREATE SCHEMA IF NOT EXISTS aerolinea.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS aerolinea.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS aerolinea.gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creacion del volumen de datos 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS aerolinea.fuentes.landing
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificacion de esquemas y volumen 

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN aerolinea;
# MAGIC SHOW VOLUMES IN aerolinea.fuentes;