# Databricks notebook source
spark.table('databox.logistica_comum.sop_sku').display()

# COMMAND ----------

# MAGIC %sql SELECT * FROM databox.logistica_comum.sop_sku
