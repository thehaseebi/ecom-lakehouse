# Databricks notebook source
spark.sql("create schema if not exists workspace.ecom")

spark.sql("create volume if not exists workspace.ecom.raw")

for folder in ['orders', 'customers', 'products']:
    dbutils.fs.mkdirs(f'/Volumes/workspace/ecom/raw/{folder}')

display(dbutils.fs.ls('/Volumes/workspace/ecom/raw'))

# COMMAND ----------

