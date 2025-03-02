-- Databricks notebook source
drop database if exists f1_processed cascade

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####cascade will drop all the tables within a db and the db itself

-- COMMAND ----------

drop database if exists f1_presentation cascade

-- COMMAND ----------

create database if not exists f1_processed
location '/mnt/formula1gokul/processed'

-- COMMAND ----------

create database if not exists f1_presentation
location '/mnt/formula1gokul/presentation'