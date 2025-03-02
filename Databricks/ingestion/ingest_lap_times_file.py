# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingesting lap time multiple csv files from raw container

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, StructType, StructField
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

# MAGIC %run ../configs

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Define schema

# COMMAND ----------

lap_time_schema = StructType(fields=[StructField("raceId", IntegerType()), 
                                     StructField("driverId", IntegerType()),
                                     StructField("lap", IntegerType()),                                     StructField("position", IntegerType()), 
                                     StructField("time", StringType()), 
                                     StructField("milliseconds", IntegerType())

])

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Read the json file as a dataframe

# COMMAND ----------

lap_times_df = spark.read.schema(lap_time_schema).csv(f"/mnt/formula1gokul/raw/{v_file_date}/lap_times")

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed("raceId", "race_id")\
                                 .withColumnRenamed("driverId", "driver_id")\
                                 .withColumn("ingested_date", current_timestamp()) \
                                 .withColumn('file_date', lit(v_file_date)) 

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
merge_delta_data(lap_times_final_df, 'f1_processed', 'lap_times', processed_path, merge_condition, 'race_id')

# COMMAND ----------

# overwrite_partition(lap_times_final_df, "f1_processed", "lap_times", "race_id")

# COMMAND ----------

# lap_times_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1) 
# MAGIC from f1_processed.lap_times
# MAGIC group by race_id
# MAGIC order by race_id desc

# COMMAND ----------

dbutils.notebook.exit("Success")