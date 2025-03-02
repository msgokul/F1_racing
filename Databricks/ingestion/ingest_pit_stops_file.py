# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingesting pits stop json file from raw container

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

pit_stop_schema = StructType(fields=[StructField("raceId", IntegerType()), 
                                     StructField("driverId", IntegerType()),
                                     StructField("stop", IntegerType()),
                                     StructField("lap", IntegerType()),
                                     StructField("time", StringType()), 
                                     StructField("duration", StringType()), 
                                     StructField("milliseconds", IntegerType())

])

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Read the json file as a dataframe

# COMMAND ----------

pit_stops_df = spark.read.schema(pit_stop_schema).option("multiLine", True)\
                    .json(f"/mnt/formula1gokul/raw/{v_file_date}/pit_stops.json")

# COMMAND ----------

pit_stops_final_df = pit_stops_df.withColumnRenamed("raceId", "race_id")\
                                 .withColumnRenamed("driverId", "driver_id")\
                                 .withColumn("ingested_date", current_timestamp()) \
                                 .withColumn('file_date', lit(v_file_date)) 

# COMMAND ----------

# overwrite_partition(pit_stops_final_df, "f1_processed", "pit_stops", "race_id")

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(pit_stops_final_df, 'f1_processed', 'pit_stops', processed_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1) 
# MAGIC from f1_processed.pit_stops
# MAGIC group by race_id
# MAGIC order by race_id desc

# COMMAND ----------

# pit_stops_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

dbutils.notebook.exit("Success")