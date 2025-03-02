# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingesting Results csv file from raw container

# COMMAND ----------

from pyspark.sql.types import DateType, StringType, IntegerType, DoubleType, StructType, StructField
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
# MAGIC 1. Defining schema

# COMMAND ----------

results_schema = StructType(fields = [StructField("resultId", IntegerType()), 
                                      StructField("raceId", IntegerType()),
                                      StructField("driverId", IntegerType()),
                                      StructField("constructorId", IntegerType()),
                                      StructField("number", IntegerType()), 
                                      StructField("grid", IntegerType()),
                                      StructField("position", IntegerType()),
                                      StructField("positionText", StringType()),
                                      StructField("positionOrder", IntegerType()),
                                      StructField("points", DoubleType()),
                                      StructField("laps", IntegerType()),
                                      StructField("time", StringType()),
                                      StructField("milliseconds", IntegerType()),
                                      StructField("fastestLap", IntegerType()),
                                      StructField("rank", IntegerType()),
                                      StructField("fastestLapTime", StringType()),
                                      StructField("fastestLapSpeed", DoubleType()),
                                      StructField("statusId", IntegerType())

])

# COMMAND ----------

# MAGIC %md
# MAGIC 2.Read the json file as a dataframe

# COMMAND ----------

results_df = spark.read.schema(results_schema).json(f"/mnt/formula1gokul/raw/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Renaming the column names and adding ingestion_date column

# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed("resultId", "result_id").\
                                withColumnRenamed("raceId", "race_id").\
                                withColumnRenamed("driverId", "driver_id").\
                                withColumnRenamed("constructorId", "constructor_id").\
                                withColumnRenamed("positionText", "position_text").\
                                withColumnRenamed("positionOrder", "position_order").\
                                withColumnRenamed("fastestLapTime", "fastest_lap_time").\
                                withColumnRenamed("fastestLap", "fastest_lap").\
                                withColumnRenamed("fastestLapSpeed", "fastest_lap_speed").\
                                withColumn("ingestion_date", current_timestamp()).\
                                withColumn('file_date', lit(v_file_date)).\
                                drop('statusId')                 

# COMMAND ----------

results_deduped_df = results_renamed_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Writing the final df into processed container in parquet format partitioned by *race_id*

# COMMAND ----------

# results_renamed_df.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# overwrite_partition(results_renamed_df, 'f1_processed', 'results', 'race_id')

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df, 'f1_processed', 'results', processed_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/formula1gokul/processed/results"

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1)
# MAGIC from f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id desc

# COMMAND ----------

dbutils.notebook.exit("Success")