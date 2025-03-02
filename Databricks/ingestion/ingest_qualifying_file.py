# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingesting qualifying json file from raw container

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, StructField, StructType
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

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType()),
                                       StructField("raceId", IntegerType()), 
                                       StructField("driverId", IntegerType()),
                                       StructField("constructorId", IntegerType()),
                                       StructField("number", IntegerType()),
                                       StructField("position", IntegerType()),
                                       StructField("q1", StringType()),
                                       StructField("q2", StringType()),
                                       StructField("q3", StringType())])



# COMMAND ----------

# MAGIC %md
# MAGIC 3. Read the multiple json files as dataframe

# COMMAND ----------

qualifying_df = spark.read.option("multiline", True).schema(qualifying_schema)\
                        .json(f"/mnt/formula1gokul/raw/{v_file_date}/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Final processing of the df

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id")\
                                    .withColumnRenamed("raceId", "race_id")\
                                    .withColumnRenamed("driverId", "driver_id")\
                                    .withColumnRenamed("constructorId", "constructor_id")\
                                    .withColumn("ingestion_date", current_timestamp())\
                                    .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# overwrite_partition(qualifying_final_df, "f1_processed", "qualifying", "race_id")

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(qualifying_final_df, 'f1_processed', 'qualifying', processed_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1) 
# MAGIC from f1_processed.qualifying
# MAGIC group by race_id
# MAGIC order by race_id desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.qualifying limit 30

# COMMAND ----------

# qualifying_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

dbutils.notebook.exit("Success")