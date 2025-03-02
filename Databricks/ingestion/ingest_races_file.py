# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingesting races file from demo container

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, StringType, TimestampType
from pyspark.sql.functions import col, lit, to_timestamp, concat, current_timestamp

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Explicitly define schema for the df

# COMMAND ----------

races_schema = StructType([StructField('raceId', IntegerType(), False), 
                           StructField('year', IntegerType()),
                           StructField('round', IntegerType()),
                           StructField('circuitId', IntegerType()),
                           StructField('name', StringType()), 
                          StructField('date', DateType()), 
                          StructField('time', StringType()), 
                          StructField('url', StringType())])

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Reading the races csv file as a dataframe

# COMMAND ----------

races_df = spark.read.option("header", True)\
                     .schema(races_schema)\
                    .csv(f"/mnt/formula1gokul/raw/{v_file_date}/races.csv")     

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Dropping unwanted columns

# COMMAND ----------

races_selected_df = races_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Renaming column names

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed('raceId', 'race_id')\
                                    .withColumnRenamed('year', 'race_year')\
                                    .withColumnRenamed('circuitId', 'circuit_id')

# COMMAND ----------

races_final_df = races_renamed_df.withColumn("race_timestamp",\
     to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"))\
         .withColumn("ingested_timestamp", current_timestamp())\
        .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy("race_year").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")