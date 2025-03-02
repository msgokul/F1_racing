# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingesting the circuits csv from raw container

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField, StructField
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Checking for the mounts

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Checking the files in raw container

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1gokul/raw

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Explicitly define schema for the dataframe

# COMMAND ----------

circuits_schema = StructType([StructField("circuitId", IntegerType(), False),
                             StructField("circuitRef", StringType(), True), 
                             StructField("name", StringType(), True), 
                             StructField("location", StringType(), True), 
                             StructField("country", StringType(), True), 
                             StructField("lat", DoubleType(), True),
                             StructField("lng", DoubleType(), True), 
                             StructField("alt", IntegerType(), True),
                             StructField("url", StringType(), True)])

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Read the circuits csv from raw container as a dataframe

# COMMAND ----------

circuits_df = spark.read.option("header", True).schema(circuits_schema).\
                                csv(f"/mnt/formula1gokul/raw/{v_file_date}/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Display the dataframe

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 6. Drop unwanted columns

# COMMAND ----------

circuits_selected_df = circuits_df.drop('url')

# COMMAND ----------

circuits_selected_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 7. Renaming the column names

# COMMAND ----------

circuits_df_renamed = circuits_selected_df.withColumnRenamed('circuitId', 'circuit_id').\
    withColumnRenamed('circuitRef', 'circuit_ref').\
    withColumnRenamed('lat', 'latitude').\
    withColumnRenamed('lng', 'longitude').\
    withColumnRenamed('alt', 'altitude')

# COMMAND ----------

# MAGIC %md
# MAGIC 8. Add new column to the existing df and populate it with current timestamp

# COMMAND ----------

circuits_final_df = circuits_df_renamed.withColumn('ingestion_date', current_timestamp()).\
                                        withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC 9. Write the dataframe as a parquet file

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")