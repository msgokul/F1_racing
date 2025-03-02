# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingesting drivers json table from raw container

# COMMAND ----------

from pyspark.sql.types import DateType, IntegerType, StringType, StructType, StructField
from pyspark.sql.functions import col, lit, current_timestamp, concat

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Defining schema for both name field and overall

# COMMAND ----------

name_schema = StructType(fields = [StructField("forename", StringType()),
                                   StructField("surname", StringType())])

# COMMAND ----------

drivers_schema = StructType(fields = [StructField("driverId", IntegerType()),
                                      StructField("driverRef", StringType()),
                                      StructField("number", IntegerType()), 
                                      StructField("code", StringType()), 
                                      StructField("name", name_schema), 
                                      StructField("dob", DateType()), 
                                      StructField("nationality", StringType()), 
                                      StructField("url", StringType())])

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Reading the json file as a dataframe

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json(f"/mnt/formula1gokul/raw/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Handling *name* column and rename other columns

# COMMAND ----------

drivers_renamed_df = drivers_df.withColumnRenamed("driverId", "driver_id").\
                                withColumnRenamed("driverRef", "driver_ref").\
                                withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))).\
                                withColumn("ingested_date", current_timestamp()).\
                                withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Drop unwanted column(s)

# COMMAND ----------

drivers_final_df = drivers_renamed_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Write the final df into processed container in parquet format

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")