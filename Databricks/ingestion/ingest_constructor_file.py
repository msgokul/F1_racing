# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingesting constructors json file from raw container

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Defining schema and reading constructors json file as a dataframe

# COMMAND ----------

constructors_schema =  "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"
constructors_df = spark.read.schema(constructors_schema)\
                        .json(f"/mnt/formula1gokul/raw/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Dropping unwanted column

# COMMAND ----------

constructors_df_dropped = constructors_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Renaming the column names

# COMMAND ----------

constructors_df_renamed = constructors_df_dropped.withColumnRenamed("constructorId", "constructor_id")\
                                        .withColumnRenamed("constructorRef", "constructor_ref")

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Adding the column *ingestion_date*

# COMMAND ----------

constructors_df_final = constructors_df_renamed.withColumn("ingestion_date", current_timestamp())\
                                        .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Writing the final df into processed container as a parquet file

# COMMAND ----------

constructors_df_final.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")