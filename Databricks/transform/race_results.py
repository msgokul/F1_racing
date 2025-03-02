# Databricks notebook source
# MAGIC %md
# MAGIC ###Joining multiple tables to get comprehensive race results

# COMMAND ----------

# MAGIC %run ../includes/common_functions
# MAGIC

# COMMAND ----------

# MAGIC %run ../configs

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_path}/circuits").withColumnRenamed("name", "circuit_name")
drivers_df = spark.read.format("delta").load(f"{processed_path}/drivers").withColumnRenamed("name", "driver_name").withColumnRenamed("nationality", "driver_nationality")
races_df = spark.read.format("delta").load(f"{processed_path}/races").withColumnRenamed("name", "race_name").withColumnRenamed("date", "race_date")
results_df = spark.read.format("delta").load(f"{processed_path}/results").withColumnRenamed("race_id","results_race_id")\
                                                            .filter(f"file_date = '{v_file_date}'" )
constructor_df = spark.read.format("delta").load(f"{processed_path}/constructors").withColumnRenamed("name", "team")

# COMMAND ----------

race_circuit_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, how='inner').\
                           select(races_df.race_year,races_df.race_date,races_df.race_id, races_df.race_name,races_df.round,circuits_df.circuit_name, circuits_df.location, circuits_df.country)  

# COMMAND ----------

results_join_df = results_df.join(drivers_df, results_df.driver_id == drivers_df.driver_id, how='inner')\
                            .join(constructor_df, results_df.constructor_id == constructor_df.constructor_id, how='inner')\
                            .join(race_circuit_df, results_df.results_race_id == race_circuit_df.race_id, how='inner')\
                            

# COMMAND ----------

results_join_df.printSchema()

# COMMAND ----------

  # Replace with actual DataFrame initialization
final_df = results_join_df.select("race_id","race_date", "race_name", "race_year","driver_nationality", "driver_name","team","circuit_name", "position_order", "position_text", "fastest_lap_time", "fastest_lap_speed","position", "points", "location").withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

# overwrite_partition(final_df, "f1_presentation", "race_results", "race_id")

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_presentation', 'race_results', presentation_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1)
# MAGIC from f1_presentation.race_results
# MAGIC group by race_id
# MAGIC order by race_id desc

# COMMAND ----------

