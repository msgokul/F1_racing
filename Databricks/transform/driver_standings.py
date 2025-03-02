# Databricks notebook source
# MAGIC %md
# MAGIC ##Generate the Driver Standings of all year

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../configs"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import col, sum, countDistinct, count, when, desc, rank
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Calling the joined results df

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{presentation_path}/race_results").filter(col("file_date") == v_file_date)

# COMMAND ----------

race_year_list = column_value_list(results_df,"race_year")

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{presentation_path}/race_results").filter(col("race_year").isin(race_year_list))

# COMMAND ----------

driver_standing_df = results_df.groupBy("race_year", "driver_nationality", "driver_name")\
                               .agg(sum("points").alias("total_points"),
                                    count(when(col("position") == 1, True)).alias("total_wins"))

# COMMAND ----------

driver_standing_window = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("total_wins"))  

# COMMAND ----------

final_driver_standing_df = driver_standing_df.withColumn("rank", rank().over(driver_standing_window))

# COMMAND ----------

# final_driver_standing_df.write.mode("overwrite").format("parquet")\
#     .saveAsTable("f1_presentation.driver_standings")

# overwrite_partition(final_driver_standing_df, 'f1_presentation', 'driver_standings', 'race_year')

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"
merge_delta_data(final_driver_standing_df, 'f1_presentation', 'driver_standings', presentation_path, merge_condition, 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_year, COUNT(1)
# MAGIC   FROM f1_presentation.driver_standings
# MAGIC  GROUP BY race_year
# MAGIC  ORDER BY race_year DESC;