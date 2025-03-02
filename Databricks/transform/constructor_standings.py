# Databricks notebook source
# MAGIC %md
# MAGIC ##Create a constructor standings table for each year

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run ../configs

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

from pyspark.sql.functions import col, desc, count, sum, rank, when
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Importing the results join df

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_path}/race_results").filter(col("file_date") == v_file_date)

# COMMAND ----------

race_year_list = column_value_list(race_results_df, "race_year")

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_path}/race_results")\
                            .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

constructor_standings_df = race_results_df.groupBy("race_year", "team")\
                                          .agg(sum("points").alias("total_points"), 
                                               count(when(col("position") == 1, True)).alias("total_wins"))\
                                          .orderBy(desc("total_points"), desc("total_wins"))

# COMMAND ----------

constructor_standings_df = race_results_df.groupBy("race_year", "team")\
                                          .agg(sum("points").alias("total_points"), 
                                               count(when(col("position") == 1, True)).alias("total_wins"))

# COMMAND ----------

constructor_standings_df.filter("race_year = 2021").orderBy(desc("total_points"), desc("total_wins")).show()

# COMMAND ----------

constructor_standings_window_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("total_wins"))

# COMMAND ----------

final_constructor_standings_df = constructor_standings_df.withColumn("rank", rank().over(constructor_standings_window_spec))

# COMMAND ----------

# final_constructor_standings_df.write.mode("overwrite").format("parquet")\
#     .saveAsTable("f1_presentation.constructor_standings")

# overwrite_partition(final_constructor_standings_df, 'f1_presentation', 'constructor_standings', 'race_year')

merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
merge_delta_data(final_constructor_standings_df, 'f1_presentation', 'constructor_standings', presentation_path, merge_condition, 'race_year')

# COMMAND ----------

