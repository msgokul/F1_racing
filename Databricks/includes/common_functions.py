# Databricks notebook source
from delta.tables import DeltaTable

# COMMAND ----------

def rearrange_partitions_column(input_df, partition_column):
    column_list = input_df.columns
    column_list.remove(partition_column)
    column_list.append(partition_column)
    output_df = input_df.select(column_list)
    return output_df

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
    output_df = rearrange_partitions_column(input_df, partition_column)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.mode("overwrite")\
                .partitionBy(partition_column).format("parquet")\
                .saveAsTable(f"{db_name}.{table_name}")



# COMMAND ----------

def column_value_list(input_df, column_name):
    df_row_list = input_df.select(column_name).distinct().collect()
    column_list = [row[column_name] for row in df_row_list]
    return column_list

# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        delta_table = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
        delta_table.alias("tgt")\
                .merge(input_df.alias("src"), merge_condition)\
                .whenMatchedUpdateAll()\
                .whenNotMatchedInsertAll()\
                .execute()
    else:
        input_df.write.mode("overwrite")\
                        .partitionBy(partition_column)\
                        .format("delta")\
                        .saveAsTable(f"{db_name}.{table_name}")


# COMMAND ----------

