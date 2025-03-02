# Databricks notebook source
def run_with_retry(notebook, timeout, params, max_retries=3):
    num_retries = 0
    while True:
        try:
            return dbutils.notebook.run(notebook, timeout, params)
        except Exception as e:
            if num_retries >= max_retries:
                raise e
            else:
                print(f"Retrying error: {e}")
                num_retries += 1

v_result = run_with_retry("ingest_circuits_file", 0,{"p_file_date": "2021-03-28"})
v_result = run_with_retry("ingest_races_file", 0,{"p_file_date": "2021-03-28"})
v_result = run_with_retry("ingest_constructor_file", 0,{"p_file_date": "2021-03-28"})
v_result = run_with_retry("ingest_driver_file", 0,{"p_file_date": "2021-03-28"})
v_result = run_with_retry("ingest_results_file", 0,{"p_file_date": "2021-03-28"})
v_result = run_with_retry("ingest_pit_stops_file", 0,{"p_file_date": "2021-03-28"})
v_result = run_with_retry("ingest_lap_times_file", 0,{"p_file_date": "2021-03-28"})
v_result = run_with_retry("ingest_qualifying_file", 0,{"p_file_date": "2021-03-28"})

# COMMAND ----------

