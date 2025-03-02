-- Databricks notebook source
show tables in f1_presentation

-- COMMAND ----------

describe f1_presentation.calculated_race_results

-- COMMAND ----------

select driver_name, count(1) as total_races, 
      sum(calculated_points) as total_points, 
      avg(calculated_points) as average_points, 
      rank() over (order by avg(calculated_points) desc) as race_rank
from f1_presentation.calculated_race_results
group by driver_name
having total_races > 50
order by average_points desc

-- COMMAND ----------

select driver_name, count(1) as total_races, 
      sum(calculated_points) as total_points, 
      avg(calculated_points) as average_points, 
      rank() over (order by avg(calculated_points) desc) as race_rank
from f1_presentation.calculated_race_results
where race_year between 2001 and 2010
group by driver_name
having total_races > 50
order by average_points desc

-- COMMAND ----------

select driver_name, count(1) as total_races, 
      sum(calculated_points) as total_points, 
      avg(calculated_points) as average_points, 
      rank() over (order by avg(calculated_points) desc) as race_rank
from f1_presentation.calculated_race_results
where race_year between 1991 and 2000
group by driver_name
having total_races > 50
order by average_points desc

-- COMMAND ----------

create or replace temp view v_dominant_drivers
as
select race_year,driver_name, count(1) as total_races, 
      sum(calculated_points) as total_points, 
      avg(calculated_points) as average_points, 
      rank() over (order by avg(calculated_points) desc) as race_rank
from f1_presentation.calculated_race_results
group by driver_name, race_year
order by average_points desc

-- COMMAND ----------

select race_year, driver_name, count(1) as total_races, 
      sum(calculated_points) as total_points, 
      avg(calculated_points) as average_points
from f1_presentation.calculated_race_results
where driver_name in (select driver_name from v_dominant_drivers where race_rank <= 10)
group by driver_name, race_year
order by race_year, average_points desc

-- COMMAND ----------

