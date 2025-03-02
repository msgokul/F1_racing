-- Databricks notebook source
show tables in f1_presentation

-- COMMAND ----------

describe f1_presentation.calculated_race_results

-- COMMAND ----------

select team, count(1) as total_races, 
      sum(calculated_points) as total_points, 
      avg(calculated_points) as average_points, 
      rank() over (order by avg(calculated_points) desc) as race_rank
from f1_presentation.calculated_race_results
group by team
having total_races > 100
order by average_points desc

-- COMMAND ----------

select team, count(1) as total_races, 
      sum(calculated_points) as total_points, 
      avg(calculated_points) as average_points, 
      rank() over (order by avg(calculated_points) desc) as race_rank
from f1_presentation.calculated_race_results
where race_year between 2000 and 2010
group by team
having total_races > 100
order by average_points desc

-- COMMAND ----------

select team, count(1) as total_races, 
      sum(calculated_points) as total_points, 
      avg(calculated_points) as average_points, 
      rank() over (order by avg(calculated_points) desc) as race_rank
from f1_presentation.calculated_race_results
where race_year between 1991 and 2000
group by team
having total_races > 50
order by average_points desc

-- COMMAND ----------

create or replace temp view v_dominant_teams as
select race_year, team, count(1) as total_races, 
      sum(calculated_points) as total_points, 
      avg(calculated_points) as average_points, 
      rank() over (order by avg(calculated_points) desc) as race_rank
from f1_presentation.calculated_race_results
group by race_year, team
order by race_year asc, average_points desc

-- COMMAND ----------

select race_year, team, count(1) as total_races, 
      sum(calculated_points) as total_points, 
      avg(calculated_points) as average_points, 
      rank() over (order by avg(calculated_points) desc) as race_rank
from f1_presentation.calculated_race_results
group by race_year, team
order by race_year asc, average_points desc

-- COMMAND ----------

