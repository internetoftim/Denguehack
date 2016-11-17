-------------------------------------------------------------------------------
-- Script       : 100_Create_Objects.sql
-- @description : Create the tables for data ingestion.
--                
-- Author(s)    : BIG DATA MNL
-- History      : 20161020 Timothy Santos Created
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------

CREATE SCHEMA life;
--grant all on schema social to ts186045;
GRANT CREATE ON SCHEMA social TO ts186045;
GRANT USAGE ON SCHEMA social TO public;
--SELECT * FROM nc_group_members;

-- Create table for ELT of Iquitos Training data
-- Raw file has no header
--
--"season","season_week","week_start_date","denv1_cases","denv2_cases","denv3_cases                                                                                                            ","denv4_cases","other_positive_cases","total_cases"
--","denv4_cases","other_positive_cases","total_cases"

DROP TABLE IF EXISTS life.iquitos_training_data_stg;
DROP TABLE IF EXISTS life.iquitos_training_data_stg;
CREATE FACT TABLE life.iquitos_training_data_stg
(  season VARCHAR 
  ,season_week INTEGER
  ,week_start_date VARCHAR
  ,denv1_cases INTEGER
  ,denv2_cases INTEGER
  ,denv3_cases INTEGER
  ,denv4_cases INTEGER
  ,other_positive_cases INTEGER
  ,total_cases INTEGER
)
DISTRIBUTE BY HASH(season);

nohup ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors -c life.iquitos_training_data_stg  Iquitos_Training_Data.csv &

-- 11134126 rows
--GRANT SELECT ON TABLE life.iquitos_training_data TO public;
--ALTER TABLE life.iquitos_training_data RENAME TO iquitos_training_data_stg;
SELECT * FROM life.iquitos_training_data_stg LIMIT 5;

DROP TABLE IF EXISTS life.iquitos_training_data;
CREATE FACT TABLE life.iquitos_training_data
DISTRIBUTE BY HASH (season)
AS
SELECT season
      ,season_week 
      ,CAST(REGEXP_REPLACE(week_start_date,'[- ]','','g') AS INT) AS yyyymmdd
      ,denv1_cases
      ,denv2_cases 
      ,denv3_cases 
      ,denv4_cases 
      ,other_positive_cases 
      ,total_cases 
FROM life.iquitos_training_data_stg
;

SELECT * FROM life.iquitos_training_data limit 5;
2003/2004 1           20030702 0           0           1           0           1                    2

-------------------------------------------------------------------------------
-- Create table for ELT of Iquitos Training data
-- Raw file has no header
--
--"season","season_week","week_start_date","denv1_cases","denv2_cases","denv3_cases"
--,"denv4_cases","other_positive_cases","additional_cases","total_cases"

DROP TABLE IF EXISTS life.san_juan_training_data_stg;
CREATE FACT TABLE life.san_juan_training_data_stg
(  season VARCHAR 
  ,season_week INTEGER
  ,week_start_date VARCHAR
  ,denv1_cases INTEGER
  ,denv2_cases INTEGER
  ,denv3_cases INTEGER
  ,denv4_cases INTEGER
  ,other_positive_cases INTEGER
  ,additional_cases INTEGER
  ,total_cases INTEGER
)
DISTRIBUTE BY HASH(season);


nohup ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors -c life.san_juan_training_data_stg  San_Juan_Training_Data.csv &



DROP TABLE IF EXISTS life.san_juan_training_data;
CREATE FACT TABLE life.san_juan_training_data
DISTRIBUTE BY HASH (season)
AS
SELECT season
      ,season_week 
      ,CAST(REGEXP_REPLACE(week_start_date,'[- ]','','g') AS INT) AS yyyymmdd
      ,denv1_cases
      ,denv2_cases 
      ,denv3_cases 
      ,denv4_cases 
      ,other_positive_cases 
      ,additional_cases 
      ,total_cases 
FROM life.san_juan_training_data_stg
;
-- 11134126 rows
GRANT SELECT ON TABLE life.iquitos_training_data TO public;
SELECT * FROM life.san_juan_training_data LIMIT 10;