-------------------------------------------------------------------------------
-- Script       : 100_Create_DB_Objects.sql
-- @description : Create the tables for data ingestion.
--                
-- Author(s)    : BIG DATA MNL
-- History      : 20161007 Timothy Santos Created
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Create table for ELT of DALY  data
-------------------------------------------------------------------------------

DROP TABLE IF EXISTS life.daly_raw;
CREATE DIMENSION TABLE life.daly_raw
(
Country VARCHAR
,DEN_DALY_1990_Index VARCHAR
,DEN_DALY_1990_Range VARCHAR
,DEN_DALY_2005_Index VARCHAR
,DEN_DALY_2005_Range VARCHAR
,DEN_DALY_2013_Index VARCHAR
,DEN_DALY_2013_Range VARCHAR
,DEN_DALY_PctChg_2005_Index VARCHAR
,DEN_DALY_PctChg_2005_Range VARCHAR
,DEN_DALY_PctChg_2013_Index VARCHAR
,DEN_DALY_PctChg_2013_Range VARCHAR
,DEN_ASDALY_1990_Index VARCHAR
,DEN_ASDALY_1990_Range VARCHAR
,DEN_ASDALY_2005_Index VARCHAR
,DEN_ASDALY_2005_Range VARCHAR
,DEN_ASDALY_2013_Index VARCHAR
,DEN_ASDALY_2013_Range VARCHAR
,DEN_ASDALY_PctChg_2005_Index VARCHAR
,DEN_ASDALY_PctChg_2005_Range VARCHAR
,DEN_ASDALY_PctChg_2013_Index VARCHAR
,DEN_ASDALY_PctChg_2013_Range VARCHAR
);

ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors --skip-rows 1 -c life.daly_raw ghd.csv

DROP TABLE IF EXISTS life.daly;
CREATE DIMENSION TABLE life.daly
( country VARCHAR
 ,yyyy INTEGER
 ,daly_index DOUBLE
 ,daly_range VARCHAR
 ,daly_range_min DOUBLE
 ,daly_range_max DOUBLE
 ,daly_pct_change_index DOUBLE
 ,daly_pct_change_range VARCHAR
 ,daly_pct_change_range_min DOUBLE
 ,daly_pct_change_range_max DOUBLE
 ,asdaly_index DOUBLE
 ,asdaly_range VARCHAR
 ,asdaly_range_min DOUBLE
 ,asdaly_range_max DOUBLE
 ,asdaly_pct_change_index DOUBLE
 ,asdaly_pct_change_range VARCHAR
 ,asdaly_pct_change_range_min DOUBLE
 ,asdaly_pct_change_range_max DOUBLE
);



INSERT INTO life.daly
SELECT country
      ,1990 AS yyyy
      ,CAST(den_daly_1990_index AS DOUBLE) AS daly_index
      ,DEN_DALY_1990_Range AS daly_range
      ,CAST(REGEXP_REPLACE(DEN_DALY_1990_Range,'^[^0-9\.\-]*([0-9\.\-]*).+$','\\1' ) AS DOUBLE) AS daly_range_min
      ,CAST(REGEXP_REPLACE(DEN_DALY_1990_Range,'^.+to ([^\)]*).+','\\1' ) AS DOUBLE) AS daly_range_max
      ,0 AS daly_pct_change_index
      ,'(0 to 0)' AS daly_pct_change_range
      ,0 AS daly_pct_change_range_min
      ,0 AS daly_pct_change_range_max
      ,CAST(DEN_ASDALY_1990_Index AS DOUBLE) AS asdaly_index
      ,DEN_ASDALY_1990_Range AS asdaly_range
      ,CAST(REGEXP_REPLACE(DEN_ASDALY_1990_Range,'^[^0-9\.\-]*([0-9\.\-]*).+$','\\1' ) AS DOUBLE) AS asdaly_range_min
      ,CAST(REGEXP_REPLACE(DEN_ASDALY_1990_Range,'^.+to ([^\)]*).+','\\1' ) AS DOUBLE) AS asdaly_range_max      
      ,0 AS asdaly_pct_change_index
      ,'(0 to 0)' AS asdaly_pct_change_range
      ,0 AS asdaly_pct_change_range_min
      ,0 AS asdaly_pct_change_range_max      
FROM life.daly_raw
WHERE den_daly_1990_index NOT ILIKE '%NA%';
--ORDER BY daly_range_min asc
--LIMIT 5;


INSERT INTO life.daly
SELECT country
      ,2005 AS yyyy
      ,CAST(den_daly_2005_index AS DOUBLE) AS daly_index
      ,DEN_DALY_2005_Range AS daly_range
      ,CAST(REGEXP_REPLACE(DEN_DALY_2005_Range,'^[^0-9\.\-]*([0-9\.\-]*).+$','\\1' ) AS DOUBLE) AS daly_range_min
      ,CAST(REGEXP_REPLACE(DEN_DALY_2005_Range,'^.+to ([^\)]*).+','\\1' ) AS DOUBLE) AS daly_range_max
      ,CAST(DEN_DALY_PctChg_2005_Index AS DOUBLE) AS daly_pct_change_index
      ,DEN_DALY_PctChg_2005_Range AS daly_pct_change_range
      ,CAST(REGEXP_REPLACE(DEN_DALY_PctChg_2005_Range,'^[^0-9\.\-]*([0-9\.\-]*).+$','\\1' ) AS DOUBLE) AS daly_pct_change_range_min
      ,CAST(REGEXP_REPLACE(DEN_DALY_PctChg_2005_Range,'^.+to ([^\)]*).+','\\1' ) AS DOUBLE) AS daly_pct_change_range_max      
      ,CAST(DEN_ASDALY_2005_Index AS DOUBLE) AS asdaly_index
      ,DEN_ASDALY_2005_Range AS asdaly_range
      ,CAST(REGEXP_REPLACE(DEN_ASDALY_2005_Range,'^[^0-9\.\-]*([0-9\.\-]*).+$','\\1' ) AS DOUBLE) AS asdaly_range_min
      ,CAST(REGEXP_REPLACE(DEN_ASDALY_2005_Range,'^.+to ([^\)]*).+','\\1' ) AS DOUBLE) AS asdaly_range_max      
      ,CAST(DEN_ASDALY_PctChg_2005_Index AS DOUBLE) AS asdaly_pct_change_index
      ,DEN_ASDALY_PctChg_2005_Range AS asdaly_pct_change_range
      ,CAST(REGEXP_REPLACE(DEN_ASDALY_PctChg_2005_Range,'^[^0-9\.\-]*([0-9\.\-]*).+$','\\1' ) AS DOUBLE) AS asdaly_pct_change_range_min
      ,CAST(REGEXP_REPLACE(DEN_ASDALY_PctChg_2005_Range,'^.+to ([^\)]*).+','\\1' ) AS DOUBLE) AS asdaly_pct_change_range_max     
FROM life.daly_raw
WHERE den_daly_2005_index NOT ILIKE '%NA%';
ORDER BY daly_range_min asc
LIMIT 5;



INSERT INTO life.daly
SELECT country
      ,2013 AS yyyy
      ,CAST(den_daly_2013_index AS DOUBLE) AS daly_index
      ,DEN_DALY_2013_Range AS daly_range
      ,CAST(REGEXP_REPLACE(DEN_DALY_2013_Range,'^[^0-9\.\-]*([0-9\.\-]*).+$','\\1' ) AS DOUBLE) AS daly_range_min
      ,CAST(REGEXP_REPLACE(DEN_DALY_2013_Range,'^.+to ([^\)]*).+','\\1' ) AS DOUBLE) AS daly_range_max
      ,CAST(DEN_DALY_PctChg_2013_Index AS DOUBLE) AS daly_pct_change_index
      ,DEN_DALY_PctChg_2013_Range AS daly_pct_change_range
      ,CAST(REGEXP_REPLACE(DEN_DALY_PctChg_2013_Range,'^[^0-9\.\-]*([0-9\.\-]*).+$','\\1' ) AS DOUBLE) AS daly_pct_change_range_min
      ,CAST(REGEXP_REPLACE(DEN_DALY_PctChg_2013_Range,'^.+to ([^\)]*).+','\\1' ) AS DOUBLE) AS daly_pct_change_range_max      
      ,CAST(DEN_ASDALY_2013_Index AS DOUBLE) AS asdaly_index
      ,DEN_ASDALY_2013_Range AS asdaly_range
      ,CAST(REGEXP_REPLACE(DEN_ASDALY_2013_Range,'^[^0-9\.\-]*([0-9\.\-]*).+$','\\1' ) AS DOUBLE) AS asdaly_range_min
      ,CAST(REGEXP_REPLACE(DEN_ASDALY_2013_Range,'^.+to ([^\)]*).+','\\1' ) AS DOUBLE) AS asdaly_range_max      
      ,CAST(DEN_ASDALY_PctChg_2013_Index AS DOUBLE) AS asdaly_pct_change_index
      ,DEN_ASDALY_PctChg_2013_Range AS asdaly_pct_change_range
      ,CAST(REGEXP_REPLACE(DEN_ASDALY_PctChg_2013_Range,'^[^0-9\.\-]*([0-9\.\-]*).+$','\\1' ) AS DOUBLE) AS asdaly_pct_change_range_min
      ,CAST(REGEXP_REPLACE(DEN_ASDALY_PctChg_2013_Range,'^.+to ([^\)]*).+','\\1' ) AS DOUBLE) AS asdaly_pct_change_range_max     
FROM life.daly_raw
WHERE den_daly_2013_index NOT ILIKE '%NA%';

GRANT SELECT ON TABLE  life.daly TO PUBLIC;

SELECT * FROM life.daly;