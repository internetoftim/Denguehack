-------------------------------------------------------------------------------
-- Script       : 100_Create_DB_Objects.sql
-- @description : Create the tables for data ingestion.
--                
-- Author(s)    : BIG DATA MNL
-- History      : 20161020 Timothy Santos Created
--
-------------------------------------------------------------------------------


DROP TABLE IF EXISTS stg.singapore_dengue_cluster;
CREATE DIMENSION TABLE stg.singapore_dengue_cluster
( rowid SERIAL GLOBAL
 ,data VARCHAR
);
--
--(  FID INTEGER
--  ,OBJECTID INTEGER
--  ,JOIN_COUNT INTEGER
--  ,AREANAME VARCHAR
--  ,DETAIL VARCHAR
--  ,INC_CRC VARCHAR
--  ,FMEL_UPD_D VARCHAR
--  ,X_ADDR DOUBLE
--  ,Y_ADDR DOUBLE
--  ,SHAPE_Leng DOUBLE
--  ,SHAPE_Area DOUBLE
--);


DROP TABLE IF EXISTS stg.singapore_dengue_cluster;
CREATE DIMENSION TABLE stg.singapore_dengue_cluster
( rowid SERIAL GLOBAL
 ,data VARCHAR
);
ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors --skip-rows 1 stg.singapore_dengue_cluster  DENGUE_CLUSTER.txt -C 'data'

--
DROP TABLE IF EXISTS stg.singapore_dengue_cluster;
CREATE DIMENSION TABLE stg.singapore_dengue_cluster
(  FID INTEGER
  ,OBJECTID INTEGER
  ,LOCALITY VARCHAR
  ,CASE_SIZE INTEGER
  ,NAME VARCHAR
  ,HYPERLINK VARCHAR
  ,INC_CRC VARCHAR
  ,FMEL_UPD_D VARCHAR
  ,X_ADDR DOUBLE
  ,Y_ADDR DOUBLE
  ,SHAPE_Leng DOUBLE
  ,SHAPE_Area DOUBLE
);


ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors --skip-rows 1 stg.singapore_dengue_cluster  DENGUE_CLUSTER.txt -c

SELECT * FROM stg.singapore_dengue_cluster LIMIT 5;

DROP TABLE IF EXISTS stg.singapore_dengue_case_central_area;
CREATE DIMENSION TABLE stg.singapore_dengue_case_central_area
(  FID INTEGER
  ,OBJECTID INTEGER
  ,JOIN_COUNT INTEGER
  ,AREANAME VARCHAR
  ,DETAIL VARCHAR
  ,INC_CRC VARCHAR
  ,FMEL_UPD_D VARCHAR
  ,X_ADDR DOUBLE
  ,Y_ADDR DOUBLE
  ,SHAPE_Leng DOUBLE
  ,SHAPE_Area DOUBLE
);


ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors --skip-rows 1 stg.singapore_dengue_case_central_area  DengueCase_Central_Area.txt -c


DROP TABLE IF EXISTS stg.singapore_dengue_case_northeast_area;
CREATE DIMENSION TABLE stg.singapore_dengue_case_northeast_area
(  FID INTEGER
  ,OBJECTID INTEGER
  ,JOIN_COUNT INTEGER
  ,AREANAME VARCHAR
  ,DETAIL VARCHAR
  ,INC_CRC VARCHAR
  ,FMEL_UPD_D VARCHAR
  ,X_ADDR DOUBLE
  ,Y_ADDR DOUBLE
  ,SHAPE_Leng DOUBLE
  ,SHAPE_Area DOUBLE
);


ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors --skip-rows 1 stg.singapore_dengue_case_northeast_area  DengueCase_Northeast_Area.txt -c


FID,OBJECTID,JOIN_COUNT,AREANAME,DETAIL,INC_CRC,FMEL_UPD_D,X_ADDR,Y_ADDR,SHAPE_Leng,SHAPE_Area


DROP TABLE IF EXISTS stg.singapore_dengue_case_northwest_area;
CREATE DIMENSION TABLE stg.singapore_dengue_case_northwest_area
(  FID INTEGER
  ,OBJECTID INTEGER
  ,JOIN_COUNT INTEGER
  ,AREANAME VARCHAR
  ,DETAIL VARCHAR
  ,INC_CRC VARCHAR
  ,FMEL_UPD_D VARCHAR
  ,X_ADDR DOUBLE
  ,Y_ADDR DOUBLE
  ,SHAPE_Leng DOUBLE
  ,SHAPE_Area DOUBLE
);


ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors --skip-rows 1 stg.singapore_dengue_case_northwest_area  DengueCase_Northwest_Area.txt -c

DROP TABLE IF EXISTS stg.singapore_dengue_case_southeast_area;
CREATE DIMENSION TABLE stg.singapore_dengue_case_southeast_area
(  FID INTEGER
  ,OBJECTID INTEGER
  ,JOIN_COUNT INTEGER
  ,AREANAME VARCHAR
  ,DETAIL VARCHAR
  ,INC_CRC VARCHAR
  ,FMEL_UPD_D VARCHAR
  ,X_ADDR DOUBLE
  ,Y_ADDR DOUBLE
  ,SHAPE_Leng DOUBLE
  ,SHAPE_Area DOUBLE
);


ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors --skip-rows 1 stg.singapore_dengue_case_southeast_area  DengueCase_Southeast_Area.txt -c


DROP TABLE IF EXISTS stg.singapore_dengue_case_southwest_area;
CREATE DIMENSION TABLE stg.singapore_dengue_case_southwest_area
(  FID INTEGER
  ,OBJECTID INTEGER
  ,JOIN_COUNT INTEGER
  ,AREANAME VARCHAR
  ,DETAIL VARCHAR
  ,INC_CRC VARCHAR
  ,FMEL_UPD_D VARCHAR
  ,X_ADDR DOUBLE
  ,Y_ADDR DOUBLE
  ,SHAPE_Leng DOUBLE
  ,SHAPE_Area DOUBLE
);


ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors --skip-rows 1 stg.singapore_dengue_case_southwest_area  DengueCase_Southwest_Area.txt -c



DROP TABLE IF EXISTS stg.singapore_breeding_habitat_northwest_area;
CREATE DIMENSION TABLE stg.singapore_breeding_habitat_northwest_area
(  FID INTEGER
  ,OBJECTID INTEGER
  ,JOIN_COUNT INTEGER
  ,AREANAME VARCHAR
  ,DETAIL VARCHAR
  ,INC_CRC VARCHAR
  ,FMEL_UPD_D VARCHAR
  ,X_ADDR DOUBLE
  ,Y_ADDR DOUBLE
  ,SHAPE_Leng DOUBLE
  ,SHAPE_Area DOUBLE
);


ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors --skip-rows 1 stg.singapore_breeding_habitat_northwest_area  BreedingHabitat_Northwest_Area.txt -c


DROP TABLE IF EXISTS stg.singapore_breeding_habitat_southeast_area;
CREATE DIMENSION TABLE stg.singapore_breeding_habitat_southeast_area
(  FID INTEGER
  ,OBJECTID INTEGER
  ,JOIN_COUNT INTEGER
  ,AREANAME VARCHAR
  ,DETAIL VARCHAR
  ,INC_CRC VARCHAR
  ,FMEL_UPD_D VARCHAR
  ,X_ADDR DOUBLE
  ,Y_ADDR DOUBLE
  ,SHAPE_Leng DOUBLE
  ,SHAPE_Area DOUBLE
);


ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors --skip-rows 1 stg.singapore_breeding_habitat_southeast_area  BreedingHabitat_Southeast_Area.txt -c


DROP TABLE IF EXISTS stg.singapore_breeding_habitat_southwest_area;
CREATE DIMENSION TABLE stg.singapore_breeding_habitat_southwest_area
(  FID INTEGER
  ,OBJECTID INTEGER
  ,JOIN_COUNT INTEGER
  ,AREANAME VARCHAR
  ,DETAIL VARCHAR
  ,INC_CRC VARCHAR
  ,FMEL_UPD_D VARCHAR
  ,X_ADDR DOUBLE
  ,Y_ADDR DOUBLE
  ,SHAPE_Leng DOUBLE
  ,SHAPE_Area DOUBLE
);


ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors --skip-rows 1 stg.singapore_breeding_habitat_southwest_area  BreedingHabitat_Southwest_Area.txt -c


CREATE TABLE life.singapore_breeding_habitat
DISTRIBUTE BY HASH(objectid) 
AS
SELECT FID 
      ,OBJECTID 
      ,JOIN_COUNT 
      ,AREANAME 
      ,cast('southwest area' AS VARCHAR) AS district
      ,DETAIL 
      ,INC_CRC 
      ,FMEL_UPD_D 
      ,X_ADDR 
      ,Y_ADDR 
      ,SHAPE_Leng 
      ,SHAPE_Area 
FROM stg.singapore_breeding_habitat_southwest_area;

INSERT INTO life.singapore_breeding_habitat
SELECT FID 
      ,OBJECTID 
      ,JOIN_COUNT 
      ,AREANAME 
      ,cast('southeast area' AS VARCHAR) AS district
      ,DETAIL 
      ,INC_CRC 
      ,FMEL_UPD_D 
      ,X_ADDR 
      ,Y_ADDR 
      ,SHAPE_Leng 
      ,SHAPE_Area 
FROM stg.singapore_breeding_habitat_southeast_area;

INSERT INTO life.singapore_breeding_habitat
SELECT FID 
      ,OBJECTID 
      ,JOIN_COUNT 
      ,AREANAME 
      ,cast('northwest area' AS VARCHAR) AS district
      ,DETAIL 
      ,INC_CRC 
      ,FMEL_UPD_D 
      ,X_ADDR 
      ,Y_ADDR 
      ,SHAPE_Leng 
      ,SHAPE_Area 
FROM stg.singapore_breeding_habitat_northwest_area;
 


CREATE TABLE life.singapore_dengue_cases
DISTRIBUTE BY HASH(objectid) 
AS
SELECT FID 
      ,OBJECTID 
      ,JOIN_COUNT 
      ,AREANAME 
      ,cast('southwest area' AS VARCHAR) AS district
      ,DETAIL 
      ,INC_CRC 
      ,FMEL_UPD_D 
      ,X_ADDR 
      ,Y_ADDR 
      ,SHAPE_Leng 
      ,SHAPE_Area 
FROM stg.singapore_dengue_case_southwest_area;

INSERT INTO life.singapore_dengue_cases
SELECT FID 
      ,OBJECTID 
      ,JOIN_COUNT 
      ,AREANAME 
      ,cast('southeast area' AS VARCHAR) AS district
      ,DETAIL 
      ,INC_CRC 
      ,FMEL_UPD_D 
      ,X_ADDR 
      ,Y_ADDR 
      ,SHAPE_Leng 
      ,SHAPE_Area 
FROM stg.singapore_dengue_case_southeast_area;

INSERT INTO life.singapore_dengue_cases
SELECT FID 
      ,OBJECTID 
      ,JOIN_COUNT 
      ,AREANAME 
      ,cast('northwest area' AS VARCHAR) AS district
      ,DETAIL 
      ,INC_CRC 
      ,FMEL_UPD_D 
      ,X_ADDR 
      ,Y_ADDR 
      ,SHAPE_Leng 
      ,SHAPE_Area 
FROM stg.singapore_dengue_case_northwest_area;

INSERT INTO life.singapore_dengue_cases
SELECT FID 
      ,OBJECTID 
      ,JOIN_COUNT 
      ,AREANAME 
      ,cast('northeast area' AS VARCHAR) AS district
      ,DETAIL 
      ,INC_CRC 
      ,FMEL_UPD_D 
      ,X_ADDR 
      ,Y_ADDR 
      ,SHAPE_Leng 
      ,SHAPE_Area 
FROM stg.singapore_dengue_case_northeast_area;

INSERT INTO life.singapore_dengue_cases
SELECT FID 
      ,OBJECTID 
      ,JOIN_COUNT 
      ,AREANAME 
      ,cast('northeast area' AS VARCHAR) AS district
      ,DETAIL 
      ,INC_CRC 
      ,FMEL_UPD_D 
      ,X_ADDR 
      ,Y_ADDR 
      ,SHAPE_Leng 
      ,SHAPE_Area 
FROM stg.singapore_dengue_case_central_area;
singapore_dengue_case_central_area

ALTER TABLE stg.singapore_dengue_cluster SET SCHEMA life;

SELECT * FROM life.singapore_dengue_cluster;
SELECT * FROM life.singapore_dengue_cases;
SELECT * FROM life.singapore_breeding_habitat;
SELECT  COUNT(*) FROM life.singapore_dengue_cluster;
SELECT COUNT(*) FROM life.singapore_dengue_cases;
SELECT COUNT(*) FROM life.singapore_breeding_habitat;

GRANT SELECT ON TABLE  life.singapore_dengue_cluster TO public;
GRANT SELECT ON TABLE  life.singapore_dengue_cases TO public;
GRANT SELECT ON TABLE  life.singapore_breeding_habitat TO public;

SELECT * FROM stg.singapore_dengue_case_southwest_area;

