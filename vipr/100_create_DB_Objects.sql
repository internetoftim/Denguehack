-------------------------------------------------------------------------------
-- Script       : 100_Create_DB_Objects.sql
-- @description : Create the tables for data ingestion.
--                
-- Author(s)    : BIG DATA MNL
-- History      : 20161107 Timothy Santos Created
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Create table for ELT of VIPR daily data
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS stg.vipr_data;

CREATE DIMENSION TABLE stg.vipr_data
(
  rowid SERIAL GLOBAL
 ,data VARCHAR
);

ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors stg.vipr_data VIPR_data.csv -C data

BEGIN TRANSACTION;

CREATE TEMPORARY TABLE YEARS
DISTRIBUTE BY REPLICATION AS
SELECT CAST(token AS INTEGER) AS yyyy 
FROM text_parser(
ON (
     SELECT REGEXP_REPLACE(REGEXP_REPLACE(data,'^[^,]+,',''),',',' ','g') AS YYYY FROM stg.vipr_data WHERE rowid=1
   )
TEXT_COLUMN('yyyy')
CASE_INSENSITIVE('true')
LIST_POSITIONS('true')
Output_By_Word('true')
) order by yyyy;
                                

SELECT CAST(token AS INTEGER) AS yyyy , position
FROM text_parser(
ON (
     SELECT REGEXP_REPLACE(REGEXP_REPLACE(data,'^[^,]+,',''),',',' ','g') AS YYYY FROM stg.vipr_data WHERE rowid=1
   )
TEXT_COLUMN('yyyy')
CASE_INSENSITIVE('true')
LIST_POSITIONS('true')
Output_By_Word('true')
) order by yyyy;

DROP TABLE IF EXISTS stg.vipr_country;
CREATE DIMENSION TABLE stg.vipr_country 
AS
SELECT rowid, token 
--CAST(token AS INTEGER) AS yyyy , position
FROM text_parser(
ON (
--     SELECT REGEXP_REPLACE(REGEXP_REPLACE(data,'^[^,]+,',''),',',' ','g') AS YYYY FROM stg.vipr_data WHERE rowid>1
     SELECT rowid, REGEXP_REPLACE(data,'Islands,','Islands') AS YYYY  FROM stg.vipr_data WHERE rowid>1 
   )
TEXT_COLUMN('yyyy')
Delimiter(',')
CASE_INSENSITIVE('true')
Output_By_Word('true')
) 
WHERE position=0
order by yyyy
;

SELECT * FROM stg.vipr_country LIMIT 5;

DROP TABLE IF EXISTS stg.denguenet_vipr;
CREATE FACT TABLE stg.denguenet_vipr
DISTRIBUTE BY HASH(country_region)
AS
SELECT 
	  REGEXP_REPLACE(b.token,'^.+(','') AS country
--	  ,REGEXP_REPLACE(b.token,'(^.+) \(.+','\1') AS region
--      ,
       b.token AS country_region
      ,a.token AS cases
      ,case
		when (CAST(position AS INTEGER)<24) then CAST(position AS INTEGER)+1955-1
		when (CAST(position AS INTEGER)>29) then CAST(position AS INTEGER) + 1955 + 1
		else CAST(position AS INTEGER) + 1955
	   end AS yyyy
FROM text_parser(
ON (
     SELECT rowid, REGEXP_REPLACE(data,'Islands,','Islands') AS YYYY FROM stg.vipr_data WHERE rowid>1
   )
TEXT_COLUMN('yyyy')
Delimiter(',')
Punctuation('')
CASE_INSENSITIVE('true')
Output_By_Word('true')
) a
--WHERE CAST(yyyy AS INTEGER)>1954
INNER JOIN
stg.vipr_country b
ON a.rowid=b.rowid
WHERE a.position>0
--order by yyyy
--LIMIT 100
;
SELECT * FROM stg.denguenet_vipr WHERE country_region ilike '%virgin%';
SELECT COUNT(*) FROM stg.denguenet_vipr;
SELECT COUNT(*) FROM stg.denguenet_vipr WHERE cases IS NULL;
SELECT COUNT(*) FROM stg.denguenet_vipr WHERE NOT cases='';
SELECT * FROM stg.denguenet_vipr WHERE cases ILIKE '% %';

.vipr_country LIMIT 5;

DROP TABLE IF EXISTS life.denguenet_vipr;
CREATE FACT TABLE life.denguenet_vipr
DISTRIBUTE BY HASH(country_region)
AS
SELECT 
       country_region
      ,CAST(cases AS INTEGER) AS cases
      ,yyyy
FROM stg.denguenet_vipr 
WHERE NOT cases='';     

SELECT COUNT(*) FROM life.denguenet_vipr;



SELECT * FROM life.denguenet_vipr;
	  
GRANT SELECT ON life.denguenet_vipr TO PUBLIC;

SELECT * FROM life.denguenet_vipr;