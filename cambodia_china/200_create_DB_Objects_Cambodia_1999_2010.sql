-------------------------------------------------------------------------------
-- Script       : 200_Create_DB_Objects_Cambodia_1990_2010.sql
-- @description : Create the tables for data ingestion.
--                
-- Author(s)    : BIG DATA MNL
-- History      : 20161109 Timothy Santos Created
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Create table for ELT of Cambodia-China monthly data for 2010
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS stg.cambodia_1998_2010;

CREATE DIMENSION TABLE stg.cambodia_1998_2010
(
   rowid SERIAL GLOBAL
  ,data VARCHAR
);

ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors stg.cambodia_1998_2010 Cambodia_1998_2010.csv -C 'data'

SELECT * FROM stg.cambodia_1998_2010;




DROP TABLE IF EXISTS life.cambodia_monthly_cases_1998_to_2010;
CREATE FACT TABLE life.cambodia_monthly_cases_1998_to_2010
DISTRIBUTE BY HASH(admin_level1_area)
AS
SELECT 
--a.data
--      ,a.position
       b.admin_level1_area
      ,CAST(REGEXP_REPLACE(a.token,'month.+','') AS INTEGER)*100 + CAST(REGEXP_REPLACE(a.token,'^.+month:','') AS INTEGER) AS yyyymm
      ,CAST(REGEXP_REPLACE(b.token,'n/a','-9999') AS INTEGER) AS cases
--      ,b.denv1_isolates
--      ,CAST(a.token AS INTEGER) AS sequence_position
--      ,b.token AS value	   
--,rowid, token 
--CAST(token AS INTEGER) AS yyyy , position
FROM text_parser(
ON (
--     SELECT REGEXP_REPLACE(REGEXP_REPLACE(data,'^[^,]+,',''),',',' ','g') AS YYYY FROM stg.vipr_data WHERE rowid>1
     SELECT (REGEXP_REPLACE(data,'^[^,]*,','')) AS data FROM stg.cambodia_1998_2010 WHERE rowid=3 
--     SELECT * FROM stg.cambodia_1998_2010 WHERE rowid=3 
   )
TEXT_COLUMN('data')
Delimiter(',')
CASE_INSENSITIVE('true')
Output_By_Word('true')
) a
INNER JOIN
(
SELECT rowid
      ,token
      ,position
      ,admin_level1_area
--,rowid, token 
--CAST(token AS INTEGER) AS yyyy , position
FROM text_parser(
ON (
--     SELECT REGEXP_REPLACE(REGEXP_REPLACE(data,'^[^,]+,',''),',',' ','g') AS YYYY FROM stg.vipr_data WHERE rowid>1
     SELECT rowid
           ,data AS dbkp
           ,REGEXP_REPLACE(data,'^"([^"]*)",.*$','\\1') AS admin_level1_area
           ,REGEXP_REPLACE(data,'^"([^"]*)",(.*)$','\\2') AS data
     FROM stg.cambodia_1998_2010 WHERE rowid>3
--     GROUP BY denv1_isolates,rowid
--     SELECT * FROM stg.brazil_mmc2 WHERE rowid>1
   )
TEXT_COLUMN('data')
Delimiter(',')
CASE_INSENSITIVE('true')
Output_By_Word('true')
ACCUMULATE('admin_level1_area','rowid')
) 
) b
ON a.position=b.position
;

GRANT SELECT ON life.cambodia_monthly_cases_1998_to_2010 TO public;

SELECT * FROM life.cambodia_monthly_cases_1998_to_2010;

DROP TABLE IF EXISTS stg.cambodia_1993_2009;

CREATE DIMENSION TABLE stg.cambodia_1993_2009
(
   rowid SERIAL GLOBAL
  ,data VARCHAR
);

ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors stg.cambodia_1993_2009 Tycho_DengueCasesDHDHF_Cambodia_1993-2009.csv -C 'data'





DROP TABLE IF EXISTS life.cambodia_monthly_cases_1993_to_2009;
CREATE FACT TABLE life.cambodia_monthly_cases_1993_to_2009
DISTRIBUTE BY HASH(admin_level1_area)
AS
SELECT 
--a.data
--      a.position
--       b.admin_level1_area
--      ,CAST(REGEXP_REPLACE(a.token,'month.+','') AS INTEGER)*100 + CAST(REGEXP_REPLACE(a.token,'^.+month:','') AS INTEGER) AS yyyymm
--      ,CAST(REGEXP_REPLACE(b.token,'n/a','-9999') AS INTEGER) AS cases
--      ,b.denv1_isolates
--      ,CAST(a.token AS INTEGER) AS sequence_position
--      ,b.token AS value	   
--,rowid, 
      REGEXP_REPLACE(a.token,'^ ','') AS admin_level1_area
      ,b.yyyymm
--      ,b.token AS cases
      ,case b.token
		when '-' then -9999
		else CAST(b.token AS INTEGER)
	   end AS cases
--CAST(token AS INTEGER) AS yyyy , position
FROM text_parser(
ON (
--     SELECT REGEXP_REPLACE(REGEXP_REPLACE(data,'^[^,]+,',''),',',' ','g') AS YYYY FROM stg.vipr_data WHERE rowid>1
     SELECT (REGEXP_REPLACE(data,'^[^,]*,[^,]*,','')) AS data 
     FROM stg.cambodia_1993_2009 
     WHERE rowid=3 
--     SELECT * FROM stg.cambodia_1998_2010 WHERE rowid=3 
   )
TEXT_COLUMN('data')
Delimiter(',')
CASE_INSENSITIVE('true')
Output_By_Word('true')
) a
INNER JOIN
(
SELECT rowid
      ,token
      ,position
      ,CAST(yyyy AS INTEGER)*100 + CAST(mm AS INTEGER) AS yyyymm
--,rowid, token 
--CAST(token AS INTEGER) AS yyyy , position
FROM text_parser(
ON (
--     SELECT REGEXP_REPLACE(REGEXP_REPLACE(data,'^[^,]+,',''),',',' ','g') AS YYYY FROM stg.vipr_data WHERE rowid>1
     SELECT rowid
           ,data AS dbkp
           ,REGEXP_REPLACE(data,'^([^,]*),.*$','\\1') AS yyyy
           ,REGEXP_REPLACE(data,'^([^,]*),([^,]*),.*$','\\2') AS mm
           ,REGEXP_REPLACE(data,'^([^,]*),([^,]*),(.*)$','\\3') AS data
     FROM stg.cambodia_1993_2009 WHERE rowid>3
--     GROUP BY denv1_isolates,rowid
--     SELECT * FROM stg.brazil_mmc2 WHERE rowid>1
   )
TEXT_COLUMN('data')
Delimiter(',')
CASE_INSENSITIVE('true')
Output_By_Word('true')
ACCUMULATE('rowid','yyyy','mm')
) 
) b
ON a.position=b.position
;

GRANT SELECT ON life.cambodia_monthly_cases_1993_to_2009 TO public;

SELECT * FROM life.cambodia_monthly_cases_1993_to_2009;



DROP TABLE IF EXISTS stg.cambodia_1993_2009_revised;

CREATE DIMENSION TABLE stg.cambodia_1993_2009_revised
(
   country_iso2_id VARCHAR
  ,country_name VARCHAR
  ,admin_lvl1_id VARCHAR
  ,admin_lvl1_name VARCHAR
  ,yyyymmdd INTEGER
  ,dengue_total_cases INTEGER
);

ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors stg.cambodia_1993_2009_revised khm_denguenet_data_refactored.csv  -c --skip-rows 1

SELECT * FROM life.cambodia_monthly_cases_1993_to_2009 LIMIT 100;

SELECT * FROM stg.cambodia_1993_2009_revised LIMIT 100;

ALTER TABLE life.cambodia_monthly_cases_1993_to_2009 SET SCHEMA stg;
ALTER TABLE stg.cambodia_1993_2009_revised SET SCHEMA life;
ALTER TABLE life.cambodia_1993_2009_revised RENAME TO cambodia_monthly_cases_1993_to_2009;

GRANT SELECT ON life.cambodia_monthly_cases_1993_to_2009 TO public;


