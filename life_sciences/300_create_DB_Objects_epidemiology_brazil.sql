-------------------------------------------------------------------------------
-- Script       : 400_Create_DB_Objects_singapore.sql
-- @description : Create the tables for data ingestion.
--                
-- Author(s)    : BIG DATA MNL
-- History      : 20161107 Timothy Santos Created
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Create table for ELT of Singapore data
-------------------------------------------------------------------------------

DROP TABLE IF EXISTS stg.brazil_mmc1;

CREATE DIMENSION TABLE stg.brazil_mmc1
(
  sequential INTEGER
 ,id_sequence VARCHAR
 ,genbank VARCHAR
 ,location_of_isolation VARCHAR
 ,yyyy INTEGER
);

ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors stg.brazil_mmc1 mmc1.csv -c --skip-rows 1

ALTER TABLE stg.brazil_mmc1
RENAME TO epidemiology_brazil_metadata;

ALTER TABLE stg.epidemiology_brazil_metadata
SET SCHEMA life;

SELECT * FROM life.epidemiology_brazil_metadata;

GRANT SELECT ON life.epidemiology_brazil_metadata TO public;

DROP TABLE IF EXISTS stg.brazil_mmc2;
CREATE DIMENSION TABLE stg.brazil_mmc2
(
  rowid SERIAL GLOBAL
 ,data VARCHAR
);

ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors stg.brazil_mmc2 mmc2.csv -C 'data' 

--DROP TABLE IF EXISTS stg.vipr_country;
--CREATE DIMENSION TABLE stg.vipr_country 
--AS
SELECT token
      ,position
--,rowid, token 
--CAST(token AS INTEGER) AS yyyy , position
FROM text_parser(
ON (
--     SELECT REGEXP_REPLACE(REGEXP_REPLACE(data,'^[^,]+,',''),',',' ','g') AS YYYY FROM stg.vipr_data WHERE rowid>1
     SELECT REGEXP_REPLACE(data,'Genotype.+isolates,','') AS data FROM stg.brazil_mmc2 WHERE rowid=1 
   )
TEXT_COLUMN('data')
Delimiter(',')
CASE_INSENSITIVE('true')
Output_By_Word('true')
) 
;

DROP TABLE IF EXISTS life.brazil_epidemiology_data;
CREATE FACT TABLE life.brazil_epidemiology_data
DISTRIBUTE BY HASH(denv1_isolates)
AS
SELECT b.genotype
      ,b.lineage
      ,case
		when ( (CAST(a.token AS INTEGER) >5 AND CAST(a.token AS INTEGER) <9) OR (CAST(a.token AS INTEGER) >154 AND CAST(a.token AS INTEGER) <172))
		    then 'Protein Domain I'
		when  ( (CAST(a.token AS INTEGER) >54 AND CAST(a.token AS INTEGER) <121) OR (CAST(a.token AS INTEGER) =227 ))
		    then 'Protein Domain II'		    
		when ( (CAST(a.token AS INTEGER) >296 AND CAST(a.token AS INTEGER) <395) )
		    then 'Protein Domain III'		    
		else 'insoluble region'
	   end AS aa_positions
      ,b.denv1_isolates
      ,CAST(a.token AS INTEGER) AS sequence_position
      ,b.token AS value	   
--,rowid, token 
--CAST(token AS INTEGER) AS yyyy , position
FROM text_parser(
ON (
--     SELECT REGEXP_REPLACE(REGEXP_REPLACE(data,'^[^,]+,',''),',',' ','g') AS YYYY FROM stg.vipr_data WHERE rowid>1
     SELECT REGEXP_REPLACE(data,'Genotype.+isolates,','') AS data FROM stg.brazil_mmc2 WHERE rowid=1 
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
      ,genotype
      ,lineage
      ,denv1_isolates
--,rowid, token 
--CAST(token AS INTEGER) AS yyyy , position
FROM text_parser(
ON (
--     SELECT REGEXP_REPLACE(REGEXP_REPLACE(data,'^[^,]+,',''),',',' ','g') AS YYYY FROM stg.vipr_data WHERE rowid>1
     SELECT rowid, data AS dbkp
           ,REGEXP_REPLACE(data,'^([^,]*),[^,]*,[^,]*,.*$','\\1') AS genotype
           ,REGEXP_REPLACE(data,'^[^,]*,([^,]*),[^,]*,.*$','\\1') AS lineage
           ,REGEXP_REPLACE(data,'^[^,]*,[^,]*,([^,]*),.*$','\\1') AS denv1_isolates
           ,REGEXP_REPLACE(data,'^[^,]*,[^,]*,[^,]*,','') AS data 
     FROM stg.brazil_mmc2 WHERE rowid>1
--     GROUP BY denv1_isolates,rowid
--     SELECT * FROM stg.brazil_mmc2 WHERE rowid>1
   )
TEXT_COLUMN('data')
Delimiter(',')
CASE_INSENSITIVE('true')
Output_By_Word('true')
ACCUMULATE('rowid','genotype','lineage','denv1_isolates')
) 

) b
ON a.position=b.position
;

GRANT SELECT ON life.brazil_epidemiology_data TO public;
--WHERE position>3
;


SELECT * FROM LIFE.brazil_epidemiology_data WHERE denv1_isolates='KF672759_BRRJ_2010';

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