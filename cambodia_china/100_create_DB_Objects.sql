-------------------------------------------------------------------------------
-- Script       : 100_Create_DB_Objects.sql
-- @description : Create the tables for data ingestion.
--                
-- Author(s)    : BIG DATA MNL
-- History      : 20161109 Timothy Santos Created
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Create table for ELT of Cambodia-China monthly data for 2010
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS stg.cambodia_china_cases;

CREATE DIMENSION TABLE stg.cambodia_china_cases
(
   rowid SERIAL GLOBAL
  ,data VARCHAR
);

ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors stg.cambodia_china_cases Workbook1.csv -C 'data'

SELECT * FROM stg.cambodia_china_cases
WHERE data ILIKE '%cambodia%'
OR data ILIKE '%china%';

SELECT rowid
      ,REGEXP_REPLACE(data,'^"(.+)",.+','\\1') AS country_region
      ,REGEXP_REPLACE(data,'^".+",','') AS data
FROM stg.cambodia_china_cases
WHERE data ILIKE '%cambodia%'
OR data ILIKE '%china%';

SELECT rowid
      ,REGEXP_REPLACE(data,'^"(.+)",.+','\\1') AS country_region
      ,REGEXP_REPLACE(data,'^".+",','') AS data
FROM stg.cambodia_china_cases
WHERE data ILIKE '%cambodia%'
OR data ILIKE '%china%';


DROP TABLE IF EXISTS stg.cambodia_china_regions;
CREATE DIMENSION TABLE stg.cambodia_china_regions
AS
SELECT DISTINCT REGEXP_REPLACE(data,'^"(.+)",.+','\\1') AS country_region
FROM stg.cambodia_china_cases
WHERE data ILIKE '%cambodia%'
OR data ILIKE '%china%';

SELECT * FROM stg.cambodia_china_regions;

SELECT rowid
      ,REGEXP_REPLACE(data,'^"(.+)",.+','\\1') AS country_region
      ,REGEXP_REPLACE(data,'^".+",','') AS data
FROM stg.cambodia_china_cases
WHERE data ILIKE '%cambodia%'
OR data ILIKE '%china%';

DROP TABLE IF EXISTS stg.cambodia_china_total_cases;
CREATE DIMENSION TABLE stg.cambodia_china_total_cases
AS
SELECT rowid
      ,country_region
      ,token AS data
      ,201000 + position+1 AS yyyymm
FROM text_parser(
ON (
      SELECT rowid
            ,REGEXP_REPLACE(data,'^"(.+)",.+','\\1') AS country_region
            ,REGEXP_REPLACE(data,'^".+",','') AS data
      FROM stg.cambodia_china_cases
      WHERE data ILIKE '%cambodia%'
      OR data ILIKE '%china%'
   )
TEXT_COLUMN('data')
Delimiter(',')
Punctuation('')
CASE_INSENSITIVE('true')
Output_By_Word('true')
) a
WHERE rowid>2
AND   rowid<58
;

SELECT * FROM stg.cambodia_china_total_cases;


DROP TABLE IF EXISTS stg.cambodia_china_df_total_cases;
CREATE DIMENSION TABLE stg.cambodia_china_df_total_cases
AS
SELECT rowid
      ,country_region
      ,token AS data
      ,201000 + position+1 AS yyyymm
FROM text_parser(
ON (
      SELECT rowid
            ,REGEXP_REPLACE(data,'^"(.+)",.+','\\1') AS country_region
            ,REGEXP_REPLACE(data,'^".+",','') AS data
      FROM stg.cambodia_china_cases
      WHERE data ILIKE '%cambodia%'
      OR data ILIKE '%china%'
   )
TEXT_COLUMN('data')
Delimiter(',')
Punctuation('')
CASE_INSENSITIVE('true')
Output_By_Word('true')
) a
WHERE rowid>60
AND   rowid<116
;

SELECT * FROM stg.cambodia_china_df_total_cases;


DROP TABLE IF EXISTS stg.cambodia_china_dhf_dss_total_cases;
CREATE DIMENSION TABLE stg.cambodia_china_dhf_dss_total_cases
AS
SELECT rowid
      ,country_region
      ,token AS data
      ,201000 + position+1 AS yyyymm
FROM text_parser(
ON (
      SELECT rowid
            ,REGEXP_REPLACE(data,'^"(.+)",.+','\\1') AS country_region
            ,REGEXP_REPLACE(data,'^".+",','') AS data
      FROM stg.cambodia_china_cases
      WHERE data ILIKE '%cambodia%'
      OR data ILIKE '%china%'
   )
TEXT_COLUMN('data')
Delimiter(',')
Punctuation('')
CASE_INSENSITIVE('true')
Output_By_Word('true')
) a
WHERE rowid>118
AND   rowid<174
;

SELECT * FROM stg.cambodia_china_dhf_dss_total_cases;



DROP TABLE IF EXISTS stg.cambodia_china_deaths_total;
CREATE DIMENSION TABLE stg.cambodia_china_deaths_total
AS
SELECT rowid
      ,country_region
      ,token AS data
      ,201000 + position+1 AS yyyymm
FROM text_parser(
ON (
      SELECT rowid
            ,REGEXP_REPLACE(data,'^"(.+)",.+','\\1') AS country_region
            ,REGEXP_REPLACE(data,'^".+",','') AS data
      FROM stg.cambodia_china_cases
      WHERE data ILIKE '%cambodia%'
      OR data ILIKE '%china%'
   )
TEXT_COLUMN('data')
Delimiter(',')
Punctuation('')
CASE_INSENSITIVE('true')
Output_By_Word('true')
) a
WHERE rowid>176
AND   rowid<232
;

SELECT * FROM stg.cambodia_china_deaths_total;


DROP TABLE IF EXISTS stg.cambodia_china_fatality_rate_dhf_dss_cases;
CREATE DIMENSION TABLE stg.cambodia_china_fatality_rate_dhf_dss_cases
AS
SELECT rowid
      ,country_region
      ,token AS data
      ,201000 + position+1 AS yyyymm
FROM text_parser(
ON (
      SELECT rowid
            ,REGEXP_REPLACE(data,'^"(.+)",.+','\\1') AS country_region
            ,REGEXP_REPLACE(data,'^".+",','') AS data
      FROM stg.cambodia_china_cases
      WHERE data ILIKE '%cambodia%'
      OR data ILIKE '%china%'
   )
TEXT_COLUMN('data')
Delimiter(',')
Punctuation('')
CASE_INSENSITIVE('true')
Output_By_Word('true')
) a
WHERE rowid>234
AND   rowid<259
;

SELECT * FROM stg.cambodia_china_fatality_rate_dhf_dss_cases;


DROP TABLE IF EXISTS stg.cambodia_china_fatality_rate_total_cases;
CREATE DIMENSION TABLE stg.cambodia_china_fatality_rate_total_cases
AS
SELECT rowid
      ,country_region
      ,token AS data
      ,201000 + position+1 AS yyyymm
FROM text_parser(
ON (
      SELECT rowid
            ,REGEXP_REPLACE(data,'^"(.+)",.+','\\1') AS country_region
            ,REGEXP_REPLACE(data,'^".+",','') AS data
      FROM stg.cambodia_china_cases
      WHERE data ILIKE '%cambodia%'
      OR data ILIKE '%china%'
   )
TEXT_COLUMN('data')
Delimiter(',')
Punctuation('')
CASE_INSENSITIVE('true')
Output_By_Word('true')
) a
WHERE rowid>262
AND   rowid<304
;

SELECT * FROM stg.cambodia_china_fatality_rate_total_cases;



DROP TABLE IF EXISTS stg.cambodia_china_ratio_dh_dss_total_cases;
CREATE DIMENSION TABLE stg.cambodia_china_ratio_dh_dss_total_cases
AS
SELECT rowid
      ,country_region
      ,token AS data
      ,201000 + position+1 AS yyyymm
FROM text_parser(
ON (
      SELECT rowid
            ,REGEXP_REPLACE(data,'^"(.+)",.+','\\1') AS country_region
            ,REGEXP_REPLACE(data,'^".+",','') AS data
      FROM stg.cambodia_china_cases
      WHERE data ILIKE '%cambodia%'
      OR data ILIKE '%china%'
   )
TEXT_COLUMN('data')
Delimiter(',')
Punctuation('')
CASE_INSENSITIVE('true')
Output_By_Word('true')
) a
WHERE rowid>306
AND   rowid<349
;

SELECT * FROM stg.cambodia_china_ratio_dh_dss_total_cases;

--DROP TABLE IF EXISTS life.cambodia_china_dengue_monthly;
--CREATE FACT life.cambodia_china_dengue_monthly
--DISTRIBUTE BY HASH (country_region)
--AS
--SELECT rgn.country_region AS country_region
--      ,tot.yyyymm
--      ,tot.data AS total_cases
--      ,*
--FROM stg.cambodia_china_regions rgn 
--INNER JOIN 
--stg.cambodia_china_total_cases tot
--ON rgn.country_region=tot.country_region
--INNER JOIN
--stg.cambodia_china_df_total_cases df_tot
--ON rgn.country_region=df_tot.country_region
--
--
--DROP TABLE IF EXISTS life.cambodia_china_dengue_monthly;
--CREATE FACT TABLE life.cambodia_china_dengue_monthly
--DISTRIBUTE BY HASH (country_region)
--AS
--SELECT country_region
--      ,yyyymm
--      ,tot.data  total_cases
--
--      ,NULLIF(df_tot.data, 'n/a') AS df_total_cases
--      ,NULLIF(dhf_dss_tot.data, 'n/a') AS dhf_dss_total_cases
--      ,NULLIF(deaths.data, 'n/a') AS deaths
--      ,NULLIF(fatality_rate_dhf_dss.data, 'n/a') AS fatality_rate_dhf_dss
--      ,NULLIF(fatality_rate_total.data, 'n/a') AS fatality_rate_total_cases
--      ,NULLIF(ratio_dh_dss_total.data, 'n/a') AS ratio_dh_dss_total
--FROM   stg.cambodia_china_total_cases tot
--FULL OUTER JOIN
--stg.cambodia_china_df_total_cases df_tot
--USING (country_region, yyyymm)
--FULL OUTER JOIN
--stg.cambodia_china_dhf_dss_total_cases dhf_dss_tot
--USING (country_region,yyyymm)
--FULL OUTER JOIN
--stg.cambodia_china_deaths_total deaths
--USING (country_region,yyyymm)
--FULL OUTER JOIN
--stg.cambodia_china_fatality_rate_dhf_dss_cases  fatality_rate_dhf_dss
--USING (country_region,yyyymm)
--FULL OUTER JOIN
--stg.cambodia_china_fatality_rate_total_cases  fatality_rate_total
--USING (country_region,yyyymm)
--FULL OUTER JOIN
--stg.cambodia_china_ratio_dh_dss_total_cases  ratio_dh_dss_total
--USING (country_region,yyyymm)
--;

select count(*) from life.cambodia_china_dengue_monthly;

DROP TABLE IF EXISTS life.cambodia_china_dengue_monthly;
CREATE FACT TABLE life.cambodia_china_dengue_monthly
DISTRIBUTE BY HASH (country_region)
AS
SELECT country_region
      ,yyyymm
      ,case
		when NULLIF(tot.data, 'n/a') IS NULL  then -9999
		else CAST(tot.data AS INTEGER) 
	   end AS total_cases
      ,case
		when NULLIF(df_tot.data, 'n/a') IS NULL  then -9999
		else CAST(df_tot.data AS INTEGER) 
	   end AS df_total_cases
      ,case
		when NULLIF(dhf_dss_tot.data, 'n/a') IS NULL  then -9999
		else CAST(dhf_dss_tot.data AS INTEGER) 
	   end AS dhf_dss_total_cases
      ,case
		when NULLIF(deaths.data, 'n/a') IS NULL  then -9999
		else CAST(deaths.data AS INTEGER) 
	   end AS deaths
      ,case
		when NULLIF(fatality_rate_dhf_dss.data, 'n/a') IS NULL  then -9999.0
		else CAST(fatality_rate_dhf_dss.data AS DOUBLE) 
	   end AS fatality_rate_dhf_dss
      ,case
		when NULLIF(fatality_rate_total.data, 'n/a') IS NULL  then -9999.0
		else CAST(fatality_rate_total.data AS DOUBLE) 
	   end AS fatality_rate_total_cases
      ,case
		when NULLIF(ratio_dh_dss_total.data, 'n/a') IS NULL  then -9999.0
		else CAST(ratio_dh_dss_total.data AS DOUBLE) 
	   end AS ratio_dh_dss_total
FROM   stg.cambodia_china_total_cases tot
FULL OUTER JOIN
stg.cambodia_china_df_total_cases df_tot
USING (country_region, yyyymm)
FULL OUTER JOIN
stg.cambodia_china_dhf_dss_total_cases dhf_dss_tot
USING (country_region,yyyymm)
FULL OUTER JOIN
stg.cambodia_china_deaths_total deaths
USING (country_region,yyyymm)
FULL OUTER JOIN
stg.cambodia_china_fatality_rate_dhf_dss_cases  fatality_rate_dhf_dss
USING (country_region,yyyymm)
FULL OUTER JOIN
stg.cambodia_china_fatality_rate_total_cases  fatality_rate_total
USING (country_region,yyyymm)
FULL OUTER JOIN
stg.cambodia_china_ratio_dh_dss_total_cases  ratio_dh_dss_total
USING (country_region,yyyymm)
;

GRANT SELECT ON life.cambodia_china_dengue_monthly TO public;

SELECT * FROM  life.cambodia_china_dengue_monthly limit 100;



SELECT CAST(NULLIF(data, 'n/a') AS INTEGER) FROM life.cambodia_china_dengue_monthly

SELECT COUNT(*) FROM life.cambodia_china_dengue_monthly;

SELECT country_region, yyyy, cases FROM life.denguenet_vipr LIMIT  100;