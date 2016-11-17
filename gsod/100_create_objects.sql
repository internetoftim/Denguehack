-------------------------------------------------------------------------------
-- Script       : 100_Create_DB_Objects.sql
-- @description : Create the tables for data ingestion.
--                
-- Author(s)    : BIG DATA MNL
-- History      : 20161007 Timothy Santos Created
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Create table for ELT of GSOD daily data
-------------------------------------------------------------------------------

DROP TABLE IF EXISTS earth.gsod_raw;

CREATE DIMENSION TABLE earth.gsod_raw
(   data VARCHAR	
);

GRANT SELECT ON TABLE earth.gsod_raw TO public;

DROP TABLE IF EXISTS earth.gsod;
CREATE FACT TABLE earth.gsod 
DISTRIBUTE BY HASH (stn)
AS
SELECT *
FROM UNPACK (
              ON (
                   SELECT * 
                   FROM TextTokenizer (
                                        ON earth.gsod_raw
                                        PARTITION BY ANY
                                        TextColumn ('data') 
                                        OutputByWord ('false')
                                      )
                 )
                                 
              DATA_COLUMN('token')
              COLUMN_NAMES('stn','wban','yearmoda','temp','temp_count','dewp'
                            ,'dewp_count','slp','slp_count','stp','stp_count'
                            ,'visib','visib_count','wdsp', 'wdsp_count'
                            ,'mxdsp','gust','max','min','prcp','sndp','frshtt')
              COLUMN_TYPES ('varchar','varchar', 'varchar','varchar', 'varchar'
                           ,'varchar', 'varchar','varchar', 'varchar','varchar'
                           ,'varchar','varchar', 'varchar','varchar', 'varchar'
                           ,'varchar', 'varchar','varchar', 'varchar','varchar'
                           , 'varchar','varchar')
              COLUMN_DELIMITER('\/')
              DATA_GROUP(1)
              IGNORE_BAD_ROWS('true')
            );


DROP TABLE IF EXISTS earth.gsod_raw_temp;

CREATE DIMENSION TABLE earth.gsod_raw_temp
(   data VARCHAR	
);

ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors --skip-rows 1 earth.gsod_raw_temp raw_files

GRANT SELECT ON TABLE earth.gsod_raw_temp TO public;
            
DROP TABLE IF EXISTS earth.gsod_temp_agg;
CREATE FACT TABLE earth.gsod_temp_agg
DISTRIBUTE BY HASH (stn)
AS
SELECT *
FROM UNPACK (
              ON (
                   SELECT * 
                   FROM TextTokenizer (
                                        ON earth.gsod_raw_temp
                                        PARTITION BY ANY
                                        TextColumn ('data') 
                                        OutputByWord ('false')
                                      )
                 )
                                 
              DATA_COLUMN('token')
              COLUMN_NAMES('stn','wban','yearmoda','temp','temp_count','dewp'
                            ,'dewp_count','slp','slp_count','stp','stp_count'
                            ,'visib','visib_count','wdsp', 'wdsp_count'
                            ,'mxdsp','gust','max','min','prcp','sndp','frshtt')
              COLUMN_TYPES ('varchar','varchar', 'varchar','varchar', 'varchar'
                           ,'varchar', 'varchar','varchar', 'varchar','varchar'
                           ,'varchar','varchar', 'varchar','varchar', 'varchar'
                           ,'varchar', 'varchar','varchar', 'varchar','varchar'
                           , 'varchar','varchar')
              COLUMN_DELIMITER('\/')
              DATA_GROUP(1)
              IGNORE_BAD_ROWS('true')
            );            
 GRANT SELECT ON TABLE earth.gsod_temp_agg TO public;
           
            
SELECT COUNT(DISTINCT stn) FROM earth.gsod_temp_agg LIMIT 100;
SELECT COUNT(DISTINCT stn) FROM earth.gsod_agg LIMIT 100;
SELECT COUNT (DISTINCT stn, wban, REGEXP_REPLACE(yearmoda,'....$','')) FROM earth.gsod_agg;
\o loaded.csv
SELECT DISTINCT( stn || '-' ||  wban || '-' ||  REGEXP_REPLACE(yearmoda,'....$','') || '.op') FROM earth.gsod_agg;
\o

SELECT count(distinct stn, wban) FROM earth.gsod_agg;
SELECT * FROM earth.gsod_combined LIMIT 5;

DROP TABLE IF EXISTS earth.gsod_combined;
CREATE FACT TABLE earth.gsod_combined
DISTRIBUTE BY HASH (stn)
AS
SELECT *
FROM UNPACK (
              ON (
                   SELECT * 
                   FROM TextTokenizer (
                                        ON earth.gsod_raw
                                        PARTITION BY ANY
                                        TextColumn ('data') 
                                        OutputByWord ('false')
                                      )
                 )
                                 
              DATA_COLUMN('token')
              COLUMN_NAMES('stn','wban','yearmoda','temp','temp_count','dewp'
                            ,'dewp_count','slp','slp_count','stp','stp_count'
                            ,'visib','visib_count','wdsp', 'wdsp_count'
                            ,'mxdsp','gust','max','min','prcp','sndp','frshtt')
              COLUMN_TYPES ('varchar','varchar', 'varchar','varchar', 'varchar'
                           ,'varchar', 'varchar','varchar', 'varchar','varchar'
                           ,'varchar','varchar', 'varchar','varchar', 'varchar'
                           ,'varchar', 'varchar','varchar', 'varchar','varchar'
                           , 'varchar','varchar')
              COLUMN_DELIMITER('\/')
              DATA_GROUP(1)
              IGNORE_BAD_ROWS('true')
            );            
SELECT * FROM earth.gsod_tagg LIMIT 100;

SELECT count(*) FROM earth.gsod_raw LIMIT 5;

DROP TABLE IF EXISTS earth.gsod_raw_bkp;

CREATE DIMENSION TABLE earth.gsod_raw_bkp
(   data VARCHAR	
);



DROP TABLE IF EXISTS earth.gsod_raw_set2;

CREATE DIMENSION TABLE earth.gsod_raw_set2
(   data VARCHAR	
);

ALTER TABLE RENAME earth.gsod

ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors --skip-rows 1 earth.gsod_raw_set2 raw_files

GRANT SELECT ON TABLE earth.gsod_raw_set2 TO public;


 select * from earth.gsod_raw_set2 limit 5 
 
 
DROP TABLE IF EXISTS earth.gsod_agg_set2;
CREATE FACT TABLE earth.gsod_agg_set2
DISTRIBUTE BY HASH (stn)
AS
SELECT *
FROM UNPACK (
              ON (
                   SELECT * 
                   FROM TextTokenizer (
                                        ON earth.gsod_raw_set2
                                        PARTITION BY ANY
                                        TextColumn ('data') 
                                        OutputByWord ('false')
                                      )
                 )
                                 
              DATA_COLUMN('token')
              COLUMN_NAMES('stn','wban','yearmoda','temp','temp_count','dewp'
                            ,'dewp_count','slp','slp_count','stp','stp_count'
                            ,'visib','visib_count','wdsp', 'wdsp_count'
                            ,'mxdsp','gust','max','min','prcp','sndp','frshtt')
              COLUMN_TYPES ('varchar','varchar', 'varchar','varchar', 'varchar'
                           ,'varchar', 'varchar','varchar', 'varchar','varchar'
                           ,'varchar','varchar', 'varchar','varchar', 'varchar'
                           ,'varchar', 'varchar','varchar', 'varchar','varchar'
                           , 'varchar','varchar')
              COLUMN_DELIMITER('\/')
              DATA_GROUP(1)
              IGNORE_BAD_ROWS('true')
            );            
            
            
DROP TABLE IF EXISTS earth.gsod_lookup;            
CREATE DIMENSION TABLE  earth.gsod_lookup
AS
SELECT DISTINCT( stn || '-' ||  wban || '-' ||  REGEXP_REPLACE(yearmoda,'....$','') || '.op') AS src_file FROM earth.gsod_combined;
SELECT COUNT(*) FROM earth.gsod_lookup;


INSERT INTO earth.gsod_lookup
SELECT DISTINCT( stn || '-' ||  wban || '-' ||  REGEXP_REPLACE(yearmoda,'....$','') || '.op')  AS src_file  FROM earth.gsod_agg_set2;

SELECT COUNT(DISTINCT src_file) FROM earth.gsod_lookup;
            
CREATE FACT TABLE earth.gsod_combined_xyz
DISTRIBUTE BY HASH (stn)
AS
SELECT * FROM earth.gsod_combined
UNION
SELECT * FROM earth.gsod_agg_set2;
SELECT * FROM earth.gsod_combined_xyz LIMIT 5;
SELECT COUNT(*) FROM earth.gsod_combined_xyz LIMIT 5;


SELECT * FROM earth.gsod_lookup limit 5;




DROP TABLE IF EXISTS earth.worldwide_station_list;

CREATE DIMENSION TABLE earth.worldwide_station_list	
(   number VARCHAR	
   ,call VARCHAR	 
   ,name VARCHAR	 
   ,country_state VARCHAR	 
   ,lat VARCHAR	 
   ,lon VARCHAR	 
   ,elev VARCHAR	 
);
GRANT SELECT ON TABLE earth.worldwide_station_list TO public;

SELECT count(*) FROM earth.worldwide_station_list limit 5;

ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors --skip-rows 1 earth.worldwide_station_list Worldwide\ Station\ List.csv -c




DROP TABLE IF EXISTS earth.station_list;

CREATE DIMENSION TABLE earth.station_list	
(   regionid VARCHAR	
   ,regionname VARCHAR	 
   ,countryarea VARCHAR	 
   ,countrycode VARCHAR	 
   ,stationid VARCHAR	 
   ,indexnbr VARCHAR	 
   ,indexsubnbr VARCHAR	 
   ,stationname VARCHAR	 
   ,latitude VARCHAR	 
   ,longitude VARCHAR	 
   ,hp VARCHAR	 
   ,hpflag VARCHAR	 
   ,hha VARCHAR	 
   ,hhaflag VARCHAR	 
   ,pressuredefid VARCHAR	 
   ,so_1 VARCHAR	 
   ,so_2 VARCHAR	 
   ,so_3 VARCHAR	 
   ,so_4 VARCHAR	 
   ,so_5 VARCHAR	 
   ,so_6 VARCHAR	 
   ,so_7 VARCHAR	 
   ,so_8 VARCHAR	 
   ,obshs VARCHAR	 
   ,ua_1 VARCHAR	 
   ,ua_2 VARCHAR	 
   ,ua_3 VARCHAR	 
   ,ua_4 VARCHAR	 
   ,obsrems VARCHAR	 
);
GRANT SELECT ON TABLE earth.station_list TO public;


ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors --skip-rows 1 earth.station_list stations.txt '-D ' 

SELECT * FROM earth.station_list limit 5;
SELECT count(*) FROM earth.station_list limit 5;


DROP TABLE IF EXISTS earth.station_list_v2;

CREATE DIMENSION TABLE earth.station_list_v2	
(   icc VARCHAR	
   ,wmo_number VARCHAR	 
   ,wmo_modifier VARCHAR	 
   ,name VARCHAR	 
   ,lat VARCHAR	 
   ,lon VARCHAR	 
   ,elev VARCHAR	 
   ,tele VARCHAR	 
   ,p VARCHAR	 
   ,pop VARCHAR	 
   ,tp VARCHAR	 
   ,v VARCHAR	 
   ,lo VARCHAR	 
   ,co VARCHAR	 
   ,a VARCHAR	 
   ,ds VARCHAR	 
   ,vege VARCHAR	 
   ,bi VARCHAR	 
);
GRANT SELECT ON TABLE earth.station_list TO public;

SELECT * FROM earth.station_list_v2 limit 5;
SELECT count(*) FROM earth.station_list_v2 limit 5;

ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors --skip-rows 1 earth.station_list_v2 stationsv2_mod.txt



SELECT * FROM earth.station_list_v2 where lon ilike '%-%' limit 5;
SELECT * FROM earth.station_list limit 5;
SELECT * FROM earth.worldwide_station_list limit 5;
SELECT count(*) FROM earth.worldwide_station_list limit 5;

DROP TABLE IF EXISTS earth.station_list_combined_xyz;            
CREATE DIMENSION TABLE  earth.station_list_combined_xyz
DISTRIBUTE BY HASH(stationid)
AS
SELECT number as stationid
      ,CASE REGEXP_REPLACE(lat,'^.*([eEwW]).*$','\\1')
       WHEN 'E' THEN CAST(REGEXP_REPLACE(lat,'^(...).*$','\\1') AS DOUBLE) + (CAST(REGEXP_REPLACE(lat,'^... (..)$','\\1') AS DOUBLE) / 60)
       ELSE  (CAST(REGEXP_REPLACE(lat,'^(...).*$','\\1') AS DOUBLE) + (CAST(REGEXP_REPLACE(lat,'^... (..).*$','\\1') AS DOUBLE) / 60) ) * -1
       END AS xcoord            
      ,CASE REGEXP_REPLACE(lon,'^.*([sSnN]).*$','\\1')
       WHEN 'N' THEN CAST(REGEXP_REPLACE(lon,'^(..).*$','\\1') AS DOUBLE) + (CAST(REGEXP_REPLACE(lon,'^.. (..).*$','\\1') AS DOUBLE) / 60)
       ELSE  (CAST(REGEXP_REPLACE(lon,'^(..).*$','\\1') AS DOUBLE) + (CAST(REGEXP_REPLACE(lon,'^.. (..).*$','\\1') AS DOUBLE) / 60) ) * -1
       END AS ycoord       
 FROM earth.worldwide_station_list
 LIMIT 5;
 
 
SELECT distinct * FROM earth.gsod_combined a
INNER JOIN 
earth.station_list b
ON CAST(a.stn as varchar)=cast(b.stationid as varchar)
limit 5;


 
SELECT distinct * FROM earth.gsod_combined a
INNER JOIN 
earth.worldwide_station_list b
ON CAST(a.stn as varchar)=cast(b.number as varchar)
limit 5;


SELECT * FROM earth.worldwide_station_list limit 5;

SELECT * FROM earth.gsod_combined_xyz_temp LIMIT 50;

DROP TABLE IF EXISTS earth.gsod_combined_xyz_temp;
CREATE FACT TABLE earth.gsod_combined_xyz_temp
DISTRIBUTE BY HASH (stn)
AS
SELECT CAST(stn AS INTEGER) AS stn
      ,CAST(wban AS INTEGER) AS wban
      ,CAST(yearmoda AS INTEGER) AS yyyymmdd
      ,CAST(temp AS DOUBLE) AS temp
      ,CAST(temp_count AS INTEGER) AS temp_count
      ,CAST(dewp AS DOUBLE) AS dewp
      ,CAST(dewp_count AS INTEGER) AS dewp_count
      ,CAST(slp AS DOUBLE) AS slp
      ,CAST(slp_count AS INTEGER) AS slp_count
      ,CAST(stp AS DOUBLE) AS stp
      ,CAST(stp_count AS INTEGER) AS stp_count
      ,CAST(visib AS DOUBLE) AS visib
      ,CAST(visib_count AS INTEGER) AS visib_count
      ,CAST(wdsp AS DOUBLE) AS wdsp
      ,CAST(wdsp_count AS INTEGER) AS wdsp_count
      ,CAST(mxdsp AS DOUBLE) AS mxdsp
      ,CAST(gust AS DOUBLE) AS gust
      ,max
      ,min
      ,prcp
      ,CAST(sndp AS DOUBLE) AS sndp
      ,frshtt
FROM earth.gsod_combined_XYZ;

LIMIT 50;
ALTER TABLE earth.gsod_combined_XYZ RENAME TO gsod_combined_XYZ_tmp;
ALTER TABLE earth.gsod_combined_xyz_temp RENAME TO gsod_combined_XYZ;
GRANT SELECT ON TABLE earth.gsod_combined_XYZ TO PUBLIC;
SELECT COUNT(*) FROM earth.gsod_combined_XYZ;
SELECT COUNT(*) FROM earth.gsod_combined_xyz_temp;