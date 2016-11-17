-------------------------------------------------------------------------------
-- Script       : 100_Create_DB_Objects.sql
-- @description : Create the tables for data ingestion.
--                
-- Author(s)    : BIG DATA MNL
-- History      : 20161007 Timothy Santos Created
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Create table for ELT of Elevation Topography (SRTM) daily data
-------------------------------------------------------------------------------
select * from EARTH.srtmgl1_hgt_raw LIMIT 5;
DROP TABLE IF EXISTS earth.srtmgl1_hgt_raw;

CREATE DIMENSION TABLE earth.srtmgl1_hgt_raw
(  src VARCHAR
  ,row VARCHAR
  ,col VARCHAR
  ,elevation VARCHAR
);

GRANT SELECT ON TABLE earth.srtmgl1_hgt_raw TO public;
ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors -c earth.srtmgl1_hgt_raw srtmgl1_hgt_row.csv

SELECT * FROM earth.srtmgl1_hgt_raw LIMIT 5;

DROP TABLE IF EXISTS earth.srtmgl1_num_raw;

CREATE DIMENSION TABLE earth.srtmgl1_num_raw
(   data VARCHAR	
);

GRANT SELECT ON TABLE earth.srtmgl1_num_raw TO public;
ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors --skip-rows 1 earth.srtmgl1_num_raw srtmgl1num.csv

SELECT * FROM earth.srtmgl1_num_raw LIMIT 5;


--DROP TABLE IF EXISTS earth.srtmgl30_raw;
--
--CREATE DIMENSION TABLE earth.srtmgl30_raw
--(   data VARCHAR	
--);
--
--GRANT SELECT ON TABLE earth.srtmgl30_raw TO public;
--ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors --skip-rows 1 earth.srtmgl30_raw srtmgl30.csv


DROP TABLE IF EXISTS earth.srtmgl30_raw;

CREATE DIMENSION TABLE earth.srtmgl30_raw
(  src VARCHAR
  ,row VARCHAR
  ,col VARCHAR
  ,elevation VARCHAR
);

GRANT SELECT ON TABLE earth.srtmgl30_raw TO public;
ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors -c earth.srtmgl30_raw srtmgl30_dem.csv &


DROP TABLE IF EXISTS earth.srtmgl30_dem_xyz;
CREATE FACT TABLE earth.srtmgl30_dem_xyz
DISTRIBUTE BY HASH (src_file)
AS
SELECT src AS src_file
      ,CASE REGEXP_REPLACE(src,'^.*([snSN])([0-9]*).*$','\\1') 
       WHEN 'N' THEN CAST(REGEXP_REPLACE(src,'^.*([snSN])([0-9]*).*$','\\2') AS DOUBLE) + CAST((4800-CAST(ROW AS DOUBLE)) * 30/4800  AS DOUBLE)
       WHEN 'n' THEN CAST(REGEXP_REPLACE(src,'^.*([snSN])([0-9]*).*$','\\2') AS DOUBLE) + CAST((4800-CAST(ROW AS DOUBLE)) * 30/4800  AS DOUBLE)
       ELSE (CAST(REGEXP_REPLACE(src,'^.*([snSN])([0-9]*).*$','\\2')  AS DOUBLE) * -1 ) + CAST((4800-CAST(ROW AS DOUBLE)) * 30/4800  AS DOUBLE)
       END AS ycoord
      ,CASE REGEXP_REPLACE(src,'^([ewEW])([0-9]*).*$','\\1')  
       WHEN 'E' THEN CAST(REGEXP_REPLACE(src,'^([ewEW])([0-9]*).*$','\\2') AS DOUBLE) + (CAST(COL AS DOUBLE) * 30/6000)
       WHEN 'e' THEN CAST(REGEXP_REPLACE(src,'^([ewEW])([0-9]*).*$','\\2') AS DOUBLE) + (CAST(COL AS DOUBLE) * 30/6000)
       ELSE (CAST(REGEXP_REPLACE(src,'^([ewEW])([0-9]*).*$','\\2')  AS DOUBLE) * -1 ) + (CAST(COL AS DOUBLE) * 30/6000)
       END AS xcoord       
      ,elevation 
FROM earth.srtmgl30_raw
;
GRANT SELECT ON TABLE earth.srtmgl30_dem_xyz TO public;
SELECT * FROM earth.srtmgl30_dem_xyz LIMIT 5;
SELECT COUNT(*) FROM earth.srtmgl30_dem_xyz LIMIT 5;

SELECT COUNT(*) FROM earth.srtmgl30_raw LIMIT 1;
SELECT * FROM earth.srtmgl30_raw limit 5;
   SELECT * 
   FROM TextTokenizer (
                        ON earth.srtmgl1_hgt_raw
                        PARTITION BY ANY
                        TextColumn ('data') 
                        OutputByWord ('true')
                      )
   LIMIT 10;
SELECT * FROM earth.srtmgl30_raw LIMIT 10;


DROP TABLE IF EXISTS earth.srtmgl1_hgt_xyz;
CREATE DIMENSION TABLE earth.srtmgl1_hgt_xyz   
SELECT * FROM text_parser(
ON earth.srtmgl30_raw
TEXT_COLUMN('data')
CASE_INSENSITIVE('true')
PUNCTUATION(',')
LIST_POSITIONS('true')
)
LIMIT 100;


SELECT *
FROM UNPACK (
              ON (
                   SELECT * FROM earth.srtmgl30_raw LIMIT 1
                 )
                                 
              DATA_COLUMN('data')
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

SELECT * FROM Unpack (
ON  ON (
                   SELECT * FROM earth.srtmgl30_raw LIMIT 1
                 )
Data_Column ('src')
Column_Names ('ns','lat','ew','lat')
Column_Types (varchar,integer,varchar,integer)
 Data_Pattern ('([snSN])([0-9]*)([ewEW])([0-9]*).*')
[ Data_Group ('group_number') ]
);            
SELECT src_file,xcoord,ycoord,elevation FROM earth.srtmgl1_hgt_xyz order by src_file desc limit 5;

ALTER TABLE earth.srtmgl1_hgt_xyz RENAME COLUMN LON TO YCOORD;
ALTER TABLE earth.srtmgl1_hgt_xyz RENAME COLUMN LAT TO XCOORD;

DROP TABLE IF EXISTS earth.srtmgl1_hgt_reg1_raw;

CREATE DIMENSION TABLE earth.srtmgl1_hgt_reg1_raw
(  src VARCHAR
  ,row VARCHAR
  ,col VARCHAR
  ,elevation VARCHAR
);

GRANT SELECT ON TABLE earth.srtmgl1_hgt_reg1_raw TO public;
ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors -c earth.srtmgl1_hgt_reg1_raw srtmgl1_hgt_reg1.csv


DROP TABLE IF EXISTS earth.srtmgl1_hgt_reg1_xyz;
CREATE FACT TABLE earth.srtmgl1_hgt_reg1_xyz
DISTRIBUTE BY HASH (src_file)
AS
SELECT src AS src_file
      ,CASE REGEXP_REPLACE(src,'^([snSN])([0-9]*)([ewEW])([0-9]*).*$','\\1') 
       WHEN 'N' THEN CAST(REGEXP_REPLACE(src,'^([snSN])([0-9]*)([ewEW])([0-9]*).*$','\\2') AS DOUBLE) + ((1201-CAST(ROW AS DOUBLE)) * 1/1200)
       ELSE (CAST(REGEXP_REPLACE(src,'^([snSN])([0-9]*)([ewEW])([0-9]*).*$','\\2')  AS DOUBLE) * -1 ) + ((1201-CAST(ROW AS DOUBLE)) * 1/1200)
       END AS ycoord
      ,CASE REGEXP_REPLACE(src,'^([snSN])([0-9]*)([ewEW])([0-9]*).*$','\\3') 
       WHEN 'E' THEN CAST(REGEXP_REPLACE(src,'^([snSN])([0-9]*)([ewEW])([0-9]*).*$','\\4') AS DOUBLE) + (CAST(COL AS DOUBLE) * 1 / 1200)
       ELSE (CAST(REGEXP_REPLACE(src,'^([snSN])([0-9]*)([ewEW])([0-9]*).*$','\\4')  AS DOUBLE) * -1) + (CAST(COL AS DOUBLE) * 1 / 1200)
       END AS xcoord
      ,elevation FROM earth.srtmgl1_hgt_reg1_raw;
GRANT SELECT ON TABLE earth.srtmgl1_hgt_reg1_xyz TO public;
      
SELECT count(*) FROM earth.srtmgl1_hgt_reg1_xyz     

SELECT * FROM earth.srtmgl1_hgt_reg7_raw LIMIT 10;      
SELECT * FROM earth.srtmgl1_hgt_reg6_raw LIMIT 10;      
SELECT * FROM earth.srtmgl1_hgt_reg5_raw LIMIT 10;      
SELECT * FROM earth.srtmgl1_hgt_reg1_raw LIMIT 10;      

      


DROP TABLE IF EXISTS earth.srtmgl1_hgt_reg7_raw;

CREATE DIMENSION TABLE earth.srtmgl1_hgt_reg7_raw
(  src VARCHAR
  ,row VARCHAR
  ,col VARCHAR
  ,elevation VARCHAR
);

GRANT SELECT ON TABLE earth.srtmgl1_hgt_reg7_raw TO public;
ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors -c earth.srtmgl1_hgt_reg7_raw srtmgl1_hgt_reg7.csv &


DROP TABLE IF EXISTS earth.srtmgl1_hgt_reg7_xyz;
CREATE FACT TABLE earth.srtmgl1_hgt_reg7_xyz
DISTRIBUTE BY HASH (src_file)
AS
SELECT src AS src_file
      ,CASE REGEXP_REPLACE(src,'^([snSN])([0-9]*)([ewEW])([0-9]*).*$','\\1') 
       WHEN 'N' THEN CAST(REGEXP_REPLACE(src,'^([snSN])([0-9]*)([ewEW])([0-9]*).*$','\\2') AS DOUBLE) + ((1201-CAST(ROW AS DOUBLE)) * 1/1200)
       ELSE (CAST(REGEXP_REPLACE(src,'^([snSN])([0-9]*)([ewEW])([0-9]*).*$','\\2')  AS DOUBLE) * -1 ) + ((1201-CAST(ROW AS DOUBLE)) * 1/1200)
       END AS ycoord
      ,CASE REGEXP_REPLACE(src,'^([snSN])([0-9]*)([ewEW])([0-9]*).*$','\\3') 
       WHEN 'E' THEN CAST(REGEXP_REPLACE(src,'^([snSN])([0-9]*)([ewEW])([0-9]*).*$','\\4') AS DOUBLE) + (CAST(COL AS DOUBLE) * 1 / 1200)
       ELSE (CAST(REGEXP_REPLACE(src,'^([snSN])([0-9]*)([ewEW])([0-9]*).*$','\\4')  AS DOUBLE) * -1) + (CAST(COL AS DOUBLE) * 1 / 1200)
       END AS xcoord
      ,elevation FROM earth.srtmgl1_hgt_reg7_raw;
GRANT SELECT ON TABLE earth.srtmgl1_hgt_reg7_xyz TO public;
      
SELECT count(*) FROM earth.srtmgl1_hgt_xyz     ;




DROP TABLE IF EXISTS earth.srtmgl1_hgt_reg6_raw;

CREATE DIMENSION TABLE earth.srtmgl1_hgt_reg6_raw
(  src VARCHAR
  ,row VARCHAR
  ,col VARCHAR
  ,elevation VARCHAR
);

GRANT SELECT ON TABLE earth.srtmgl1_hgt_reg6_raw TO public;
ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors -c earth.srtmgl1_hgt_reg6_raw srtmgl1_hgt_reg6.csv &


DROP TABLE IF EXISTS earth.srtmgl1_hgt_reg6_xyz;
CREATE FACT TABLE earth.srtmgl1_hgt_reg6_xyz
DISTRIBUTE BY HASH (src_file)
AS
SELECT src AS src_file
      ,CASE REGEXP_REPLACE(src,'^([snSN])([0-9]*)([ewEW])([0-9]*).*$','\\1') 
       WHEN 'N' THEN CAST(REGEXP_REPLACE(src,'^([snSN])([0-9]*)([ewEW])([0-9]*).*$','\\2') AS DOUBLE) + ((1201-CAST(ROW AS DOUBLE)) * 1/1200)
       ELSE (CAST(REGEXP_REPLACE(src,'^([snSN])([0-9]*)([ewEW])([0-9]*).*$','\\2')  AS DOUBLE) * -1 ) + ((1201-CAST(ROW AS DOUBLE)) * 1/1200)
       END AS ycoord
      ,CASE REGEXP_REPLACE(src,'^([snSN])([0-9]*)([ewEW])([0-9]*).*$','\\3') 
       WHEN 'E' THEN CAST(REGEXP_REPLACE(src,'^([snSN])([0-9]*)([ewEW])([0-9]*).*$','\\4') AS DOUBLE) + (CAST(COL AS DOUBLE) * 1 / 1200)
       ELSE (CAST(REGEXP_REPLACE(src,'^([snSN])([0-9]*)([ewEW])([0-9]*).*$','\\4')  AS DOUBLE) * -1) + (CAST(COL AS DOUBLE) * 1 / 1200)
       END AS xcoord
      ,elevation FROM earth.srtmgl1_hgt_reg6_raw;
GRANT SELECT ON TABLE earth.srtmgl1_hgt_reg6_xyz TO public;
      
SELECT count(*) FROM earth.srtmgl1_hgt_xyz     



DROP TABLE IF EXISTS earth.srtmgl1_hgt_reg5_raw;

CREATE DIMENSION TABLE earth.srtmgl1_hgt_reg5_raw
(  src VARCHAR
  ,row VARCHAR
  ,col VARCHAR
  ,elevation VARCHAR
);

GRANT SELECT ON TABLE earth.srtmgl1_hgt_reg5_raw TO public;
ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors -c earth.srtmgl1_hgt_reg5_raw srtmgl1_hgt_reg5.csv &


DROP TABLE IF EXISTS earth.srtmgl1_hgt_reg5_xyz;
CREATE FACT TABLE earth.srtmgl1_hgt_reg5_xyz
DISTRIBUTE BY HASH (src_file)
AS
SELECT src AS src_file
      ,CASE REGEXP_REPLACE(src,'^([snSN])([0-9]*)([ewEW])([0-9]*).*$','\\1') 
       WHEN 'N' THEN CAST(REGEXP_REPLACE(src,'^([snSN])([0-9]*)([ewEW])([0-9]*).*$','\\2') AS DOUBLE) + ((1201-CAST(ROW AS DOUBLE)) * 1/1200)
       ELSE (CAST(REGEXP_REPLACE(src,'^([snSN])([0-9]*)([ewEW])([0-9]*).*$','\\2')  AS DOUBLE) * -1 ) + ((1201-CAST(ROW AS DOUBLE)) * 1/1200)
       END AS ycoord
      ,CASE REGEXP_REPLACE(src,'^([snSN])([0-9]*)([ewEW])([0-9]*).*$','\\3') 
       WHEN 'E' THEN CAST(REGEXP_REPLACE(src,'^([snSN])([0-9]*)([ewEW])([0-9]*).*$','\\4') AS DOUBLE) + (CAST(COL AS DOUBLE) * 1 / 1200)
       ELSE (CAST(REGEXP_REPLACE(src,'^([snSN])([0-9]*)([ewEW])([0-9]*).*$','\\4')  AS DOUBLE) * -1) + (CAST(COL AS DOUBLE) * 1 / 1200)
       END AS xcoord
      ,elevation FROM earth.srtmgl1_hgt_reg5_raw;
 GRANT SELECT ON TABLE earth.srtmgl1_hgt_reg5_xyz TO public;
     
SELECT count(*) FROM earth.srtmgl1_hgt_xyz     

DROP TABLE IF EXISTS earth.srtmgl30_dem_xyz_temp;
CREATE FACT TABLE  earth.srtmgl30_dem_xyz_temp
DISTRIBUTE BY HASH(src_file)
AS
SELECT src_file
      ,xcoord
      ,ycoord
      ,CAST(elevation AS INTEGER) AS elevation
FROM earth.srtmgl30_dem_xyz;

SELECT COUNT(*) FROM earth.srtmgl30_dem_xyz;
SELECT COUNT(*) FROM earth.srtmgl30_dem_xyz_temp;

ALTER TABLE earth.srtmgl30_dem_xyz RENAME TO srtmgl30_dem_xyz_tmp;
ALTER TABLE earth.srtmgl30_dem_xyz_temp RENAME TO srtmgl30_dem_xyz;
GRANT SELECT ON TABLE earth.srtmgl30_dem_xyz TO PUBLIC;

