-------------------------------------------------------------------------------
-- Script       : 000_scratch.sql
-- @description : Create the tables for data ingestion.
--                
-- Author(s)    : BIG DATA MNL
-- History      : 20161007 Timothy Santos Created
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Create table for loading glipha data - livestock
-------------------------------------------------------------------------------

DROP TABLE IF EXISTS stg.glipha_sheep_raw;

CREATE DIMENSION TABLE stg.glipha_sheep_raw
(   rowid SERIAL GLOBAL
    ,data VARCHAR	
);
nohup ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive  --el-enabled --el-discard-errors stg.glipha_sheep_raw  shp1.asc -C "data" &



DROP TABLE IF EXISTS stg.glipha_ducks_raw;

CREATE DIMENSION TABLE stg.glipha_ducks_raw
(   rowid SERIAL GLOBAL
    ,data VARCHAR	
);
nohup ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors stg.glipha_ducks_raw  dks.asc -C "data" &


DROP TABLE IF EXISTS stg.glipha_goats_raw;

CREATE DIMENSION TABLE stg.glipha_goats_raw
(   rowid SERIAL GLOBAL
    ,data VARCHAR	
);
nohup ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors stg.glipha_goats_raw  gt1.asc -C "data" &



DROP TABLE IF EXISTS stg.glipha_pigs_raw;

CREATE DIMENSION TABLE stg.glipha_pigs_raw
(   rowid SERIAL GLOBAL
    ,data VARCHAR	
);
nohup ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors stg.glipha_pigs_raw  pg.asc -C "data" &

SELECT COUNT(*) FROM stg.glipha_pigs_raw;
SELECT COUNT(*) FROM stg.glipha_ducks_raw;
SELECT COUNT(*) FROM stg.glipha_goats_raw;
SELECT * FROM stg.glipha_ducks_raw WHERE rowid<7;

DROP TABLE IF EXISTS stg.glipha_combined;
CREATE FACT TABLE stg.glipha_combined
DISTRIBUTE BY HASH(asc_row)
AS
SELECT CAST('pigs' AS VARCHAR) AS livestock
      ,(CAST(rowid AS INTEGER) - 7) AS asc_row
      ,(CAST(sn AS INTEGER) -1) AS asc_col
      ,(-180 + CAST((SN-1) + 0.5  AS DOUBLE) * 0.008333333333) AS xcoord 
      ,(-90 + CAST(21600 + (-(rowid - 7)  + 0.5)  AS DOUBLE) * 0.008333333333) AS ycoord       
      ,token AS livestock_density
FROM TextTokenizer (
                    ON (
                         SELECT * FROM stg.glipha_pigs_raw 
                         WHERE rowid>6
--                         LIMIT 2
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;



DROP TABLE IF EXISTS stg.glipha;
CREATE FACT TABLE stg.glipha
DISTRIBUTE BY HASH(asc_row)
AS
SELECT CAST('pigs' AS VARCHAR) AS livestock
      ,(CAST(rowid AS INTEGER) - 7) AS asc_row
      ,(CAST(sn AS INTEGER) -1) AS asc_col
      ,(-180 + CAST((SN-1) + 0.5  AS DOUBLE) * 0.008333333333) AS xcoord 
      ,(-90 + CAST(21600 + (-(rowid - 7)  + 0.5)  AS DOUBLE) * 0.008333333333) AS ycoord       
      ,token AS livestock_density
FROM TextTokenizer (
                    ON (
                         SELECT * FROM stg.glipha_pigs_raw 
                         WHERE rowid>6
--                         LIMIT 2
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;

INSERT INTO stg.glipha 
SELECT CAST('ducks' AS VARCHAR) AS livestock
      ,(CAST(rowid AS INTEGER) - 7) AS asc_row
      ,(CAST(sn AS INTEGER) -1) AS asc_col
      ,(-180 + CAST((SN-1) + 0.5  AS DOUBLE) * 0.008333333333) AS xcoord 
      ,(-90 + CAST(21600 + (-(rowid - 7)  + 0.5)  AS DOUBLE) * 0.008333333333) AS ycoord       
      ,token AS livestock_density
FROM TextTokenizer (
                    ON (
                         SELECT * FROM stg.glipha_ducks_raw 
                         WHERE rowid>6
--                         LIMIT 2
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;

INSERT INTO stg.glipha 
SELECT CAST('goats' AS VARCHAR) AS livestock
      ,(CAST(rowid AS INTEGER) - 7) AS asc_row
      ,(CAST(sn AS INTEGER) -1) AS asc_col
      ,(-180 + CAST((SN-1) + 0.5  AS DOUBLE) * 0.008333333333) AS xcoord 
      ,(-90 + CAST(21600 + (-(rowid - 7)  + 0.5)  AS DOUBLE) * 0.008333333333) AS ycoord       
      ,token AS livestock_density
FROM TextTokenizer (
                    ON (
                         SELECT * FROM stg.glipha_goats_raw 
                         WHERE rowid>6
--                         LIMIT 2
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;
INSERT INTO stg.glipha 
SELECT CAST('sheep' AS VARCHAR) AS livestock
      ,(CAST(rowid AS INTEGER) - 7) AS asc_row
      ,(CAST(sn AS INTEGER) -1) AS asc_col
      ,(-180 + CAST((SN-1) + 0.5  AS DOUBLE) * 0.008333333333) AS xcoord 
      ,(-90 + CAST(21600 + (-(rowid - 7)  + 0.5)  AS DOUBLE) * 0.008333333333) AS ycoord       
      ,token AS livestock_density
FROM TextTokenizer (
                    ON (
                         SELECT * FROM stg.glipha_sheep_raw 
                         WHERE rowid>6
--                         LIMIT 2
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;

select * from stg.glipha_sheep_raw limit 5;

DROP TABLE IF EXISTS life.glw_metadata;
CREATE DIMENSION TABLE life.glw_metadata
AS
SELECT CAST('Glb_SHAD_2006.tif' AS VARCHAR) AS source_file, CAST('sheep' AS VARCHAR) AS livestock, * FROM stg.glipha_sheep_raw WHERE rowid<7;
INSERT INTO life.glw_metadata
SELECT 'Glb_GTAD_2006.tif' AS source_file, 'goats' AS livestock, * FROM stg.glipha_goats_raw WHERE rowid<7;
INSERT INTO life.glw_metadata
SELECT 'Glb_Pigs_CC2006_AD.tif' AS source_file, 'pigs' AS livestock, * FROM stg.glipha_pigs_raw WHERE rowid<7;
INSERT INTO life.glw_metadata
SELECT 'GLb_Ducks_CC2006_AD.tif' AS source_file, 'ducks' AS livestock, * FROM stg.glipha_ducks_raw WHERE rowid<7;

GRANT SELECT ON TABLE life.glw_metadata TO PUBLIC;
SELECT DISTINCT  livestock from STG.glipha 
