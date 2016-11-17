-------------------------------------------------------------------------------
-- Script       : 201_Create_gpwv4_xyz.sql
-- @description : Create the tables for data ingestion.
--                
-- Author(s)    : BIG DATA MNL
-- History      : 20161007 Timothy Santos Created
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Create table for ELT of Population GPWV4 data
-------------------------------------------------------------------------------
SELECT * FROM social.gpwv4_longlat_xyz  limit 10
-------------------------------------------------------------------------------
-- Transform to Formatted xyz table
-------------------------------------------------------------------------------
--DROP TABLE IF EXISTS social.gpwv4_longlat_xyztemp;

DROP TABLE IF EXISTS social.gpwv4_longlat_xyz;
CREATE FACT TABLE social.gpwv4_longlat_xyz
DISTRIBUTE BY HASH(asc_row)
AS
SELECT CAST('2000' AS VARCHAR) AS year
      ,(CAST(rowid AS INTEGER) - 7) AS asc_row
      ,(CAST(sn AS INTEGER) -1) AS asc_col
      ,(-180 + CAST((SN-1) + 0.5  AS DOUBLE) * 0.0083333333333334) AS xcoord 
      ,(-59.999999999992 + CAST(17400 + (-(rowid - 7)  + 0.5)  AS DOUBLE) * 0.0083333333333334) AS ycoord       
      ,token AS pop
FROM TextTokenizer (
                    ON (
                         SELECT * FROM social.gpwv4_2000 
                         WHERE rowid>6
--                         LIMIT 2
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;

INSERT INTO social.gpwv4_longlat_xyz 
SELECT CAST('2005' AS VARCHAR) AS year
      ,(CAST(rowid AS INTEGER) - 7) AS asc_row
      ,(CAST(sn AS INTEGER) -1) AS asc_col
      ,(-180 + CAST((SN-1) + 0.5  AS DOUBLE) * 0.0083333333333334) AS xcoord 
      ,(-59.999999999992 + CAST(17400 + (-(rowid - 7) + 0.5)  AS DOUBLE) * 0.0083333333333334) AS ycoord       
      ,token AS pop
FROM TextTokenizer (
                    ON (
                         SELECT * FROM social.gpwv4_2005 
                         WHERE rowid>6
--                         LIMIT 2
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;


INSERT INTO social.gpwv4_longlat_xyz 
SELECT CAST('2010' AS VARCHAR) AS year
      ,(CAST(rowid AS INTEGER) - 7) AS asc_row
      ,(CAST(sn AS INTEGER) -1) AS asc_col
      ,(-180 + CAST((SN-1) + 0.5  AS DOUBLE) * 0.0083333333333334) AS xcoord 
      ,(-59.999999999992 + CAST(17400 + (-(rowid - 7)  + 0.5)  AS DOUBLE) * 0.0083333333333334) AS ycoord       
      ,token AS pop
FROM TextTokenizer (
                    ON (
                         SELECT * FROM social.gpwv4_2010 
                         WHERE rowid>6
--                         LIMIT 2
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;


INSERT INTO social.gpwv4_longlat_xyz 
SELECT CAST('2015' AS VARCHAR) AS year
      ,(CAST(rowid AS INTEGER) - 7) AS asc_row
      ,(CAST(sn AS INTEGER) -1) AS asc_col
      ,(-180 + CAST((SN-1) + 0.5  AS DOUBLE) * 0.0083333333333334) AS xcoord 
      ,(-59.999999999992 + CAST(17400 + (-(rowid - 7)  + 0.5)  AS DOUBLE) * 0.0083333333333334) AS ycoord       
      ,token AS pop
FROM TextTokenizer (
                    ON (
                         SELECT * FROM social.gpwv4_2015 
                         WHERE rowid>6
--                         LIMIT 2
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;


INSERT INTO social.gpwv4_longlat_xyz 
SELECT CAST('2020' AS VARCHAR) AS year
      ,(CAST(rowid AS INTEGER) - 7) AS asc_row
      ,(CAST(sn AS INTEGER) -1) AS asc_col
      ,(-180 + CAST((SN-1) + 0.5  AS DOUBLE) * 0.0083333333333334) AS xcoord 
      ,(-59.999999999992 + CAST(17400 + (-(rowid - 7)  + 0.5)  AS DOUBLE) * 0.0083333333333334) AS ycoord       
      ,token AS pop
FROM TextTokenizer (
                    ON (
                         SELECT * FROM social.gpwv4_2020 
                         WHERE rowid>6
--                         LIMIT 2
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;
GRANT SELECT ON TABLE social.gpwv4_longlat_xyz TO public;

SELECT * FROM social.gpwv4_longlat_xyz WHERE pop not ilike '%-9999%' AND year='2000' LIMIT 100;

SELECT * FROM social.gpwv4_agg_xyz LIMIT 100;
ALTER TABLE

DROP TABLE IF EXISTS social.gpwv4_combined_xyz;
CREATE FACT TABLE social.gpwv4_combined_xyz
DISTRIBUTE BY HASH(year)
AS
SELECT year, xcoord, ycoord, pop
FROM social.gpwv4_longlat_xyz;

GRANT SELECT ON TABLE social.gpwv4_combined_xyz TO public;



SELECT * FROM social.gpwv4_agg_xyz LIMIT 5;

ALTER TABLE social.gpwv4_combined_xyz ALTER COLUMN pop TYPE DOUBLE;

DROP TABLE IF EXISTS social.gpwv4_temp;
CREATE FACT TABLE social.gpwv4_temp
DISTRIBUTE BY HASH (yyyy)
AS
SELECT CAST(year AS INTEGER) AS YYYY
      ,xcoord
      ,ycoord
      ,CAST(pop AS DOUBLE) AS pop
      FROM social.gpwv4_combined_xyz;
      
      SELECT COUNT(*) FROM social.gpwv4_temp;
      SELECT COUNT(*) FROM social.gpwv4_combined_xyz;
      
      ALTER TABLE social.gpwv4_combined_xyz RENAME TO gpwv4_combined_xyz_tmp;
      ALTER TABLE social.gpwv4_temp RENAME TO gpwv4_combined_xyz;
      
      
      where pop not ilike '%999%' limit 100 